# utils.py
# ===========================================================================
# Shared helper functions for the Python scripts.
# ===========================================================================

import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


# Fama-French 12 industry classification
# Maps SIC codes to industry names and numbers.
# Source: Kenneth French's data library.

FF12_RANGES = [
    # (low, high, name, num)
    (100, 999, "Consumer Nondurables", 1),
    (2000, 2399, "Consumer Nondurables", 1),
    (2700, 2749, "Consumer Nondurables", 1),
    (2770, 2799, "Consumer Nondurables", 1),
    (3100, 3199, "Consumer Nondurables", 1),
    (3940, 3989, "Consumer Nondurables", 1),
    (2500, 2519, "Consumer Durables", 2),
    (2590, 2599, "Consumer Durables", 2),
    (3630, 3659, "Consumer Durables", 2),
    (3710, 3711, "Consumer Durables", 2),
    (3714, 3714, "Consumer Durables", 2),
    (3716, 3716, "Consumer Durables", 2),
    (3750, 3751, "Consumer Durables", 2),
    (3792, 3792, "Consumer Durables", 2),
    (3900, 3939, "Consumer Durables", 2),
    (3990, 3999, "Consumer Durables", 2),
    (2520, 2589, "Manufacturing", 3),
    (2600, 2699, "Manufacturing", 3),
    (2750, 2769, "Manufacturing", 3),
    (3000, 3099, "Manufacturing", 3),
    (3200, 3569, "Manufacturing", 3),
    (3580, 3629, "Manufacturing", 3),
    (3700, 3709, "Manufacturing", 3),
    (3712, 3713, "Manufacturing", 3),
    (3715, 3715, "Manufacturing", 3),
    (3717, 3749, "Manufacturing", 3),
    (3752, 3791, "Manufacturing", 3),
    (3793, 3799, "Manufacturing", 3),
    (3830, 3839, "Manufacturing", 3),
    (3860, 3899, "Manufacturing", 3),
    (1200, 1399, "Energy", 4),
    (2900, 2999, "Energy", 4),
    (2800, 2829, "Chemicals", 5),
    (2840, 2899, "Chemicals", 5),
    (3570, 3579, "Business Equipment", 6),
    (3660, 3692, "Business Equipment", 6),
    (3694, 3699, "Business Equipment", 6),
    (3810, 3829, "Business Equipment", 6),
    (7370, 7379, "Business Equipment", 6),
    (4800, 4899, "Telecommunications", 7),
    (4900, 4949, "Utilities", 8),
    (5000, 5999, "Retail", 9),
    (7200, 7299, "Retail", 9),
    (7600, 7699, "Retail", 9),
    (2830, 2839, "Healthcare", 10),
    (3693, 3693, "Healthcare", 10),
    (3840, 3859, "Healthcare", 10),
    (8000, 8099, "Healthcare", 10),
    (6000, 6999, "Finance", 11),
]


def assign_ff12(sic_col: str = "siccd") -> list[pl.Expr]:
    """Return polars expressions for FF12 and ff12num columns from a SIC column."""
    # Build a chained when/then for the name
    expr_name = pl.lit("Other")
    expr_num = pl.lit(12)
    # Process in reverse so the first match wins
    for lo, hi, name, num in reversed(FF12_RANGES):
        cond = (pl.col(sic_col) >= lo) & (pl.col(sic_col) <= hi)
        expr_name = pl.when(cond).then(pl.lit(name)).otherwise(expr_name)
        expr_num = pl.when(cond).then(pl.lit(num)).otherwise(expr_num)
    return [expr_name.alias("FF12"), expr_num.cast(pl.Float64).alias("ff12num")]


def _normalize_arrow_schema(table: pa.Table) -> pa.Table:
    """Cast decimal128 columns to float64 to avoid schema mismatches.

    PostgreSQL NUMERIC columns can have varying precision across batches
    (e.g., decimal128(9,4) in batch 1, decimal128(8,4) in batch 2). This
    causes pyarrow's ParquetWriter to fail because the schema must be
    consistent across all batches.

    Casting to float64 is safe for financial data and matches what polars
    uses internally for these columns.
    """
    new_fields = []
    needs_cast = False
    for field in table.schema:
        if pa.types.is_decimal(field.type):
            new_fields.append(pa.field(field.name, pa.float64()))
            needs_cast = True
        else:
            new_fields.append(field)

    if needs_cast:
        return table.cast(pa.schema(new_fields))
    return table


def download_wrds(
    sql: str,
    output_path: str,
    connection,
    max_ram_mb: int = 8000,
    batch_size: int | None = None,
    compression: str = "zstd",
):
    """Download a WRDS table to a local parquet file using chunked streaming.

    Uses a server-side cursor to stream rows in batches and writes each batch
    to a parquet file via pyarrow's ParquetWriter. Peak memory = one batch.

    If batch_size is not specified, it is auto-calculated from max_ram_mb by
    peeking at the number of columns in the query result:
        batch_size = max_ram_mb * 1e6 / (n_columns * 8)
    The auto-sized value is then SILENTLY CAPPED at 5,000,000 rows so that
    users see periodic progress output instead of a long silent fetch on
    huge narrow tables (CRSP daily would otherwise be one batch of ~333M
    rows = 4 minutes of silence). The cap only applies to auto-sized
    batches; if you pass batch_size explicitly, your value is respected.

    NOTE: Actual peak RAM = max_ram_mb + ~1-2 GB baseline overhead (Python
    interpreter, loaded packages, writer buffers). On a 16 GB machine the
    default 8 GB target is conservative; on a 4 GB machine, try max_ram_mb=2000.

    Args:
        sql: SQL query to execute on WRDS.
        output_path: Path for the output parquet file.
        connection: An open psycopg2 connection to WRDS.
        max_ram_mb: Target peak RAM in MB for auto batch sizing (default 8000).
        batch_size: Rows per batch. If None, auto-calculated from max_ram_mb.
        compression: Parquet compression codec (default "zstd").
    """
    # Create a unique cursor name from the output filename to avoid collisions
    # if multiple downloads run in the same session.
    cursor_name = Path(output_path).stem.replace("-", "_")

    # Auto-calculate batch_size from max_ram_mb if not specified.
    # Peek at the number of columns by running the query with LIMIT 0.
    if batch_size is None:
        peek_cursor = connection.cursor()
        peek_cursor.execute(f"SELECT * FROM ({sql}) q LIMIT 0")
        n_cols = len(peek_cursor.description)
        peek_cursor.close()
        # ~8 bytes per value (floats, ints, dates are all 8 bytes in memory)
        raw = max(1_000, int(max_ram_mb * 1e6 / (n_cols * 8)))
        # Silent cap: large narrow tables would otherwise be fetched in one
        # giant batch, leaving the user staring at no output for minutes.
        # Capping at 5M lets progress messages print every batch (~10-20s on
        # a 100M-row download) so users see things moving.
        batch_size = min(raw, 5_000_000)
        print(f"  Auto batch size: {batch_size:,} rows "
              f"({n_cols} columns, {max_ram_mb:,} MB RAM target)")

    # Server-side cursor (name= parameter makes it server-side in psycopg2).
    cursor = connection.cursor(name=cursor_name)
    cursor.execute(sql)

    writer = None
    total_rows = 0
    start = time.time()

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        total_rows += len(rows)
        elapsed = time.time() - start
        # Show file size on disk so users see bytes accumulating across
        # batches, not just row count.
        size_mb = (Path(output_path).stat().st_size / 1e6
                   if Path(output_path).exists() else 0)
        print(
            f"\r  {total_rows:,} rows | {elapsed / 60:.1f} min | "
            f"~{size_mb:.0f} MB on disk",
            end="",
        )

        # Convert list-of-tuples to pyarrow Table
        col_names = [desc[0] for desc in cursor.description]
        col_data = {col: [row[i] for row in rows] for i, col in enumerate(col_names)}
        table = _normalize_arrow_schema(pa.Table.from_pydict(col_data))

        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema, compression=compression)

        writer.write_table(table)

    if writer is not None:
        writer.close()

    cursor.close()

    print()  # newline after progress
    size_mb = Path(output_path).stat().st_size / 1e6
    print(f"  Saved {total_rows:,} rows, {size_mb:.1f} MB")
