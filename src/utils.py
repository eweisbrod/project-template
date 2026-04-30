# utils.py
# ===========================================================================
# Shared helper functions for the Python scripts.
# ===========================================================================

import getpass
import re
import subprocess
import sys
import time
import webbrowser
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


def download_parquet(
    sql: str,
    output_path: str,
    connection,
    max_ram_mb: int = 8000,
    batch_size: int | None = None,
    compression: str = "zstd",
    skip_if_exists: bool = True,
):
    """Stream a SQL result to a local parquet file via a server-side cursor.

    Database-agnostic — anything that gives you a DBAPI 2.0 / psycopg2 cursor
    will work (WRDS PostgreSQL, BigQuery via google-cloud-bigquery's DB-API,
    Snowflake's connector, a local DuckDB connection, etc.). It used to be
    called download_wrds(); the rename reflects that the WRDS-specific bit
    lives in your connection setup, not in this function.

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
        skip_if_exists: If True (default) and output_path already exists, skip
            the download and return. Delete the file to force a re-download.
            Makes replication runs cheap on the WRDS side.
    """
    # Skip-if-exists: don't re-pull a parquet that's already on disk. Lets a
    # downstream user (or the original analyst) re-run the pipeline without
    # hitting WRDS, and lets a single file be refreshed by deleting it.
    if skip_if_exists and Path(output_path).exists():
        size_mb = Path(output_path).stat().st_size / 1e6
        print(f"  Skipping download — file exists: {output_path} ({size_mb:.1f} MB)")
        return

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


# Run a Python script via run_with_echo (batch_run) ---------------------------

# batch_run() is the Python parallel of R's batch_run(): it runs a script
# in a fresh child process and writes a sibling .log file containing every
# top-level statement echoed (`>>> ` / `... ` continuations) with output
# interleaved — the same SAS-log shape produced by R CMD BATCH and by SAS
# / Stata's native log behavior.
#
# Why a child process? Each pipeline step gets a clean Python interpreter,
# so a crash in one script doesn't poison the others, and the .log file
# captures exactly what one script does, end-to-end. Matches how R CMD
# BATCH works for R, and how `sas -sysin foo.sas` works for SAS.
#
# How it works: this function shells out to `uv run python run_with_echo.py
# <script>` and pipes the resulting stdout/stderr into the .log file. The
# AST-echo work happens inside run_with_echo.py — see that file for details.

def batch_run(
    script: str | Path,
    log_path: str | Path | None = None,
    open_: bool = True,
) -> dict:
    """Run a Python script via run_with_echo and capture its log.

    Default log_path is sibling .log (e.g. "pulls/foo.py" -> "pulls/foo.log"),
    matching R's batch_run() default and R CMD BATCH's own behavior.

    Parameters
    ----------
    script : path to the .py file to run.
    log_path : where to write the log. Defaults to script with .py replaced
        by .log.
    open_ : if True and an interactive context is available, open the log
        in the system default editor when finished. Pass False from
        non-interactive callers (run-all.py, CI).

    Returns
    -------
    dict with keys 'returncode' and 'log_path'.
    """
    script = Path(script)
    if not script.exists():
        raise FileNotFoundError(f"batch_run: script not found: {script}")

    if log_path is None:
        log_path = re.sub(r"\.py$", ".log", str(script))
        if log_path == str(script):
            log_path = f"{script}.log"
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # run_with_echo.py is expected to live next to utils.py (in src/).
    wrapper = Path(__file__).parent / "run_with_echo.py"

    with log_path.open("w", encoding="utf-8") as f:
        result = subprocess.run(
            ["uv", "run", "python", str(wrapper), str(script)],
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
        )

    if result.returncode == 0:
        print(f"batch_run OK -> {log_path}")
    else:
        print(f"batch_run: subprocess exited {result.returncode} (see {log_path})",
              file=sys.stderr)

    if open_ and sys.stdin.isatty():
        try:
            webbrowser.open(log_path.resolve().as_uri())
        except Exception:
            pass

    return {"returncode": result.returncode, "log_path": str(log_path)}


# First-time project setup --------------------------------------------------

# project_setup() is an idempotent first-run helper. It's called at the top
# of 1-download-data.py; on subsequent runs it sees .env on disk and returns
# immediately. On the FIRST run (no .env yet) it prompts for:
#   - language combination (Python-inclusive only: 2, 3, 4, 6)
#   - RAW_DATA_DIR, DATA_DIR, OUTPUT_DIR
#   - WRDS keyring credentials
#   - optional pruning of files for languages you didn't pick
# and writes .env. The .env file's existence is the "have I been set up?"
# flag — no separate state.
#
# This function is Python-only by design: it only offers language combos
# that include Python, so it can never end up deleting itself or the file
# that called it. The R sister `project_setup()` in utils.R mirrors this
# rule with R-inclusive combos.

# Combos that include Python.
_PY_COMBOS = {
    "2": ("Full Python (no R, no Stata)",      {"py"}),
    "3": ("Python + R (parallel figures and tables)", {"py", "R"}),
    "4": ("Python + Stata (Python pipeline, Stata tables)", {"py", "do"}),
    "6": ("All three (R + Python + Stata; demo / comparison mode)",
          {"py", "R", "do"}),
}


_NUMBERED = (
    "1-download-data", "2-transform-data", "3-figures",
    "4-analyze-data", "5-data-provenance",
)


def project_setup(force: bool = False) -> None:
    """First-time setup: language combo, paths, credentials, prune.

    Idempotent — returns immediately if `.env` exists. Pass force=True to
    re-run. Run from an interactive Python console (or via uv run on a
    script that calls it interactively).
    """
    if not force and Path(".env").exists():
        return

    if not sys.stdin.isatty():
        raise SystemExit(
            "project_setup: no .env file and Python is not interactive.\n"
            "  Run src/1-download-data.py interactively (e.g. via VS Code\n"
            "  or `uv run python -i src/1-download-data.py`) to walk\n"
            "  through first-time setup, then any subsequent run\n"
            "  (including run-all.py) will work."
        )

    print("\n=== First-time project setup ===\n")
    name, exts = _ask_language_combo()
    paths      = _ask_paths()
    _write_env(paths)
    _ask_credentials()
    _maybe_prune(name, exts)
    print("\nSetup complete. .env is on disk; future runs skip this prompt.\n")


def _ask_language_combo() -> tuple[str, set[str]]:
    print("Which language(s) will this project use?")
    print("  2. Full Python")
    print("  3. Python + R")
    print("  4. Python + Stata")
    print("  6. All three (default; useful for demos)")
    print()
    choice = input("Choice [6]: ").strip() or "6"
    if choice not in _PY_COMBOS:
        raise SystemExit(f"Invalid choice {choice!r}. Pick 2, 3, 4, or 6.")
    name, exts = _PY_COMBOS[choice]
    print(f"Selected: {name}")
    return name, exts


def _ask_paths() -> dict[str, str]:
    print("\n--- Paths ---")
    print("Use forward slashes (/) on Windows. These should be OUTSIDE the")
    print("project folder (e.g. a Dropbox folder), since data is not committed.")
    print()
    raw     = input("RAW_DATA_DIR (raw WRDS pulls): ").strip().replace("\\", "/")
    derived = input("DATA_DIR (derived parquets):   ").strip().replace("\\", "/")
    output  = input("OUTPUT_DIR [output]:           ").strip().replace("\\", "/")
    if not output:
        output = "output"
    return {"raw": raw, "derived": derived, "output": output}


def _write_env(paths: dict[str, str]) -> None:
    Path(".env").write_text(
        f"RAW_DATA_DIR={paths['raw']}\n"
        f"DATA_DIR={paths['derived']}\n"
        f"OUTPUT_DIR={paths['output']}\n",
        encoding="utf-8",
    )
    for d in paths.values():
        Path(d).mkdir(parents=True, exist_ok=True)
        print(f"  ready: {d}")
    print(".env written")


def _ask_credentials() -> None:
    try:
        import keyring
    except ImportError:
        print("WARNING: keyring not installed; skipping WRDS credential setup.")
        return
    print("\n--- WRDS credentials ---")
    print("Stored in your OS keyring (Windows Credential Manager / macOS")
    print("Keychain). Both R and Python read from the same entries.")
    print()
    existing = keyring.get_password("wrds", "username") or ""
    if existing:
        print(f"Existing WRDS username: {existing}")
        if input("Update? (y/n) [n]: ").strip().lower() != "y":
            return
    user = input("WRDS username: ").strip()
    pw   = getpass.getpass("WRDS password (input hidden): ")
    keyring.set_password("wrds", "username", user)
    keyring.set_password("wrds", "password", pw)
    print(f"Stored credentials for {user}.")


def _maybe_prune(combo_name: str, exts: set[str]) -> None:
    delete: list[Path] = []
    src = Path("src")
    for stem in _NUMBERED:
        for e in ("py", "R", "do"):
            f = src / f"{stem}.{e}"
            if f.exists() and e not in exts:
                delete.append(f)

    if "R" not in exts:
        for p in (src / "utils.R", src / "run-all.R"):
            if p.exists():
                delete.append(p)
    if "do" not in exts:
        f = src / "4-analyze-data.do"
        if f.exists() and f not in delete:
            delete.append(f)

    if not delete:
        print("\nNo files to prune for this combo.")
        return

    print(f"\nThe following files are not needed for {combo_name}:")
    for f in delete:
        print(f"  {f}")
    print()
    if input("Delete them? (y/n) [n]: ").strip().lower() != "y":
        print("Skipped pruning. Files left in place.")
        return
    for f in delete:
        try:
            f.unlink()
        except OSError as e:
            print(f"  WARN could not delete {f}: {e}")
        else:
            print(f"  removed {f}")
