# ==============================================================================
# 1-download-data.py
#
# Purpose:
#   Download raw WRDS tables to local parquet files. Demonstrates two
#   download approaches at increasing levels of RAM efficiency:
#     1. polars.read_database() into a DataFrame (CCM link, CRSP
#        stocknames) — simplest, full result in memory.
#     2. Chunked pyarrow.ParquetWriter via a psycopg2 server-side cursor
#        (Compustat fundq, CRSP daily, market index). Peak memory = one
#        batch, bounded regardless of table size.
#
# Inputs:
#   WRDS PostgreSQL endpoint (credentials from the OS keyring; service
#   `wrds`, keys `username` and `password`, stored once by setup).
#
# Outputs (to RAW_DATA_DIR):
#   ccm-link.parquet         CCM link table (gvkey -> permno)
#   crsp-stocknames.parquet  CRSP SIC codes by permno + date range
#   fundq-raw.parquet        Compustat quarterly fundamentals
#   crsp-dsf-v2.parquet      CRSP daily stock returns
#   crsp-index.parquet       CRSP value-weighted market index returns
#
# Notes:
#   - Run via `uv run src/1-download-data.py`. uv manages the virtual
#     env and installs dependencies from pyproject.toml automatically.
#   - download_parquet() (the wrapper for the chunked approach) defaults
#     to skip_if_exists=True, so a re-run does NOT re-pull anything
#     already on disk. Delete a file in RAW_DATA_DIR to force a refresh.
#   - Runtime: ~15-20 minutes for the full pull. CRSP daily is the long
#     pole at ~100M rows.
# ==============================================================================


# Setup ------------------------------------------------------------------------

import os
import sys
import time
from pathlib import Path

import keyring
import polars as pl
import psycopg2

# Windows terminals default to cp1252 encoding, which can't render the Unicode
# box-drawing characters that polars uses for DataFrame display. Switching
# stdout to UTF-8 fixes this.
sys.stdout.reconfigure(encoding="utf-8")
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Helpers from utils.py (download_parquet, batch_run, project_setup, ...).
from utils import download_parquet, project_setup

# First-time setup: prompts for paths, language combo, and WRDS credentials,
# then writes .env. Idempotent — instant no-op if .env already exists. The
# .env file's existence is the "have I been set up?" flag, no separate
# state needed.
project_setup()

# Load paths from .env (which project_setup() has just created or was
# already there). override=True ensures the .env file wins over any
# system-level environment variables with the same name.
load_dotenv(".env", override=True)
raw_data_dir = os.getenv("RAW_DATA_DIR")
data_dir = os.getenv("DATA_DIR")
output_dir = os.getenv("OUTPUT_DIR")


# Connect to WRDS --------------------------------------------------------------

# We use the keyring package to securely retrieve WRDS credentials from your
# operating system's credential store (Windows Credential Manager, macOS
# Keychain). This avoids putting passwords in .env files or in your code.
#
# FIRST TIME SETUP — run these lines in a Python console:
#   import keyring
#   keyring.set_password("wrds", "username", "your_wrds_username")
#   keyring.set_password("wrds", "password", "your_wrds_password")
# This stores credentials in your system's keychain. You only need to do
# this once per computer. To update after a password change, re-run above.

# psycopg2 is a PostgreSQL adapter for Python. WRDS exposes its data through
# a PostgreSQL server, so we connect to it directly with SQL.
# Credentials are retrieved inline from keyring so they never sit in a variable.
wrds = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    user=keyring.get_password("wrds", "username"),
    password=keyring.get_password("wrds", "password"),
    dbname="wrds",
    sslmode="require",
)

print(f"Connected to WRDS: {wrds.info.host}:{wrds.info.port}")


# Download CCM link table ------------------------------------------------------

# The CCM link table maps Compustat gvkey to CRSP permno over time.
# It's small (~87K rows) so we load it all at once with polars.read_database().
#
# polars.read_database() sends a SQL query to the database and returns the
# full result as a polars DataFrame. This is the simplest download method —
# fine when the table fits comfortably in RAM.
#
# The skip-if-exists guard around the download block means a replication run
# (or re-running this script) won't re-pull the file unless you delete it
# first. Matches the JAR data policy expectation that raw inputs are
# preserved verbatim from the analyst's original pull.

ccm_path = f"{raw_data_dir}/ccm-link.parquet"

if Path(ccm_path).exists():
    print(f"Skipping CCM link download — file exists: {ccm_path}")
else:
    start = time.time()

    ccm_link = pl.read_database(
        "SELECT * FROM crsp.ccmxpf_lnkhist",
        connection=wrds,
    )

    print(f"CCM link: {ccm_link.shape[0]:,} rows, {time.time() - start:.1f}s")
    print(ccm_link.head())

    ccm_link.write_parquet(ccm_path)


# Download CRSP stocknames_v2 --------------------------------------------------

# This table provides time-varying SIC codes by permno and date range.
# Small table, so polars.read_database() is fine.

stocknames_path = f"{raw_data_dir}/crsp-stocknames.parquet"

if Path(stocknames_path).exists():
    print(f"Skipping stocknames download — file exists: {stocknames_path}")
else:
    start = time.time()

    stocknames = pl.read_database(
        "SELECT permno, namedt, nameenddt, siccd FROM crsp.stocknames_v2",
        connection=wrds,
    )

    print(f"Stocknames: {stocknames.shape[0]:,} rows, {time.time() - start:.1f}s")

    stocknames.write_parquet(stocknames_path)


# Download Compustat fundq (CHUNKED DOWNLOAD APPROACH) -------------------------

# Compustat quarterly fundamentals is a medium-large table. Rather than loading
# the whole thing into memory, we use a server-side cursor to stream rows in
# batches, writing each batch to a single parquet file using pyarrow's
# ParquetWriter.
#
# How it works:
#   - cursor(name=...) creates a SERVER-SIDE cursor on WRDS. Passing a name
#     is what makes it server-side — without a name, psycopg2 creates a
#     client-side cursor that loads everything into memory at execute() time.
#   - cursor.fetchmany(BATCH_SIZE) pulls BATCH_SIZE rows at a time.
#   - We convert each batch to a pyarrow Table and append it to the parquet file.
#   - Peak memory = one batch, not the full table.
#
# FIRST I DEMONSTRATE THIS APPROACH with all the details visible. After this,
# we use the download_parquet() wrapper from utils.py which does the same thing
# but hides the boilerplate.

BATCH_SIZE = 50_000

# Goes to RAW_DATA_DIR — raw inputs live separately from the derived parquets
# scripts 2-4 produce.
fundq_path = f"{raw_data_dir}/fundq-raw.parquet"

# We send raw SQL to WRDS with standard Compustat filters applied server-side
# to reduce the download size. Depending on your needs, you might download
# the entire table and filter locally for reusability.
fundq_sql = """
    SELECT gvkey, datadate, fyearq, fqtr, rdq,
           conm, cusip, cik,
           saleq, ibq, epspiq, atq, cshoq, prccq, ajexq
    FROM comp.fundq
    WHERE indfmt = 'INDL' AND datafmt = 'STD'
      AND popsrc = 'D' AND consol = 'C'
      AND fyearq >= 1970
      AND rdq IS NOT NULL
"""

# Skip-if-exists guard: don't re-pull fundq if it's already on disk.
# Delete the file to force a re-download.
if Path(fundq_path).exists():
    print(f"Skipping fundq download — file exists: {fundq_path}")
else:
    start = time.time()

    # Open a server-side cursor on WRDS. No data is transferred yet — the
    # database holds the query plan ready to stream results when we ask.
    cursor = wrds.cursor(name="fundq_download")
    cursor.execute(fundq_sql)

    # We'll create the ParquetWriter on the first batch (we need its schema first).
    writer = None
    total_rows = 0

    # Loop: fetch batches until the server has no more rows.
    while True:
        # Pull the next BATCH_SIZE rows. Returns a list of tuples (one per row).
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        total_rows += len(rows)
        elapsed = time.time() - start
        size_mb = Path(fundq_path).stat().st_size / 1e6 if Path(fundq_path).exists() else 0
        print(
            f"\r  {total_rows:,} rows | {elapsed / 60:.1f} min | "
            f"~{size_mb:.0f} MB on disk",
            end="",
        )

        # Convert the list of tuples to a pyarrow Table.
        # cursor.description gives us the column names from the SQL result.
        col_names = [desc[0] for desc in cursor.description]
        # Transpose rows (list of tuples) into columns (dict of lists)
        col_data = {col: [row[i] for row in rows] for i, col in enumerate(col_names)}
        table = pa.Table.from_pydict(col_data)

        # Cast DECIMAL columns to float64. PostgreSQL NUMERIC precision can vary
        # between batches (e.g., decimal128(9,4) then decimal128(8,4)), which
        # causes a schema mismatch error in the ParquetWriter. Casting to float64
        # is standard for financial data and avoids this issue.
        new_schema = pa.schema([
            pa.field(f.name, pa.float64()) if pa.types.is_decimal(f.type) else f
            for f in table.schema
        ])
        table = table.cast(new_schema)

        # First batch: create the ParquetWriter using the batch's schema.
        # We use zstd compression — good ratio, fast to read back.
        if writer is None:
            writer = pq.ParquetWriter(fundq_path, table.schema, compression="zstd")

        # Append this batch to the parquet file as a new row group.
        writer.write_table(table)

    # Finalize the parquet file (writes footer metadata — required for valid file).
    if writer is not None:
        writer.close()

    # Close the server-side cursor to free resources on WRDS.
    cursor.close()

    print()  # newline after progress line
    elapsed = time.time() - start
    print(f"Saved {total_rows:,} rows, {Path(fundq_path).stat().st_size / 1e6:.1f} MB, {elapsed:.0f}s")


# Download CRSP daily returns (download_parquet wrapper) --------------------------

# CRSP daily stock file (v2) is the largest table — about 100M rows.
# Now that you've seen the raw chunked loop above (for fundq), here we use
# download_parquet() from utils.py. It does the same thing — server-side cursor
# + fetchmany + ParquetWriter — but handles the boilerplate for you.
#
# CRSP v2 column names:
#   dsf_v2:            dlycaldt (date), dlyret (return)
#   inddlyseriesdata:  dlycaldt, dlytotret (market return), indno (index ID)
#   indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca VW market index

print("\nDownloading CRSP daily returns (this will take 10-15+ minutes)...")

# download_parquet() defaults to skip_if_exists=True, so re-running this script
# does not re-pull tables that are already on disk.
start = time.time()

download_parquet(
    sql="SELECT permno, dlycaldt, dlyret FROM crsp.dsf_v2 WHERE dlycaldt >= '1970-01-01'",
    output_path=f"{raw_data_dir}/crsp-dsf-v2.parquet",
    connection=wrds,
)

print(f"  {time.time() - start:.0f}s total")


# Market index returns ---------------------------------------------------------

# The market index is used to compute abnormal returns (BHAR).
# indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca value-weighted market index.

start = time.time()

download_parquet(
    sql="""SELECT dlycaldt, dlytotret
           FROM crsp.inddlyseriesdata
           WHERE indno = 1000200
             AND dlycaldt >= '1970-01-01'""",
    output_path=f"{raw_data_dir}/crsp-index.parquet",
    connection=wrds,
)

print(f"  {time.time() - start:.0f}s total")


# Disconnect from WRDS ---------------------------------------------------------

wrds.close()

print("\nDone. Parquet files saved to:", raw_data_dir)
print("  ccm-link.parquet         — CCM link table (gvkey -> permno)")
print("  crsp-stocknames.parquet  — CRSP SIC codes by permno + date range")
print("  fundq-raw.parquet        — Compustat quarterly fundamentals")
print("  crsp-dsf-v2.parquet      — CRSP daily stock returns")
print("  crsp-index.parquet       — CRSP daily market index returns")
print("\nNext: run src/2-transform-data.py to merge and create variables.")
