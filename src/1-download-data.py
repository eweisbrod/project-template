# 1-download-data.py
# ===========================================================================
# Download data for the earnings event study to local parquet files.
#
# This script downloads raw tables from WRDS (Wharton Research Data Services)
# and saves them as parquet files. It showcases two download approaches:
#
#   1. Small tables (CCM link, stocknames): load the full result into a polars
#      DataFrame with polars.read_database(), then write to parquet.
#   2. Large tables (fundq, CRSP daily): stream rows in batches via a
#      server-side cursor, writing each batch to parquet with pyarrow's
#      ParquetWriter. Peak memory = one batch, not the full table.
#
# WHY TWO METHODS?
# polars.read_database() loads the entire result into memory. This is fine
# for small tables, but for large ones like CRSP daily (~100M rows) it can
# exceed your RAM and crash Python. The chunked ParquetWriter approach
# streams data in batches, so memory stays bounded regardless of table size.
#
# HOW TO RUN:
#   uv run src/1-download-data.py
# uv automatically manages the virtual environment and installs dependencies
# from pyproject.toml — no manual pip install or venv activation needed.
# ===========================================================================


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

# Load paths from the .env file. Each user has their own .env pointing to
# their local data and output directories (see .env.example).
# override=True ensures the .env file wins over any system-level environment
# variables with the same name (e.g., DATA_DIR set from another project).
load_dotenv(".env", override=True)
data_dir = os.getenv("DATA_DIR")
output_dir = os.getenv("OUTPUT_DIR")

# Import the download_wrds() helper from utils.py.
from utils import download_wrds


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

start = time.time()

ccm_link = pl.read_database(
    "SELECT * FROM crsp.ccmxpf_lnkhist",
    connection=wrds,
)

print(f"CCM link: {ccm_link.shape[0]:,} rows, {time.time() - start:.1f}s")
print(ccm_link.head())

# polars writes parquet natively via Arrow — no conversion needed.
ccm_link.write_parquet(f"{data_dir}/ccm-link.parquet")


# Download CRSP stocknames_v2 --------------------------------------------------

# This table provides time-varying SIC codes by permno and date range.
# Small table, so polars.read_database() is fine.

start = time.time()

stocknames = pl.read_database(
    "SELECT permno, namedt, nameenddt, siccd FROM crsp.stocknames_v2",
    connection=wrds,
)

print(f"Stocknames: {stocknames.shape[0]:,} rows, {time.time() - start:.1f}s")

stocknames.write_parquet(f"{data_dir}/crsp-stocknames.parquet")


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
# we use the download_wrds() wrapper from utils.py which does the same thing
# but hides the boilerplate.

BATCH_SIZE = 50_000

fundq_path = f"{data_dir}/fundq-raw.parquet"

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


# Download CRSP daily returns (download_wrds wrapper) --------------------------

# CRSP daily stock file (v2) is the largest table — about 100M rows.
# Now that you've seen the raw chunked loop above (for fundq), here we use
# download_wrds() from utils.py. It does the same thing — server-side cursor
# + fetchmany + ParquetWriter — but handles the boilerplate for you.
#
# CRSP v2 column names:
#   dsf_v2:            dlycaldt (date), dlyret (return)
#   inddlyseriesdata:  dlycaldt, dlytotret (market return), indno (index ID)
#   indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca VW market index

print("\nDownloading CRSP daily returns (this will take 10-15+ minutes)...")

start = time.time()

download_wrds(
    sql="SELECT permno, dlycaldt, dlyret FROM crsp.dsf_v2 WHERE dlycaldt >= '1970-01-01'",
    output_path=f"{data_dir}/crsp-dsf-v2.parquet",
    connection=wrds,
)

print(f"  {time.time() - start:.0f}s total")


# Market index returns ---------------------------------------------------------

# The market index is used to compute abnormal returns (BHAR).
# indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca value-weighted market index.

start = time.time()

download_wrds(
    sql="""SELECT dlycaldt, dlytotret
           FROM crsp.inddlyseriesdata
           WHERE indno = 1000200
             AND dlycaldt >= '1970-01-01'""",
    output_path=f"{data_dir}/crsp-index.parquet",
    connection=wrds,
)

print(f"  {time.time() - start:.0f}s total")


# Disconnect from WRDS ---------------------------------------------------------

wrds.close()

print("\nDone. Parquet files saved to:", data_dir)
print("  ccm-link.parquet         — CCM link table (gvkey -> permno)")
print("  crsp-stocknames.parquet  — CRSP SIC codes by permno + date range")
print("  fundq-raw.parquet        — Compustat quarterly fundamentals")
print("  crsp-dsf-v2.parquet      — CRSP daily stock returns")
print("  crsp-index.parquet       — CRSP daily market index returns")
print("\nNext: run src/2-transform-data.py to merge and create variables.")
