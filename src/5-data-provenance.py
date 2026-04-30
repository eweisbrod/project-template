# 5-data-provenance.py — sample identifiers + raw/derived/output provenance
# ============================================================================
# This is the JAR Data and Code Sharing Policy artifact step. It produces
# two things:
#
#   1. sample-identifiers.{parquet,csv} in DATA_DIR — the gvkey / permno /
#      rdq triples for every observation in the regression sample, sorted
#      by gvkey then rdq. JAR asks for these so a replicator can verify
#      the final sample even without WRDS access.
#
#   2. A printed table of every raw, derived, and output file with its
#      mtime, size, and SHA256 hash. Because this script is run via
#      batch_run() (which spawns it through run_with_echo.py), the table
#      lands inside this script's .log file with surrounding "Started:"
#      timestamp and the full echoed source — so the .log itself is the
#      provenance log.
#
# Run as part of run-all.py, or standalone via:
#   uv run python -c "from utils import batch_run; batch_run('src/5-data-provenance.py')"
# ============================================================================


import hashlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(".env", override=True)
raw_data_dir = os.getenv("RAW_DATA_DIR")
data_dir     = os.getenv("DATA_DIR")
output_dir   = os.getenv("OUTPUT_DIR")

# Run metadata
print(datetime.now())
print(f"raw_data_dir = {raw_data_dir}")
print(f"data_dir     = {data_dir}")
print(f"output_dir   = {output_dir}")


# Export sample identifiers ----------------------------------------------------

# JAR: "whenever feasible, authors should provide the identifiers (e.g.,
# CIK, CUSIP) of all the observations that make up the final sample."

regdata = pl.read_parquet(f"{data_dir}/regdata.parquet")

sample_ids = (regdata
    .select("gvkey", "permno", "rdq", "datadate", "fyearq", "fqtr")
    .sort("gvkey", "rdq")
)

sample_ids.write_parquet(f"{data_dir}/sample-identifiers.parquet")
sample_ids.to_pandas().to_csv(f"{data_dir}/sample-identifiers.csv", index=False)

# Sample summary
print(f"rows           = {sample_ids.shape[0]:,}")
print(f"distinct gvkey = {sample_ids['gvkey'].n_unique():,}")
print(f"rdq range      = {sample_ids['rdq'].min()} to {sample_ids['rdq'].max()}")


# File provenance: mtime, size, SHA256 -----------------------------------------

# SHA256 hashes prove a JAR replicator's downloaded data matches the
# original analyst's. mtime alone gets clobbered by zip / copy / download;
# the hash is content-addressed and survives.

def print_file_info(files, directory):
    if not directory:
        print("  (env var not set; skipping)")
        return
    for f in files:
        path = Path(directory) / f
        if not path.exists():
            print(f"  {f:<40s}  (missing)")
            continue
        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        size_mb = path.stat().st_size / 1e6
        mtime = time.strftime("%Y-%m-%d %H:%M",
                              time.localtime(path.stat().st_mtime))
        print(f"  {f:<40s}  {mtime}  {size_mb:>8.1f} MB  sha256={h.hexdigest()}")


raw_files = ["ccm-link.parquet", "crsp-stocknames.parquet",
             "fundq-raw.parquet", "crsp-dsf-v2.parquet",
             "crsp-index.parquet"]

derived_files = ["regdata.parquet", "regdata.dta",
                 "figure-data.parquet", "trading-dates.parquet",
                 "sample-selection.parquet", "sample-selection.dta",
                 "sample-identifiers.parquet", "sample-identifiers.csv"]

print("Raw data files (RAW_DATA_DIR):")
print_file_info(raw_files, raw_data_dir)

print("\nDerived data files (DATA_DIR):")
print_file_info(derived_files, data_dir)

print("\nOutput files (OUTPUT_DIR):")
out_files = sorted(
    f.name for f in Path(output_dir).iterdir()
    if f.suffix in (".tex", ".pdf", ".png", ".docx", ".rtf")
)
print_file_info(out_files, output_dir)
