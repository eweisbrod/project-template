# 5-data-provenance.py — sample identifiers + raw/derived/output inventory
# ============================================================================
# Two artifacts:
#
#   1. sample-identifiers.{parquet,csv} in DATA_DIR — the gvkey / permno /
#      rdq triples for every observation in the regression sample. Useful
#      to a co-author or future-self for reconstructing the sample
#      without re-running scripts 1-2.
#
#   2. A printed inventory of every file in RAW_DATA_DIR, DATA_DIR, and
#      OUTPUT_DIR with mtime, size, and SHA256 hash. Run via batch_run()
#      so the inventory lands inside this script's .log file.
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
# CIK, CUSIP) of all the observations that make up the final sample." A
# replicator with their own WRDS access can use these to verify the
# sample without rerunning scripts 1-2.

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


# File inventory ---------------------------------------------------------------

def list_dir(directory):
    if not directory or not Path(directory).is_dir():
        print("  (directory not set or missing; skipping)")
        return
    for f in sorted(Path(directory).iterdir()):
        if not f.is_file():
            continue
        h = hashlib.sha256()
        with f.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        size_mb = f.stat().st_size / 1e6
        mtime = time.strftime("%Y-%m-%d %H:%M",
                              time.localtime(f.stat().st_mtime))
        print(f"  {f.name:<40s}  {mtime}  {size_mb:>8.1f} MB  sha256={h.hexdigest()}")


# Raw data (RAW_DATA_DIR)
list_dir(raw_data_dir)

# Derived data (DATA_DIR)
list_dir(data_dir)

# Output (OUTPUT_DIR)
list_dir(output_dir)
