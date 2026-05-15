# ==============================================================================
# 5-data-provenance.py
#
# Purpose:
#   Produce the JAR-style replication artifacts: sample identifiers and
#   a file inventory of RAW_DATA_DIR / DATA_DIR / OUTPUT_DIR with mtime,
#   size, and SHA256 hash for every file.
#
# Inputs (from DATA_DIR):
#   regdata.parquet
#
# Outputs (to DATA_DIR):
#   sample-identifiers.parquet
#   sample-identifiers.csv
#
# Notes:
#   - Intended to be run via batch_run() so the printed inventory lands
#     inside the script's .log file. The .log itself becomes the
#     provenance log shipped with the rest of the JAR data package.
#   - Standalone run:
#       uv run python -c "from utils import batch_run; batch_run('src/5-data-provenance.py')"
# ==============================================================================


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

# Function header below is a docstring — Python's standard for
# function-level documentation (PEP 257). The format we use here is
# Google-style (with explicit `Args:` and `Returns:` sections); NumPy
# style and reStructuredText are also common. IDEs and tools like Sphinx
# render docstrings as in-editor help and as published API docs.

def list_dir(directory):
    """Print a directory listing with mtime, size, and SHA256 hash per file.

    Used to record what was on disk at the time the script ran. Bails out
    cleanly if `directory` is unset or missing so the rest of the script
    can still report what it can find.

    Args:
        directory: Path-like to the directory to inventory. None, empty
            string, or a non-existent path all produce a "skipping"
            message and return early.

    Returns:
        None. Called for the printing side effect.
    """
    if not directory or not Path(directory).is_dir():
        print("  (directory not set or missing; skipping)")
        return

    # iterdir() yields Path objects (not just basenames); sort by name
    # for deterministic order across runs.
    for f in sorted(Path(directory).iterdir(), key=lambda p: p.name):
        if not f.is_file():
            continue  # one-level only; skip subdirectories

        # Stream the file in 1 MB chunks to compute SHA256 without
        # loading multi-GB parquets into RAM.
        h = hashlib.sha256()
        with f.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)

        size_mb = f.stat().st_size / 1e6
        mtime = time.strftime("%Y-%m-%d %H:%M",
                              time.localtime(f.stat().st_mtime))
        # f-string column widths line everything up so the inventory
        # reads like a table.
        print(f"  {f.name:<40s}  {mtime}  {size_mb:>8.1f} MB  sha256={h.hexdigest()}")


# Raw data (RAW_DATA_DIR)
list_dir(raw_data_dir)

# Derived data (DATA_DIR)
list_dir(data_dir)

# Output (OUTPUT_DIR)
list_dir(output_dir)
