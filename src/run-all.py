# run-all.py — Run the full pipeline with logging
# ===========================================================================
# HOW TO RUN:
#   uv run src/run-all.py
#
# A log file will be created in the log/ folder capturing all console output.
#
# This master script:
#   1. Starts a log file
#   2. Runs scripts 1-4 in order (Python versions)
#   3. Exports sample identifiers (gvkey, permno, rdq) for replication
#   4. Prints data provenance (file dates, sizes)
#   5. Closes the log
#
# JAR Data Policy notes:
#   The Journal of Accounting Research requires authors to provide:
#   - Code that converts raw data into final datasets and produces tables
#   - A comprehensive log file showing the execution of the entire code
#   - Identifiers (e.g., gvkey, permno) for the final sample
#   This master script and its output satisfy those requirements.
# ===========================================================================

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")


class TeeLogger:
    """Write to both a log file and the console simultaneously."""

    def __init__(self, log_path: str):
        self.log_file = open(log_path, "w", encoding="utf-8")
        self.console = sys.__stdout__

    def write(self, text):
        self.console.write(text)
        self.log_file.write(text)
        self.log_file.flush()

    def flush(self):
        self.console.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


def run_script(script_path: str, logger):
    """Run a Python script as a subprocess, streaming output to the logger."""
    print(f"\n{'=' * 70}")
    print(f"Running: {script_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}\n")

    result = subprocess.run(
        ["uv", "run", "python", script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    # Write the script's output to our log
    print(result.stdout)

    if result.returncode != 0:
        print(f"ERROR: {script_path} exited with code {result.returncode}")
        sys.exit(result.returncode)

    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    # --- Start logging ---
    os.makedirs("log", exist_ok=True)
    log_file = f"log/run-all-{datetime.now().strftime('%Y-%m-%d')}.log"
    logger = TeeLogger(log_file)
    sys.stdout = logger

    print(f"Pipeline started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")

    # --- Load environment ---
    load_dotenv(".env", override=True)
    data_dir = os.getenv("DATA_DIR")
    output_dir = os.getenv("OUTPUT_DIR")
    print(f"DATA_DIR: {data_dir}")
    print(f"OUTPUT_DIR: {output_dir}")

    # --- Run scripts 1-4 ---

    start_all = time.time()

    run_script("src/1-download-data.py", logger)
    run_script("src/2-transform-data.py", logger)
    run_script("src/3-figures.py", logger)
    run_script("src/4-analyze-data.py", logger)

    # --- Export sample identifiers ---

    print(f"\n{'=' * 70}")
    print("Exporting sample identifiers")
    print(f"{'=' * 70}\n")

    # JAR requires "whenever feasible, authors should provide the identifiers
    # (e.g., CIK, CUSIP) of all the observations that make up the final sample."
    regdata = pl.read_parquet(f"{data_dir}/regdata.parquet")

    sample_ids = (regdata
        .select("gvkey", "permno", "rdq", "datadate", "fyearq", "fqtr")
        .sort("gvkey", "rdq")
    )

    sample_ids.write_parquet(f"{data_dir}/sample-identifiers.parquet")
    sample_ids.to_pandas().to_csv(f"{data_dir}/sample-identifiers.csv", index=False)

    print(f"Sample identifiers: {sample_ids.shape[0]:,} rows")
    print(f"Distinct gvkeys: {sample_ids['gvkey'].n_unique():,}")
    rdq_range = sample_ids["rdq"]
    print(f"RDQ range: {rdq_range.min()} to {rdq_range.max()}")

    # --- Data provenance ---

    print(f"\n{'=' * 70}")
    print("Data provenance")
    print(f"{'=' * 70}\n")

    raw_files = ["ccm-link.parquet", "crsp-stocknames.parquet",
                 "fundq-raw.parquet", "crsp-dsf-v2.parquet", "crsp-index.parquet"]

    derived_files = ["regdata.parquet", "regdata.dta", "figure-data.parquet",
                     "trading-dates.parquet", "sample-identifiers.parquet",
                     "sample-selection.parquet"]

    def print_file_info(files, directory):
        for f in files:
            path = Path(directory) / f
            if path.exists():
                stat = path.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
                size_mb = stat.st_size / 1e6
                print(f"  {f:<40s}  {mtime}  {size_mb:.1f} MB")

    print("Raw data files (downloaded from WRDS):")
    print_file_info(raw_files, data_dir)

    print("\nDerived data files:")
    print_file_info(derived_files, data_dir)

    print("\nOutput files (tables and figures):")
    out_files = [f.name for f in Path(output_dir).iterdir()
                 if f.suffix in (".tex", ".pdf", ".png", ".docx", ".rtf")]
    print_file_info(sorted(out_files), output_dir)

    elapsed = time.time() - start_all
    print(f"\nPipeline finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed / 60:.1f} minutes")

    # --- Stop logging ---
    sys.stdout = logger.console
    logger.close()
    print(f"Log saved to: {log_file}")


if __name__ == "__main__":
    main()
