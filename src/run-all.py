# run-all.py — Run the full pipeline; each script writes its own .log
# ============================================================================
# HOW TO RUN:
#   uv run src/run-all.py
#
# Each numbered script is executed via batch_run() (utils.py), which spawns
# a fresh Python subprocess via run_with_echo.py and writes a sibling .log
# capturing:
#   - "Started: ..." timestamp at the top.
#   - Every top-level statement echoed with `>>> ` / `... ` continuations.
#   - print() output interleaved with the statements.
#   - Warnings written to stderr captured into the same log.
#
# The five .log files (1-download, 2-transform, 3-figures, 4-analyze,
# 5-provenance) for one pipeline run land in log/<script>.log. A fresh
# run overwrites the previous run's logs — the file mtime and the
# "Started:" line inside each script tell you when it was produced.
# Together they are the JAR Data and Code Sharing Policy artifacts:
#   1. The .py files (committed) are the code that produced the results.
#   2. The .log files are the comprehensive logs of execution.
#   3. 5-data-provenance.py writes sample-identifiers.{parquet,csv} into
#      DATA_DIR — the regression-sample row identifiers.
#
# This script does NOT capture its own output — the per-script .log files
# ARE the logs; the orchestration here is just "call the next batch_run."
#
# The R-only sister template (project-template-r) uses an equivalent
# batch_run() that calls R CMD BATCH instead of run_with_echo.py. SAS and
# Stata produce the same SAS-log shape natively. All four pipeline
# languages emit visually consistent per-script logs.
# ============================================================================

import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from utils import batch_run

sys.stdout.reconfigure(encoding="utf-8")


def main() -> None:
    # --- Load environment ---
    load_dotenv(".env", override=True)
    raw_data_dir = os.getenv("RAW_DATA_DIR")
    data_dir     = os.getenv("DATA_DIR")
    output_dir   = os.getenv("OUTPUT_DIR")
    print(f"RAW_DATA_DIR: {raw_data_dir}")
    print(f"DATA_DIR:     {data_dir}")
    print(f"OUTPUT_DIR:   {output_dir}")

    # --- Steps 1-5 via batch_run() ---
    # open_=False so we don't pop up an editor for every script in the run.
    # Logs go to log/<script>.log — a fresh run overwrites the previous run.
    log_dir = Path("log")
    log_dir.mkdir(parents=True, exist_ok=True)

    start_all = time.time()
    scripts = [
        "src/1-download-data.py",
        "src/2-transform-data.py",
        "src/3-figures.py",
        "src/4-analyze-data.py",
        "src/5-data-provenance.py",
    ]
    for script in scripts:
        log_path = log_dir / Path(script).with_suffix(".log").name
        result = batch_run(script, log_path=log_path, open_=False)
        if result["returncode"] != 0:
            print(f"ABORT: {script} exited {result['returncode']}; "
                  f"see {log_path}", file=sys.stderr)
            sys.exit(result["returncode"])

    elapsed = time.time() - start_all
    print(f"\nPipeline complete in {elapsed/60:.1f} min. Logs in: log/")


if __name__ == "__main__":
    main()
