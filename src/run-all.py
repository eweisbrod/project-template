# ==============================================================================
# run-all.py
#
# Purpose:
#   Master orchestrator that runs the full pipeline in language-aware
#   order (Python steps via run_with_echo.py; the Stata .do file via
#   batch_run_stata if it is on disk after setup pruning).
#
# Inputs:
#   src/1-download-data.py, src/2-transform-data.py, src/3-figures.py,
#   src/4-analyze-data.py, src/5-data-provenance.py, and conditionally
#   src/4-analyze-data.do (if a Stata-inclusive combo was chosen).
#
# Outputs (to log/):
#   1-download-data.log, 2-transform-data.log, 3-figures.log,
#   4-analyze-data.log, 5-data-provenance.log, and conditionally
#   4-analyze-data-stata.log. Each is the .log produced by batch_run()
#   / batch_run_stata().
#
# Notes:
#   - Run via `uv run src/run-all.py`.
#   - This script does NOT capture its own output. The per-script logs
#     ARE the audit trail; the orchestration here is just "call the
#     next batch_run."
#   - A fresh run overwrites the previous run's logs. The file mtime
#     and the "Started:" line inside each .log tell you when it was
#     produced.
#   - The R sister `run-all.R` uses R CMD BATCH for its per-script logs;
#     SAS and Stata produce the same visually-consistent SAS-log shape
#     natively via their respective batch runners.
# ==============================================================================

import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from utils import batch_run, batch_run_stata

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
    ]
    for script in scripts:
        log_path = log_dir / Path(script).with_suffix(".log").name
        result = batch_run(script, log_path=log_path, open_=False)
        if result["returncode"] != 0:
            print(f"ABORT: {script} exited {result['returncode']}; "
                  f"see {log_path}", file=sys.stderr)
            sys.exit(result["returncode"])

    # If the user picked a Stata-inclusive combo at setup, the .do
    # file is still on disk and we run it too. If they picked a
    # Python-only or R-only combo, project_setup() deleted the .do
    # file during pruning, so the check below is False and Stata is
    # skipped.
    if Path("src/4-analyze-data.do").exists():
        batch_run_stata("src/4-analyze-data.do",
                        log_path=log_dir / "4-analyze-data-stata.log")

    # Provenance step runs last so it inventories the Stata outputs too.
    result = batch_run("src/5-data-provenance.py",
                       log_path=log_dir / "5-data-provenance.log",
                       open_=False)
    if result["returncode"] != 0:
        sys.exit(result["returncode"])

    elapsed = time.time() - start_all
    print(f"\nPipeline complete in {elapsed/60:.1f} min. Logs in: log/")


if __name__ == "__main__":
    main()
