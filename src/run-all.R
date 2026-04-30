# run-all.R — Run the full pipeline; each script writes its own .Rout
# ============================================================================
# HOW TO RUN: Open this script in RStudio and run interactively
# (Ctrl+A, Ctrl+Enter), or step through line by line.
#
# Each numbered script is executed via batch_run(), which calls R CMD BATCH
# in a fresh child process and writes a sibling .Rout file capturing:
#   - R version banner + start timestamp + working directory
#   - Every command echoed with `> ` / `+ `
#   - Output, messages, warnings, errors interleaved
#   - proc.time() at the end (user/system/elapsed seconds)
#
# The five .Rout files (1-download, 2-transform, 3-figures, 4-analyze,
# 5-provenance) for one pipeline run land in log/<script>.Rout. A fresh
# run overwrites the previous run's logs — the file mtime and the
# proc.time block inside each .Rout tell you when it was produced.
# Together they are the JAR Data and Code Sharing Policy artifacts:
#   1. The .R files (committed) are the code that produced the results.
#   2. The .Rout files are the comprehensive logs of execution.
#   3. 5-data-provenance.R writes sample-identifiers.{parquet,csv} into
#      DATA_DIR — the regression-sample row identifiers.
#
# This script does NOT capture its own output (no run-all.log). The
# per-script .Rout files ARE the logs; the orchestration here is just
# "call the next batch_run." If you ever want a master log, you can
# `R CMD BATCH src/run-all.R run-all.Rout` from a terminal — but it's
# normally not needed.
# ============================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv)

source("src/utils.R")  # provides batch_run() and download_parquet()

load_dot_env(".env")


# Steps 1-5 via batch_run() ---------------------------------------------------

# Each batch_run() spawns R CMD BATCH in a child process. open = FALSE so
# we don't pop up RStudio editor tabs for every script in the pipeline.
# Logs go to log/<script>.Rout — a fresh run overwrites the previous run.

dir.create("log", showWarnings = FALSE, recursive = TRUE)

batch_run("src/1-download-data.R",
          log_path = "log/1-download-data.Rout", open = FALSE)

batch_run("src/2-transform-data.R",
          log_path = "log/2-transform-data.Rout", open = FALSE)

batch_run("src/3-figures.R",
          log_path = "log/3-figures.Rout", open = FALSE)

batch_run("src/4-analyze-data.R",
          log_path = "log/4-analyze-data.Rout", open = FALSE)

# If the user picked a Stata-inclusive combo at setup, the .do file
# is still on disk and we run it too. If they picked an R-only or
# Python-only combo, project_setup() deleted the .do file during
# pruning, so the check below is FALSE and Stata is skipped.
if (file.exists("src/4-analyze-data.do")) {
  batch_run_stata("src/4-analyze-data.do",
                  log_path = "log/4-analyze-data-stata.log")
}

batch_run("src/5-data-provenance.R",
          log_path = "log/5-data-provenance.Rout", open = FALSE)

cat("\nPipeline complete. Logs in: log/\n")
