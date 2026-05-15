# ==============================================================================
# run-all.R
#
# Purpose:
#   Master orchestrator that runs the full pipeline in language-aware
#   order (R steps via R CMD BATCH; the Stata .do file via Stata if it
#   is on disk after setup pruning).
#
# Inputs:
#   src/1-download-data.R, src/2-transform-data.R, src/3-figures.R,
#   src/4-analyze-data.R, src/5-data-provenance.R, and conditionally
#   src/4-analyze-data.do (if a Stata-inclusive combo was chosen).
#
# Outputs (to log/):
#   1-download-data.Rout, 2-transform-data.Rout, 3-figures.Rout,
#   4-analyze-data.Rout, 5-data-provenance.Rout, and conditionally
#   4-analyze-data-stata.log. Each is the .Rout / .log produced by
#   batch_run() / batch_run_stata().
#
# Notes:
#   - Open in RStudio and run interactively (Ctrl+A, Ctrl+Enter), or
#     step through line by line.
#   - This script does NOT capture its own output. The per-script logs
#     ARE the audit trail; the orchestration here is just "call the
#     next batch_run." If you want a master log too, run from a
#     terminal: `R CMD BATCH src/run-all.R run-all.Rout`.
#   - A fresh run overwrites the previous run's logs. The file mtime
#     and the proc.time block inside each .Rout tell you when it was
#     produced.
# ==============================================================================


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
