# run-all.R — Run the full pipeline with logging
# ===========================================================================
# HOW TO RUN: Open this script in RStudio and run it interactively.
# Select all (Ctrl+A) then Run (Ctrl+Enter), or step through line by line.
#
# Do NOT use source("src/run-all.R") — RStudio's source() function interferes
# with sink() and produces a blank log file. This is a known RStudio limitation.
#
# A log file will be created in the log/ folder capturing all console output
# including the source code of each script (echo = TRUE, max.deparse.length = Inf).
#
# This master script:
#   1. Starts a log file
#   2. Runs scripts 1-4 in order (with code echoed to the log)
#   3. Exports sample identifiers (gvkey, permno, rdq) for replication
#   4. Prints data provenance (file dates, sizes)
#   5. Closes the log
#
# Prerequisites:
#   - Run src/setup.R first (creates .env and stores WRDS credentials)
#   - Script 1 requires a WRDS connection (downloads take ~15-20 minutes)
#   - Scripts 2-4 run locally on the downloaded data (~5-10 minutes total)
#
# JAR Data Policy notes:
#   The Journal of Accounting Research requires authors to provide:
#   - Code that converts raw data into final datasets and produces tables
#   - A comprehensive log file showing the execution of the entire code
#   - Identifiers (e.g., gvkey, permno) for the final sample
#   This master script and its output satisfy those requirements.
# ===========================================================================


# Start logging ----------------------------------------------------------------

dir.create("log", showWarnings = FALSE)

# sink() with split = TRUE sends output to both the log file and the console
log_file <- paste0("log/run-all-", Sys.Date(), ".log")
log_con  <- file(log_file, open = "wt")
# Only sink standard output, not messages. RStudio adds ANSI color codes to
# message() output which show up as garbled G3; characters in the log file.
# Messages will still appear in the console, just not in the log.
sink(log_con, split = TRUE)

cat("Pipeline started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("R version:", R.version.string, "\n")


# Load environment variables ---------------------------------------------------

library(dotenv)
library(glue)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

data_dir
output_dir


# Step 1: Download data from WRDS ---------------------------------------------

# echo = TRUE prints each line of code before executing it, so the log
# shows both the commands and their output — exactly what JAR requires.
source("src/1-download-data.R", echo = TRUE, max.deparse.length = Inf)


# Step 2: Transform data -------------------------------------------------------

source("src/2-transform-data.R", echo = TRUE, max.deparse.length = Inf)


# Step 3: Figures ---------------------------------------------------------------

source("src/3-figures.R", echo = TRUE, max.deparse.length = Inf)


# Step 4: Tables (LaTeX) -------------------------------------------------------

source("src/4-analyze-data-and-tabulate-latex.R", echo = TRUE, max.deparse.length = Inf)


# Export sample identifiers ----------------------------------------------------

# JAR requires "whenever feasible, authors should provide the identifiers
# (e.g., CIK, CUSIP) of all the observations that make up the final sample."
# We export gvkey, permno, datadate, and rdq for the regression sample.

library(arrow)

regdata <- read_parquet(glue("{data_dir}/regdata.parquet"))

sample_ids <- regdata |>
  dplyr::select(gvkey, permno, rdq, datadate, fyearq, fqtr) |>
  dplyr::arrange(gvkey, rdq)

write_parquet(sample_ids, glue("{data_dir}/sample-identifiers.parquet"))
write.csv(sample_ids, glue("{data_dir}/sample-identifiers.csv"), row.names = FALSE)

# Check
nrow(sample_ids)
dplyr::n_distinct(sample_ids$gvkey)
range(sample_ids$rdq)


# Data provenance --------------------------------------------------------------

# Print creation dates and sizes of all data files. This documents exactly
# which version of the data was used. WRDS databases are updated and
# backfilled over time, so the download date matters for reproducibility.

raw_files <- c("ccm-link.parquet", "crsp-stocknames.parquet",
               "fundq-raw.parquet", "crsp-dsf-v2.parquet", "crsp-index.parquet")

derived_files <- c("regdata.parquet", "figure-data.parquet",
                   "trading-dates.parquet", "sample-identifiers.parquet")

# Helper to print file info
print_file_info <- function(files, dir) {
  for (f in files) {
    path <- file.path(dir, f)
    if (file.exists(path)) {
      info <- file.info(path)
      cat(sprintf("  %-35s  %s  %.1f MB\n",
                      f, format(info$mtime, "%Y-%m-%d %H:%M"), info$size / 1e6))
    }
  }
}

cat("\nRaw data files (downloaded from WRDS):\n")
print_file_info(raw_files, data_dir)

cat("\nDerived data files:\n")
print_file_info(derived_files, data_dir)

cat("\nOutput files (tables and figures):\n")
out_files <- list.files(output_dir, pattern = "\\.tex$|\\.pdf$|\\.png$|\\.docx$")
print_file_info(out_files, output_dir)

cat(sprintf("\nPipeline finished: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))


# Stop logging -----------------------------------------------------------------

sink()
close(log_con)
cat("Log saved to:", log_file, "\n")
