# 5-data-provenance.R — sample identifiers + raw/derived/output provenance
# ============================================================================
# This is the JAR Data and Code Sharing Policy artifact step. It produces
# two things:
#
#   1. sample-identifiers.{parquet,csv} in DATA_DIR — the gvkey / permno /
#      rdq triples for every observation in the regression sample.
#
#   2. A printed table of every raw, derived, and output file with its
#      mtime, size, and SHA256 hash. Run via batch_run() so the table
#      lands inside this script's .Rout file alongside the R version
#      banner and the proc.time block — the .Rout itself is the
#      provenance log.
#
# Style note: this script leans on R's auto-print at the top level —
# bare expressions (e.g. `nrow(sample_ids)`) print themselves, so we use
# comments as labels and skip the cat()/sprintf() boilerplate.
# ============================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, glue, arrow, dplyr, digest)

options(scipen = 999)

load_dot_env(".env")
raw_data_dir <- Sys.getenv("RAW_DATA_DIR")
data_dir     <- Sys.getenv("DATA_DIR")
output_dir   <- Sys.getenv("OUTPUT_DIR")


# Run metadata -----------------------------------------------------------------

# Wall-clock start (R CMD BATCH appends proc.time() at the end for elapsed).
Sys.time()

# Environment configuration
raw_data_dir
data_dir
output_dir


# Export sample identifiers ----------------------------------------------------

# JAR: "whenever feasible, authors should provide the identifiers (e.g.,
# CIK, CUSIP) of all the observations that make up the final sample."

regdata <- read_parquet(glue("{data_dir}/regdata.parquet"))

sample_ids <- regdata |>
  select(gvkey, permno, rdq, datadate, fyearq, fqtr) |>
  arrange(gvkey, rdq)

write_parquet(sample_ids, glue("{data_dir}/sample-identifiers.parquet"))
write.csv(sample_ids,
          glue("{data_dir}/sample-identifiers.csv"),
          row.names = FALSE)

# Sample summary
nrow(sample_ids)              # rows
n_distinct(sample_ids$gvkey)  # distinct gvkeys
range(sample_ids$rdq)         # rdq range


# File provenance: mtime, size, SHA256 -----------------------------------------

# SHA256 hashes prove a JAR replicator's downloaded data matches the
# original analyst's. mtime alone gets clobbered by zip / copy / download;
# the hash is content-addressed and survives.

print_file_info <- function(files, dir) {
  if (is.null(dir) || !nzchar(dir)) {
    message("(env var not set; skipping)")
    return(invisible())
  }
  for (f in files) {
    path <- file.path(dir, f)
    if (!file.exists(path)) {
      message(sprintf("  %-35s  (missing)", f))
      next
    }
    info <- file.info(path)
    sha  <- digest::digest(file = path, algo = "sha256")
    message(sprintf("  %-35s  %s  %8.1f MB  sha256=%s",
                    f,
                    format(info$mtime, "%Y-%m-%d %H:%M"),
                    info$size / 1e6,
                    sha))
  }
}

raw_files <- c("ccm-link.parquet", "crsp-stocknames.parquet",
               "fundq-raw.parquet", "crsp-dsf-v2.parquet",
               "crsp-index.parquet")

derived_files <- c("regdata.parquet", "figure-data.parquet",
                   "trading-dates.parquet", "sample-selection.parquet",
                   "sample-identifiers.parquet", "sample-identifiers.csv")

# Raw data files (RAW_DATA_DIR)
print_file_info(raw_files, raw_data_dir)

# Derived data files (DATA_DIR)
print_file_info(derived_files, data_dir)

# Output files (OUTPUT_DIR)
out_files <- list.files(output_dir,
                        pattern = "\\.tex$|\\.pdf$|\\.png$|\\.docx$")
print_file_info(out_files, output_dir)
