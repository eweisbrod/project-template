# 5-data-provenance.R — sample identifiers + raw/derived/output inventory
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
#      so the inventory lands inside this script's .Rout file with the R
#      version banner and proc.time block.
#
# Style note: this script leans on R's auto-print at the top level —
# bare expressions print themselves, so we use comments as labels and
# skip the cat()/sprintf() boilerplate.
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
# CIK, CUSIP) of all the observations that make up the final sample." A
# replicator with their own WRDS access can use these to verify the
# sample without rerunning scripts 1-2.

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


# File inventory ---------------------------------------------------------------

list_dir <- function(dir) {
  if (is.null(dir) || !nzchar(dir) || !dir.exists(dir)) {
    message("(directory not set or missing; skipping)")
    return(invisible())
  }
  files <- list.files(dir, no.. = TRUE)
  for (f in sort(files)) {
    path <- file.path(dir, f)
    if (dir.exists(path)) next  # skip subdirectories
    info <- file.info(path)
    sha  <- digest::digest(file = path, algo = "sha256")
    message(sprintf("  %-35s  %s  %8.1f MB  sha256=%s",
                    f,
                    format(info$mtime, "%Y-%m-%d %H:%M"),
                    info$size / 1e6,
                    sha))
  }
}

# Raw data (RAW_DATA_DIR)
list_dir(raw_data_dir)

# Derived data (DATA_DIR)
list_dir(data_dir)

# Output (OUTPUT_DIR)
list_dir(output_dir)
