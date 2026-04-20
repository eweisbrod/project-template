# 1-download-data.R
# ===========================================================================
# Download data for the earnings event study to local parquet files.
#
# This script downloads raw tables and merges them locally. It showcases three 
# different methods for downloading data from WRDS, each with different 
# RAM usage and speed characteristics:
#   1. Download CCM link + CRSP stocknames (small — simple collect)
#   2. Download Compustat fundq (medium — chunked ParquetFileWriter)
#   3. Download CRSP daily returns (large — chunked ParquetFileWriter)
#   4. Download market index returns (using download_wrds() wrapper)
#
# Each download demonstrates a different method, from simplest to most
# RAM-efficient.
#
#
# WHY THREE METHODS?
# The simplest approach (collect) loads the entire table into R memory.
# This works for small tables, but for large ones like CRSP daily (~100M rows)
# it can exceed your RAM and crash R. The chunked ParquetFileWriter method
# streams data in batches, so peak memory stays bounded regardless of table size.
#
# Benchmarks on comp.funda (935K rows x 1000 columns, 362 MB parquet):
#   collect():              528s,  18.4 GB RAM
#   Chunked ParquetWriter:  447s,   7.8 GB RAM
# The download_wrds() function in utils.R auto-sizes batches based on a
# target RAM limit (default 8 GB) so you don't have to think about it.
# ===========================================================================


# Setup ------------------------------------------------------------------------

# Check if the pacman package is installed, and install it if not.
if (!require("pacman")) install.packages("pacman")

# Load all the necessary packages using pacman::p_load. 
# This will install any missing packages automatically.
pacman::p_load(dotenv, keyring, dbplyr, RPostgres, DBI, glue, arrow,
               tictoc,
               tidyverse)

# Read from the .env file to get the data and output directories. 
#This allows us to keep our directory paths configurable for each user/machine
# and separate from the code.
load_dot_env(".env")

# reading the .env file created some environment variables 
# that we can access with Sys.getenv() to get the data and output directories.
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

# this loads a separate script called utils.R that contains helper functions. 
# This makes it easy to reuse utility functions and keeps our main script clean.
source("src/utils.R")


# Connect to WRDS --------------------------------------------------------------

# We use the keyring package to securely store your WRDS credentials in your
# operating system's credential store (Windows Credential Manager, macOS
# Keychain, etc.). This is more secure than putting passwords in .env files.
# Note: Do NOT put passwords in .env - use keyring for secrets.
#
# FIRST TIME SETUP 
# run these two lines in your R console if you did not do it in setup.R:
#   keyring::key_set("wrds_user")   # Will prompt for your WRDS username
#   keyring::key_set("wrds_pw")     # Will prompt for your WRDS password
# This stores credentials securely in your system's keychain.
# You only need to do this once per computer.
#
# To update stored credentials (e.g., after a password change), just re-run
# the key_set lines above.

wrds <- dbConnect(Postgres(),
                  host     = 'wrds-pgdata.wharton.upenn.edu',
                  port     = 9737,
                  user     = keyring::key_get("wrds_user"),
                  password = keyring::key_get("wrds_pw"),
                  sslmode  = 'require',
                  dbname   = 'wrds')

# If connection is successful, this should print connection info
wrds


# Download CCM link table ------------------------------------------------------

# The CCM link table maps Compustat gvkey to CRSP permno over time.
# It's small (~87K rows after filtering) so we just collect() it directly.
# collect() is the simplest download method — fine when the table fits in RAM.

# There are many ways to check how long a code takes.
# The tictoc package is a simple one. 
# tic() starts the timer, toc() stops it and prints the elapsed time.
tictoc::tic() #start timer
  
  # Download the full CCM link table — we'll filter it later when merging.
  # Keeping the raw file means you can reuse it with different link criteria.
  ccm_link <- tbl(wrds, in_schema("crsp", "ccmxpf_lnkhist")) |>
    collect()

tictoc::toc() #stop timer

# Check the result
nrow(ccm_link) # number of observations (rows)
head(ccm_link) # preview the first few rows to check the data looks correct

# Save the downloaded file to disk as a parquet file
write_parquet(ccm_link, glue("{data_dir}/ccm-link.parquet"))


# Download CRSP stocknames_v2 --------------------------------------------------

# This table will be used to collect SIC codes. 

# SIC codes come from CRSP rather than Compustat because:
#   - stocknames_v2 has time-varying SIC codes (matched by date range)
#   - It is current through 2025 (old stocknames table froze at 2024-12-31)
#   - We already have permno from the CCM link, so it's a natural join
# This is a small table, so collect() is fine.

tictoc::tic()

  stocknames <- tbl(wrds, in_schema("crsp", "stocknames_v2")) |>
    # example of first selecting only certain columns that we need to download
    select(permno, namedt, nameenddt, siccd) |>
    collect()

tictoc::toc()

nrow(stocknames)

write_parquet(stocknames, glue("{data_dir}/crsp-stocknames.parquet"))


# Download Compustat fundq (INTRODUCE A CHUNKED DOWNLOAD APPROACH) -------------

# Compustat quarterly fundamentals is a medium-large table. Rather than loading
# the whole thing into R memory with collect(), we use a server-side cursor to
# stream rows in batches, writing each batch to a single parquet file using
# arrow::ParquetFileWriter.

# This is the same approach used by Ian Gow's db2pqr package.
# https://github.com/iangow/db2pqr

# FIRST I DEMONSTRATE THIS APPROACH, including some overhead and technical
# details that you might not be interested in. After this first time,
# we can use the utility function download_wrds() to do this same approach but
# you won't have to see so many lines of code every time you download a file.

# How it works:
#   - dbSendQuery() creates a server-side cursor (no data transferred yet)
#   - dbFetch(n = BATCH_SIZE) pulls BATCH_SIZE rows at a time
#   - ParquetFileWriter appends each batch as a row group in the parquet file
#   - Peak memory = one batch, not the full table
#


#Some setup options
# Batch size adjusts how many rows will be batched to download at a time
# later when we wrap this in a download function we can target the batch size 
# based on the RAM available on your computer
BATCH_SIZE <- 50000

# the path is the name of the output parquet file where we want to store the 
# downloaded data 
fundq_path <- glue("{data_dir}/fundq-raw.parquet")

# AS AN EXAMPLE: We can send some raw SQL to the WRDS postgres server to apply
# some standard filters before downloading to reduce the size of the raw file. 
# Depending on your resources and needs, you might instead want to download the 
# entire raw fundq table and then filter locally. Then you can
# reuse the raw file with different criteria without re-downloading. 
fundq_sql <- "
  SELECT gvkey, datadate, fyearq, fqtr, rdq,
         conm, cusip, cik,
         saleq, ibq, epspiq, atq, cshoq, prccq, ajexq
  FROM comp.fundq
  WHERE indfmt = 'INDL' AND datafmt = 'STD'
    AND popsrc = 'D' AND consol = 'C'
    AND fyearq >= 1970
    AND rdq IS NOT NULL
"

tictoc::tic()

# Open a server-side cursor on WRDS. No rows are transferred yet — the database
# is just holding the query plan ready to stream results when we ask.
res <- dbSendQuery(wrds, fundq_sql)

# Open the output parquet file for writing (a low-level Arrow byte stream).
sink <- arrow::FileOutputStream$create(fundq_path)

# create a placeholder that we can use to point to a parquet file writer
# We'll create the ParquetFileWriter later (we need its schema first).
writer <- NULL

# Setting up counters for the progress message.
total_rows <- 0
start_time <- Sys.time()

# Loop until the cursor reports no more rows on the server side.
while (!dbHasCompleted(res)) {
  # Pull the next BATCH_SIZE rows from the server into an R data.frame.
  chunk <- dbFetch(res, n = BATCH_SIZE)
  # add a failsafe: some drivers return 0 rows before dbHasCompleted flips
  # if so — bail out.
  if (nrow(chunk) == 0) break

  # Update counters and print a one-line progress indicator (\r overwrites in place).
  total_rows <- total_rows + nrow(chunk)
  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
  cat(sprintf("\r  %s rows | %.1f min elapsed",
              format(total_rows, big.mark = ","), elapsed))

  # Convert the R data.frame to an Arrow Table (columnar, zero-copy where possible).
  tab <- arrow::Table$create(chunk)

  # First iteration only: initialize the parquet writer using the chunk's schema.
  # We use zstd compression — good ratio, fast to read back.
  if (is.null(writer)) {
    writer <- arrow::ParquetFileWriter$create(
      schema = tab$schema,
      sink = sink,
      properties = arrow::ParquetWriterProperties$create(
        column_names = tab$schema$names,
        compression = "zstd"
      )
    )
  }

  # Append this chunk to the parquet file as a new row group.
  writer$WriteTable(tab, chunk_size = BATCH_SIZE)
}

# Finalize the parquet file (writes footer metadata — required for a valid file).
if (!is.null(writer)) writer$Close()
# Close the underlying byte stream.
sink$close()
# Release the server-side cursor.
dbClearResult(res)
# Newline after the \r progress line so later output doesn't overwrite it.
cat("\n")

tictoc::toc()

# Check the result
sprintf("Saved %s rows, %.1f MB",
        format(total_rows, big.mark = ","), file.size(fundq_path) / 1e6)


# Download CRSP daily returns (download_wrds function) -------------------------

# CRSP daily stock file (v2) is the largest table we need — about 100M rows.
# Now that you've seen the raw chunked loop above (for fundq), here we use the
# download_wrds() wrapper from utils.R. It does the same thing — chunked
# dbFetch + ParquetFileWriter — but handles the connection, batching, and
# progress output for you. It also auto-sizes batches based on max_ram_mb.
#
# RAM NOTE: The default max_ram_mb = 8000 (8 GB) controls the size of each
# batch. Actual peak RAM usage will be max_ram_mb plus ~1-2 GB baseline overhead
# (R session, loaded packages, writer buffers). If you're on a machine with
# limited RAM, set a lower value, e.g., max_ram_mb = 2000 for a 4 GB machine.
# CRSP daily only has 3 columns so RAM won't be an issue here, but for wide
# tables like comp.funda (~1000 columns) the throttle makes a big difference.
#
# CRSP v2 column names differ from the old WRDS CRSP format:
#   dsf_v2:            dlycaldt (date), dlyret (return)
#   inddlyseriesdata:  dlycaldt, dlytotret (market return), indno (index ID)
#   indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca VW market index

# This will take 10-15+ minutes. The progress line updates as it goes.
tictoc::tic()

download_wrds(
  sql = "SELECT permno, dlycaldt, dlyret
         FROM crsp.dsf_v2
         WHERE dlycaldt >= '1970-01-01'",
  output_path = glue("{data_dir}/crsp-dsf-v2.parquet"),
  max_ram_mb = 8000, 
  wrds_user = keyring::key_get("wrds_user"),
  wrds_pw   = keyring::key_get("wrds_pw")
)

tictoc::toc()


# --- Market index returns (inddlyseriesdata) ---

# Same download_wrds() pattern for the market index.
# the market index will be used to compute abnormal returns relative to it.
# indno = 1000200 is the NYSE/NYSEMKT/Nasdaq/Arca VW market index.
tictoc::tic()

download_wrds(sql = "SELECT dlycaldt, dlytotret
                     FROM crsp.inddlyseriesdata
                     WHERE indno = 1000200
                       AND dlycaldt >= '1970-01-01'",
              output_path = glue("{data_dir}/crsp-index.parquet"),
              wrds_user = keyring::key_get("wrds_user"),
              wrds_pw   = keyring::key_get("wrds_pw"))

tictoc::toc()


# Disconnect from WRDS ---------------------------------------------------------

dbDisconnect(wrds)

# At this point you have the following parquet files on disk:
#   {data_dir}/ccm-link.parquet         — CCM link table (gvkey -> permno)
#   {data_dir}/crsp-stocknames.parquet  — CRSP SIC codes by permno + date range
#   {data_dir}/fundq-raw.parquet        — Compustat quarterly fundamentals
#   {data_dir}/crsp-dsf-v2.parquet      — CRSP daily stock returns
#   {data_dir}/crsp-index.parquet       — CRSP daily market index returns
#
# Next: run src/2-transform-data.R to merge these tables, create variables
# (SUE, controls), and link with CRSP returns for the event study.
