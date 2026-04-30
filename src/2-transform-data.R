# 2-transform-data.R
# ===========================================================================
# Merge downloaded tables, create variables, and link with CRSP returns.
#
# This script takes the raw parquet files from script 1 and:
#   1. Cleans fundq and merges it with CCM link + stocknames via DuckDB/dbplyr
#   2. Creates seasonal lags via explicit fiscal-period joins
#   3. Creates split-adjusted SUE, the SameSign indicator, and control variables
#   4. Applies sample filters and winsorizes continuous variables
#   5. Builds trading day windows and pulls CRSP event returns via DuckDB
#   6. Computes BHARs (buy-and-hold abnormal returns)
#   7. Saves analysis-ready datasets for scripts 3 (figures) and 4 (tables)
# ===========================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, lubridate, glue, arrow, haven, duckdb, DBI, dbplyr,
               tictoc,
               tidyverse)

# Disable DuckDB progress bar (floods console/log with "DuckDB progress: 0%")
# I have disabled this in order to clean up the log file that some journals 
# require for publication. You can comment this line out (or set to TRUE)
# if you would like to see the progress messages
options(duckdb.progress_display = FALSE)

# load our .env file and read the environment variables
load_dot_env(".env")
# RAW_DATA_DIR holds the raw WRDS pulls (read-only inputs).
# DATA_DIR holds derived parquets we produce here.
raw_data_dir <- Sys.getenv("RAW_DATA_DIR")
data_dir     <- Sys.getenv("DATA_DIR")
output_dir   <- Sys.getenv("OUTPUT_DIR")

# load my script that defines my common helper functions 
source("src/utils.R")


# Define a Sample selection tracker: 
# records the cumulative obs count after each filter step. 
# Saved at the end of the script and formatted into a LaTeX/Word
# table in script 4.

# Define the table with step number, description, and N
sample_selection <- tibble::tibble(
  step = integer(), description = character(), obs = integer()
)

#' Append one row to the running sample-selection table.
#'
#' @param tbl  The current sample-selection tibble to grow.
#' @param step Integer step number.
#' @param desc Human-readable description shown in the published table.
#' @param n    Row count after this step (use `count_rows()` defined below).
#' @return The input `tbl` with one row added.
add_step <- function(tbl, step, desc, n) {
  dplyr::bind_rows(tbl,
                   tibble::tibble(step = step, description = desc,
                                  obs = as.integer(n)))
}

# =============================================================================
# PART 1: Merge fundq + CCM + stocknames (dbplyr over DuckDB)
# =============================================================================

# We have our downloaded tables sitting on disk as parquet files. Now we need
# to merge them: fundq + CCM link (to get permno) + stocknames (to get SIC).
#
# We use dbplyr with DuckDB. This lets us write familiar dplyr code (filter,
# inner_join, mutate, etc.) but DuckDB executes it — reading directly from
# parquet files on disk. The data only enters R memory when we call collect().
#
# Why is this useful?
#   - DuckDB can handle the join without loading everything into R memory
#   - You write the same dplyr code you already know — dbplyr translates it
#     to SQL behind the scenes. You can see the generated SQL with show_query()
#   - It's a great middle ground between raw SQL and loading everything into R

# Open a DuckDB connection. This is an in-memory DuckDB instance — it doesn't
# create a database file on disk. We'll point it at our parquet files.
con <- dbConnect(duckdb())

# tbl() with a read_parquet() path creates a lazy reference to the parquet file.
# No data is loaded yet — DuckDB just knows where the file is.
fundq_tbl      <- tbl(con, glue("read_parquet('{raw_data_dir}/fundq-raw.parquet')"))
ccm_tbl        <- tbl(con, glue("read_parquet('{raw_data_dir}/ccm-link.parquet')"))
stocknames_tbl <- tbl(con, glue("read_parquet('{raw_data_dir}/crsp-stocknames.parquet')"))


#' Count rows of either a lazy dbplyr table or an in-memory data frame.
#'
#' Lets us track sample-selection counts without forcing a full
#' `collect()` of a dbplyr query (which would pull all rows into R).
#' For lazy tables we run a `SELECT COUNT(*)` on the database; for
#' in-memory data frames we just call `nrow()`.
#'
#' @param x A dbplyr lazy table or a data frame / tibble.
#' @return Integer scalar — the number of rows.
count_rows <- function(x) {
  if (inherits(x, "tbl_lazy")) {
    x |> summarize(n = n()) |> pull(n) |> as.integer()
  } else {
    nrow(x)
  }
}

# Define the sample period. The downloaded fundq already filters fyearq >= 1970
# (see script 1). We cap at 2024 because partial years (2025 has ~3 quarters,
# 2026 has ~120 obs) produce noisy year-level estimates. 
# Adjust this upper bound as more data becomes available.
fundq_tbl <- fundq_tbl |>
  filter(fyearq <= 2024)

sample_selection <- add_step(
  sample_selection, 1L,
  "Compustat fundq observations (1970 <= fyearq <= 2024, rdq not null)",
  count_rows(fundq_tbl)
)


#Fundq is our anchor table. Before merging let's make sure it is unique
# on gvkey fqtr fyearq and perhaps gvkey datadate as well

fundq_tbl |> 
  filter(!is.na(gvkey), !is.na(fyearq), !is.na(fqtr)) |>
  group_by(gvkey, fyearq, fqtr) |>
  count() |> 
  arrange(-n) # if the first rows have more than N=1 there are duplicates

#let's look at one of them more closely
fundq_tbl |> 
  filter(gvkey == "022449" & fyearq == 2023 & fqtr == 2) |> 
  collect() |> 
  print(n = Inf)
#two different datadates for the same fqtr indicates a potential fiscal year
# change

# I choose to delete these cases since the fiscal year change confounds 
# the current quarter as well as the lag structure 
# but in practice you could also keep the most recent datadate 
# (e.g., with arrange(datadate) 
# and distinct(gvkey, fyearq, fqtr, .keep_all = TRUE))
#  or do some other logic to pick one.

# filter out all gvkey-fyearq-fqtr combinations that have more than one row,
# since these are confounded by fiscal year changes and we can't be sure
# which datadate is correct for the event study
fundq_tbl <- fundq_tbl |>
  group_by(gvkey, fyearq, fqtr) |>
  filter(n() == 1) |>
  ungroup()

sample_selection <- add_step(
  sample_selection, 2L,
  "Less: obs in fiscal-year-change (gvkey, fyearq, fqtr) duplicates",
  count_rows(fundq_tbl)
)

#recheck
fundq_tbl |>
  filter(!is.na(gvkey), !is.na(fyearq), !is.na(fqtr)) |>
  group_by(gvkey, fyearq, fqtr) |>
  count() |>
  arrange(-n)
# clean now

#now let's look at duplicate gvkey datadate pairs
fundq_tbl |> 
  filter(!is.na(gvkey), !is.na(datadate)) |>
  group_by(gvkey, datadate) |>
  count() |> 
  arrange(-n) # if the first rows have more than N=1 there are duplicates

#let's look at one of them more closely
fundq_tbl |> 
  filter(gvkey == "002088" & datadate == "1985-12-31") |> 
  collect() |> 
  print(n = Inf)

#apply similar logic
# filter out all gvkey-datadate combinations that have more than one row,
# since these are confounded by fiscal year changes and we can't be sure
# which fqtr is correct for the event study
fundq_tbl <- fundq_tbl |>
  group_by(gvkey, datadate) |>
  filter(n() == 1) |>
  ungroup()

sample_selection <- add_step(
  sample_selection, 3L,
  "Less: obs in (gvkey, datadate) duplicates",
  count_rows(fundq_tbl)
)


# Step 1: Filter the CCM link table.
# Standard link filters (see Gow & Ding, "Empirical Research in Accounting")
# https://iangow.github.io/far_book/identifiers.html
# We filter here (not on download) so the raw parquet stays reusable.
#   linktype: LC (confirmed), LU (unconfirmed), LS (secondary permno only)
#   linkprim: P (primary) or C (primary when no P exists)
#   linkenddt: NULL means the link is still active — coalesce to far future
ccm_filtered <- ccm_tbl |>
  filter(linktype %in% c("LC", "LU", "LS"),
         linkprim %in% c("C", "P")) |>
  mutate(linkenddt = coalesce(linkenddt, as.Date("2099-12-31"))) |>
  select(gvkey, permno = lpermno, linkdt, linkenddt)

# Step 2: Join fundq with CCM to get CRSP permno for each firm-quarter.
# We match on gvkey and require rdq (the earnings announcement date) to fall
# within the link's valid date range [linkdt, linkenddt]. inner_join drops
# fundq rows with no valid permno — that's the filter we want to count.
fundq_with_permno <- fundq_tbl |>
  inner_join(ccm_filtered, by = "gvkey") |>
  filter(rdq >= linkdt, rdq <= linkenddt) |>
  select(-linkdt, -linkenddt)

sample_selection <- add_step(
  sample_selection, 4L,
  "Less: obs without valid CCM link to CRSP permno",
  count_rows(fundq_with_permno)
)

# Step 3: Join with CRSP stocknames_v2 to get SIC codes.
# SIC codes in CRSP are time-varying — each permno has date ranges [namedt,
# nameenddt] during which a particular SIC code applies. We use left_join
# here so that obs without a stocknames match survive for now; they get
# dropped together with the industry exclusions below.
fundq_with_sic <- fundq_with_permno |>
  left_join(stocknames_tbl, by = "permno") |>
  filter(is.na(namedt) | (rdq >= namedt & rdq <= nameenddt)) |>
  select(-namedt, -nameenddt)

# Up to this point, nothing has been computed — all steps are lazy.
# You can see the underlying SQL query with:
# fundq_with_sic |> show_query()

# collect() triggers DuckDB to execute the full query and return an R data.frame.
data1 <- fundq_with_sic |> collect()

dbDisconnect(con, shutdown = TRUE)

#Check for duplicates
data1 |> 
  group_by(permno, rdq) |>
  count() |> 
  arrange(-n) # if the first rows have more than N=1 there are duplicates

# Deduplicate: some firms report multiple quarters on the same announcement date
# (e.g., catching up on delayed filings). For the event study we need one
# observation per permno × rdq. Keep the most recent fiscal quarter — that's
# the one the market is most likely reacting to.
# Sometimes in practice I would delete both observations in these cases since 
# the announcement is confounded but sorting and choosing a best case is a more
# common example for teaching than excluding all confounded observations
dupes <- data1 |> count(permno, rdq) |> filter(n > 1)
cat(sprintf("  Duplicate permno × rdq: %d (will keep most recent quarter)\n", nrow(dupes)))

data1 <- data1 |>
  #sort by descending datadate so that most recent datadate will be kept
  arrange(permno, rdq, desc(datadate)) |>
  #distinct with keep_all TRUE will keep the first row for each distinct group
  distinct(permno, rdq, .keep_all = TRUE)

sample_selection <- add_step(
  sample_selection, 5L,
  "Less: obs with duplicate permno x rdq (kept most recent quarter)",
  count_rows(data1)
)

# Check the result
data1 |>
  group_by(permno, rdq) |>
  count() |>
  arrange(-n) # should all be 1 now


# Apply industry exclusions — done after the (permno, rdq) dedup so the table
# row counts reflect unique firm-quarter events, not intermediate link rows.
# Excluding financials (SIC 60-69) and utilities (SIC 49) is standard in
# accounting/finance research — these industries have unique accounting rules.
# Rows with missing SIC (no stocknames match) get dropped here too.
data1 <- data1 |>
  mutate(sic2 = floor(siccd / 100)) |>
  filter(!is.na(siccd),
         !(sic2 >= 60 & sic2 <= 69),
         sic2 != 49)

sample_selection <- add_step(
  sample_selection, 6L,
  "Less: obs without CRSP SIC or in financials (SIC 60-69) / utilities (SIC 49)",
  count_rows(data1)
)


# =============================================================================
# PART 2: Create seasonal lags
# =============================================================================

# For the earnings surprise (SUE), we need same-quarter earnings (ibq) from
# one year ago. This is a "seasonal lag" — lag 4 quarters, not lag 1 quarter.
# We also need the lag-1-quarter market value (mve_lag1 = cshoq_lag1 *
# prccq_lag1) to use as the SUE scaler.
#
# WHY NOT JUST USE dplyr::lag()?
# lag(x, 4) relies on the rows being sorted correctly AND on every quarter
# being present. If a firm has a gap in reporting (common — e.g., a suspended
# filer), lag(x, 4) silently pulls the wrong row and you get a bad SUE.
#
# Instead we match explicitly on fiscal-period keys (gvkey + fyearq + fqtr):
#   - Lag 1 quarter: match to (lag_fyear, lag_fqtr), where Q1 wraps back to
#     Q4 of the prior fiscal year.
#   - Lag 4 quarters: match to (fyearq - 1, fqtr) — same quarter, year earlier.
# If the target row doesn't exist, the join gives NA and we correctly drop
# the observation later.

data2 <- data1 |>
  mutate(
    # Prior-quarter keys: Q1 maps to Q4 of the prior fiscal year; else fqtr - 1
    lag_fqtr    = if_else(fqtr == 1L, 4L, fqtr - 1L),
    lag_fyear   = if_else(fqtr == 1L, fyearq - 1L, fyearq),
    # Same-quarter-last-year key: just decrement fiscal year, fqtr matches directly
    lag4_fyearq = fyearq - 1L
  )

# Build minimal lookup tables from data1 (one row per gvkey × fyearq × fqtr).
# We select only the Compustat columns needed for each lag and collapse with
# distinct(). Because fundq was deduplicated on (gvkey, fyearq, fqtr) before
# the merge (Part 1), Compustat values are identical across any CCM-induced
# duplicates (same gvkey, different permnos). So distinct() on all selected
# columns is lossless — it never has to choose between different values.
lag1_lookup <- data1 |>
  select(gvkey, fyearq, fqtr,
         prccq_lag1 = prccq,
         cshoq_lag1 = cshoq) |>
  distinct()

lag4_lookup <- data1 |>
  select(gvkey, fyearq, fqtr,
         ibq_lag4     = ibq,
         saleq_lag4   = saleq,
         datadate_lag4 = datadate) |>
  distinct()

# Join the lag values on by the explicit period keys.
# join_by(a, b == c) means: a matches a, and left.b matches right.c.
data2 <- data2 |>
  left_join(lag1_lookup,
            by = join_by(gvkey, lag_fyear == fyearq, lag_fqtr == fqtr)) |>
  left_join(lag4_lookup,
            by = join_by(gvkey, lag4_fyearq == fyearq, fqtr))

# Sanity check: the lagged datadate should be ~12 months earlier. Firms that
# changed their fiscal year end can produce 9- or 15-month gaps even when the
# fiscal quarter matches — we drop those cases below.
data2 |>
  filter(!is.na(datadate_lag4)) |>
  mutate(gap_months = interval(datadate_lag4, datadate) %/% months(1)) |>
  count(gap_months) |>
  print(n = 20)

# Keep only clean 12-month gaps. Rows without a lag4 match also get dropped
# here (they can't form a seasonal SUE), which keeps the sample definition
# consistent with requiring a valid prior-year comparison.
data2 <- data2 |>
  filter(!is.na(datadate_lag4),
         interval(datadate_lag4, datadate) %/% months(1) == 12)

sample_selection <- add_step(
  sample_selection, 7L,
  "Less: obs without valid seasonal lag (missing lag4 row or 12-month gap)",
  count_rows(data2)
)


# =============================================================================
# PART 3: SUE and variables
# =============================================================================

# We define SUE as the seasonal change in quarterly net income (ibq) scaled
# by prior-quarter market value of equity (mve_lag1):
#
#   SUE = (ibq - ibq_lag4) / mve_lag1
#
# An alternative definition scales the change in per-share earnings (epspiq)
# by the prior share price:
#
#   SUE_alt = (adj_epspiq - adj_epspiq_lag4) / prccq_lag1
#
# where the "adj_" values split-adjust epspiq back to a common share basis
# using Compustat's cumulative adjustment factor ajexq (a 2:1 split doubles
# ajexq, so adj_epspiq = epspiq * ajexq / ajexq_lag1 puts the current EPS on
# the prior quarter's share count).
#
# Why we use dollar earnings / dollar mve instead of the per-share version:
#   - No split-adjustment gymnastics: dollars are dollars and market value
#     already reflects share count changes.
#   - Conceptually cleaner — both numerator and denominator are measured in
#     dollars on the same basis.
#   - The per-share formulation is more classical (Latane/Jones, Bernard &
#     Thomas), so if you're replicating an older paper you may want to use
#     it. Both are valid SUE measures used in the literature.

data3 <- data2 |>
  mutate(
    # Prior-quarter market value of equity (the SUE scaler)
    mve_lag1 = cshoq_lag1 * prccq_lag1,

    # SUE = seasonal change in quarterly income, scaled by prior-quarter MVE
    sue = (ibq - ibq_lag4) / mve_lag1,

    # Seasonal sales change
    delta_saleq = saleq - saleq_lag4,

    # SameSign indicator: 1 if earnings change and sales change move together.
    # This is our interaction variable — the idea is that the market trusts
    # earnings increases more when sales increase too (and vice versa). An
    # earnings increase without sales growth might be cost-cutting or accrual
    # manipulation, which is less persistent.
    same_sign = if_else(
      sign(ibq - ibq_lag4) == sign(delta_saleq), 1L, 0L
    ),

    # Loss indicator
    loss = if_else(ibq < 0, 1L, 0L),

    # Fama-French 12 industry classification (from utils.R)
    FF12 = assign_FF12(siccd),
    ff12num = assign_FF12_num(siccd),

    # Current market value of equity (for descriptive stats and log_mve control)
    mve = cshoq * prccq,

    # Log market value (common control variable)
    log_mve = log(mve)
  )

# Check SUE distribution — should be centered near zero
summary(data3$sue)

# How often do earnings and sales move in the same direction?
table(data3$same_sign, useNA = "ifany")
mean(data3$same_sign, na.rm = TRUE)


# =============================================================================
# PART 4: Sample filters and winsorization
# =============================================================================

# Drop penny stocks: firms with share price below $1 at the prior quarter-end.
# This is a standard filter in empirical finance to remove illiquid micro-caps
# whose returns are noisy and can distort the analysis.
data4 <- data3 |>
  filter(!is.na(prccq_lag1),
         prccq_lag1 > 1)

sample_selection <- add_step(
  sample_selection, 8L,
  "Less: penny stocks (prccq_lag1 <= $1)",
  count_rows(data4)
)

# Require non-missing values for remaining key variables.
# mve > 0 ensures log_mve is finite; mve_lag1 > 0 ensures the SUE scaler is
# positive (missing cshoq_lag1 would make it NA).
data4 <- data4 |>
  filter(!is.na(sue),
         is.finite(sue),   # drops Inf/-Inf from near-zero mve_lag1
         !is.na(same_sign),
         mve > 0,
         mve_lag1 > 0)

nrow(data4)

# Winsorize SUE and continuous controls to reduce the influence of outliers.
# Default is 1%/99% — see winsorize_x() in utils.R.
data4 <- data4 |>
  mutate(across(c(sue, log_mve), winsorize_x))

# Check winsorized SUE, type=2 is similar to SAS quantiles
# without type =2 the probs will not 100% match the winsorization cutoffs 
# because of different quantile definitions, but in most cases this should not
# make much difference
quantile(data4$sue, probs = c(0, .01, .25, .50, .75, .99, 1), type=2)


# =============================================================================
# PART 5: Build trading calendar and event windows
# =============================================================================

# Build a numbered list of trading days from the CRSP daily dates.
# We use DuckDB here because the CRSP parquet has ~100M rows — we just need
# the distinct dates, and DuckDB can extract them without loading the full file.

con <- dbConnect(duckdb())

trading_dates <- dbGetQuery(con, glue("
  SELECT DISTINCT dlycaldt AS date
  FROM read_parquet('{raw_data_dir}/crsp-dsf-v2.parquet')
  ORDER BY date
")) |>
  mutate(td = row_number())

dbDisconnect(con, shutdown = TRUE)

nrow(trading_dates)

# Expand each event to a [-5, +5] trading day window.
# The wide window is for figures (CAR plot); the regression uses [-1, +1].
# See utils.R for the trading_day_window() function.

firm_events <- data4 |>
  select(permno, rdq) |>
  distinct() |>
  mutate(rdq = as.Date(rdq))

event_dates <- trading_day_window(firm_events, trading_dates, rdq, -5, +5)

nrow(event_dates)


# =============================================================================
# PART 6: Pull event-window returns from CRSP (DuckDB)
# =============================================================================

# This is where DuckDB shines. We join our ~8M event-day rows against the full
# CRSP daily parquet (~100M rows). DuckDB reads the parquet directly and only
# pulls the rows we need — no need to load all of CRSP into R memory.
#
# duckdb_register() makes an R data.frame available as a virtual table in
# DuckDB, so we can join it against parquet files in a single SQL query.

tictoc::tic()

con <- dbConnect(duckdb())
duckdb_register(con, "events", event_dates |> select(permno, date, offset))

crsp_rets <- dbGetQuery(con, glue("
  SELECT ev.permno, ev.date, ev.offset,
         dsf.dlyret AS ret,
         idx.dlytotret AS vwretd
  FROM events AS ev
  INNER JOIN read_parquet('{raw_data_dir}/crsp-dsf-v2.parquet') AS dsf
    ON ev.permno = dsf.permno AND ev.date = dsf.dlycaldt
  INNER JOIN read_parquet('{raw_data_dir}/crsp-index.parquet') AS idx
    ON ev.date = idx.dlycaldt
"))

dbDisconnect(con, shutdown = TRUE)

tictoc::toc()

nrow(crsp_rets)
head(crsp_rets)


# =============================================================================
# PART 7: Compute BHARs
# =============================================================================

# BHAR = Buy-and-Hold Abnormal Return
# = cumulative stock return minus cumulative market return over the window
#
# For the regression, we compute BHAR over [-1, +1] (3 trading days).
# For the event study figure, we compute cumulative returns at each offset.

# --- Regression BHAR: [-1, +1] ---

bhar_reg <- crsp_rets |>
  filter(offset >= -1, offset <= 1) |>
  group_by(permno, date) |>
  # We use the first date in the window to identify the event
  # (date here is each day's date, not rdq — we need to get back to rdq)
  arrange(permno, date) |>
  ungroup()

# Actually, we need rdq to merge back. Let's join event_dates with crsp_rets
# to keep the rdq link, then compute BHAR grouped by permno + rdq.

bhar_reg <- event_dates |>
  select(permno, rdq, date, offset) |>
  inner_join(crsp_rets, by = c("permno", "date", "offset")) |>
  filter(offset >= -1, offset <= 1) |>
  group_by(permno, rdq) |>
  summarize(
    # BHAR = product of (1 + stock return) minus product of (1 + market return)
    bhar = prod(1 + ret, na.rm = TRUE) - prod(1 + vwretd, na.rm = TRUE),
    n_days = n(),
    .groups = "drop"
  )

nrow(bhar_reg)
summary(bhar_reg$bhar)

# --- Figure BHAR: cumulative at each offset [-5, +5] ---

bhar_fig <- event_dates |>
  select(permno, rdq, date, offset) |>
  inner_join(crsp_rets, by = c("permno", "date", "offset")) |>
  group_by(permno, rdq) |>
  arrange(permno, rdq, offset) |>
  mutate(
    cum_ret   = cumprod(1 + ret) - 1,
    cum_mkt   = cumprod(1 + vwretd) - 1,
    bhar_cum  = cum_ret - cum_mkt
  ) |>
  ungroup()

head(bhar_fig)


# =============================================================================
# PART 8: Create final analysis datasets
# =============================================================================

# Merge the [-1, +1] BHAR back to the firm-quarter data for regressions.
regdata <- data4 |>
  mutate(rdq = as.Date(rdq)) |>
  inner_join(bhar_reg, by = c("permno", "rdq")) |>
  # Require complete BHAR window (3 trading days)
  filter(n_days == 3)

sample_selection <- add_step(
  sample_selection, 9L,
  "Less: obs with missing data (SUE, size, BHAR window, etc.)",
  count_rows(regdata)
)

nrow(regdata)
head(regdata)

# Quick look at the key relationship: SUE vs BHAR
regdata |>
  mutate(sue_decile = ntile(sue, 10)) |>
  group_by(sue_decile) |>
  summarize(mean_bhar = mean(bhar, na.rm = TRUE),
            n = n())


# Save output ------------------------------------------------------------------

# Regression dataset (one row per firm-quarter)
write_parquet(regdata, glue("{data_dir}/regdata.parquet"))

# Figure dataset (one row per firm-quarter-offset, for CAR plots)
write_parquet(bhar_fig, glue("{data_dir}/figure-data.parquet"))

# Trading dates (useful for other scripts)
write_parquet(trading_dates, glue("{data_dir}/trading-dates.parquet"))

# Sample selection table (step-by-step obs counts, formatted in script 4)
print(sample_selection)
write_parquet(sample_selection, glue("{data_dir}/sample-selection.parquet"))
