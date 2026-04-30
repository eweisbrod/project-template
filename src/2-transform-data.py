# 2-transform-data.py
# ===========================================================================
# Merge downloaded tables, create variables, and link with CRSP returns.
#
# This script takes the raw parquet files from script 1 and:
#   1. Cleans fundq and merges it with CCM link + stocknames via DuckDB
#   2. Creates seasonal lags via explicit fiscal-period joins
#   3. Creates SUE (delta ibq / mve), the SameSign indicator, and controls
#   4. Applies sample filters and winsorizes continuous variables
#   5. Builds trading day windows and pulls CRSP event returns via DuckDB
#   6. Computes BHARs (buy-and-hold abnormal returns)
#   7. Saves analysis-ready datasets for scripts 3 and 4
#
# HOW TO RUN:
#   uv run src/2-transform-data.py
# ===========================================================================


# Setup ------------------------------------------------------------------------

import os
import sys
import time

import duckdb
import polars as pl
from dotenv import load_dotenv

# Import helpers from utils.py
from utils import assign_ff12

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(".env", override=True)
# RAW_DATA_DIR holds the raw WRDS pulls (read-only inputs).
# DATA_DIR holds derived parquets we produce here.
raw_data_dir = os.getenv("RAW_DATA_DIR")
data_dir = os.getenv("DATA_DIR")
output_dir = os.getenv("OUTPUT_DIR")


# Sample selection tracker — records cumulative obs count after each filter step.
# Saved at the end and formatted into a table in script 4.
sample_selection = []

def add_step(step: int, desc: str, n: int):
    sample_selection.append({"step": step, "description": desc, "obs": n})
    print(f"  Step {step}: {n:,} — {desc}")


# =============================================================================
# PART 1: Merge fundq + CCM + stocknames (DuckDB SQL)
# =============================================================================

# We use DuckDB to query parquet files directly with SQL. DuckDB reads the
# files from disk without loading them fully into memory, executes the joins
# on its own engine, and returns the result as a polars DataFrame.
#
# duckdb.sql() runs a SQL query and .pl() converts the result to polars
# via Arrow zero-copy — no pandas intermediate, no type coercion.

con = duckdb.connect()

# read_parquet() in DuckDB SQL creates a lazy reference to the file.
# No data is loaded yet.
fundq_raw = f"read_parquet('{raw_data_dir}/fundq-raw.parquet')"
ccm_raw = f"read_parquet('{raw_data_dir}/ccm-link.parquet')"
stocknames_raw = f"read_parquet('{raw_data_dir}/crsp-stocknames.parquet')"


# --- Define sample period ---
# Cap at 2024 because partial years (2025 has ~3 quarters, 2026 has ~120 obs)
# produce noisy year-level estimates.

n = con.sql(f"""
    SELECT COUNT(*) AS n FROM {fundq_raw} WHERE fyearq <= 2024
""").fetchone()[0]
add_step(1, "Compustat fundq observations (1970 <= fyearq <= 2024, rdq not null)", n)


# --- Clean fundq: drop fiscal-year-change duplicates ---
# Some firms change fiscal year ends, creating two rows for the same
# (gvkey, fyearq, fqtr) or (gvkey, datadate). We drop ALL rows for those
# combinations rather than silently picking one.

con.sql(f"""
    CREATE TEMP TABLE fundq AS
    SELECT * FROM {fundq_raw}
    WHERE fyearq <= 2024
""")

# Drop (gvkey, fyearq, fqtr) duplicates.
# We use a window function (COUNT OVER) rather than DELETE ... IN because
# SQL tuple comparisons skip NULLs — rows where fqtr IS NULL would survive
# the DELETE even if they appear in multi-row groups. The window approach
# handles NULLs the same way R's group_by + filter(n() == 1) does.
con.sql("""
    DELETE FROM fundq
    WHERE rowid IN (
        SELECT rowid FROM (
            SELECT rowid, COUNT(*) OVER (PARTITION BY gvkey, fyearq, fqtr) AS grp_n
            FROM fundq
        ) WHERE grp_n > 1
    )
""")
n = con.sql("SELECT COUNT(*) FROM fundq").fetchone()[0]
add_step(2, "Less: obs in fiscal-year-change (gvkey, fyearq, fqtr) duplicates", n)

# Drop (gvkey, datadate) duplicates — same window approach as above.
con.sql("""
    DELETE FROM fundq
    WHERE rowid IN (
        SELECT rowid FROM (
            SELECT rowid, COUNT(*) OVER (PARTITION BY gvkey, datadate) AS grp_n
            FROM fundq
        ) WHERE grp_n > 1
    )
""")
n = con.sql("SELECT COUNT(*) FROM fundq").fetchone()[0]
add_step(3, "Less: obs in (gvkey, datadate) duplicates", n)


# --- Filter CCM link table ---
# Standard link filters (Gow & Ding, "Empirical Research in Accounting"):
#   linktype: LC (confirmed), LU (unconfirmed), LS (secondary permno)
#   linkprim: P (primary) or C (primary when no P exists)
#   linkenddt: NULL means still active — coalesce to far future

con.sql(f"""
    CREATE TEMP TABLE ccm AS
    SELECT gvkey, lpermno AS permno, linkdt,
           COALESCE(linkenddt, DATE '2099-12-31') AS linkenddt
    FROM {ccm_raw}
    WHERE linktype IN ('LC', 'LU', 'LS')
      AND linkprim IN ('C', 'P')
""")


# --- Join fundq + CCM ---
# Match on gvkey; require rdq to fall within the link's valid date range.

con.sql("""
    CREATE TEMP TABLE fundq_linked AS
    SELECT f.*, c.permno
    FROM fundq f
    INNER JOIN ccm c ON f.gvkey = c.gvkey
    WHERE f.rdq >= c.linkdt AND f.rdq <= c.linkenddt
""")
n = con.sql("SELECT COUNT(*) FROM fundq_linked").fetchone()[0]
add_step(4, "Less: obs without valid CCM link to CRSP permno", n)


# --- Join with CRSP stocknames (left join for SIC codes) ---
# SIC codes in CRSP are time-varying by date range. Left join so obs without
# a stocknames match survive — they get dropped with the industry filter below.

con.sql(f"""
    CREATE TEMP TABLE fundq_with_sic AS
    SELECT f.*, s.siccd
    FROM fundq_linked f
    LEFT JOIN {stocknames_raw} s
      ON f.permno = s.permno
      AND (s.namedt IS NULL OR (f.rdq >= s.namedt AND f.rdq <= s.nameenddt))
""")

# Collect to polars for the remaining in-memory operations.
data1 = con.sql("SELECT * FROM fundq_with_sic").pl()
con.close()

print(f"  Collected {data1.shape[0]:,} rows into polars")


# --- Deduplicate (permno, rdq) ---
# Some firms report multiple quarters on the same announcement date
# (catching up on delayed filings). Keep the most recent fiscal quarter.

dupes = (data1.group_by("permno", "rdq").len()
         .filter(pl.col("len") > 1).shape[0])
print(f"  Duplicate permno x rdq: {dupes} (will keep most recent quarter)")

data1 = (data1
    .sort("permno", "rdq", pl.col("datadate").cast(pl.Date), descending=[False, False, True])
    .unique(subset=["permno", "rdq"], keep="first")
)

add_step(5, "Less: obs with duplicate permno x rdq (kept most recent quarter)",
         data1.shape[0])


# --- Industry exclusions ---
# Excluding financials (SIC 60-69) and utilities (SIC 49) is standard.
# Rows with missing SIC (no stocknames match) are dropped too.

data1 = (data1
    .with_columns((pl.col("siccd") / 100).floor().cast(pl.Int32).alias("sic2"))
    .filter(
        pl.col("siccd").is_not_null(),
        ~((pl.col("sic2") >= 60) & (pl.col("sic2") <= 69)),
        pl.col("sic2") != 49,
    )
)
add_step(6, "Less: obs without CRSP SIC or in financials (SIC 60-69) / utilities (SIC 49)",
         data1.shape[0])


# =============================================================================
# PART 2: Create seasonal lags
# =============================================================================

# For SUE we need same-quarter earnings (ibq) from one year ago. We match
# explicitly on fiscal-period keys (gvkey + fyearq + fqtr) rather than using
# a positional lag, which silently misfires when a firm has reporting gaps.

data2 = data1.with_columns(
    # Prior-quarter keys: Q1 wraps to Q4 of prior fiscal year
    pl.when(pl.col("fqtr") == 1).then(4).otherwise(pl.col("fqtr") - 1)
      .cast(pl.Int32).alias("lag_fqtr"),
    pl.when(pl.col("fqtr") == 1).then(pl.col("fyearq") - 1).otherwise(pl.col("fyearq"))
      .cast(pl.Int32).alias("lag_fyear"),
    # Same-quarter-last-year: just decrement fiscal year
    (pl.col("fyearq") - 1).alias("lag4_fyearq"),
)

# Build lookup tables from data1.  Because fundq was deduplicated on
# (gvkey, fyearq, fqtr) before the merge, Compustat values are identical
# across any CCM-induced duplicates. unique() on all columns is lossless.
lag1_lookup = (data1
    .select(
        "gvkey", "fyearq", "fqtr",
        pl.col("prccq").alias("prccq_lag1"),
        pl.col("cshoq").alias("cshoq_lag1"),
    )
    .unique()
)

lag4_lookup = (data1
    .select(
        "gvkey", "fyearq", "fqtr",
        pl.col("ibq").alias("ibq_lag4"),
        pl.col("saleq").alias("saleq_lag4"),
        pl.col("datadate").alias("datadate_lag4"),
    )
    .unique()
)

# Join lag values on explicit period keys.
data2 = (data2
    .join(lag1_lookup,
          left_on=["gvkey", "lag_fyear", "lag_fqtr"],
          right_on=["gvkey", "fyearq", "fqtr"],
          how="left")
    .join(lag4_lookup,
          left_on=["gvkey", "lag4_fyearq", "fqtr"],
          right_on=["gvkey", "fyearq", "fqtr"],
          how="left")
)

# Keep only clean 12-month gaps. Rows without a lag4 match get dropped too.
data2 = data2.with_columns(
    ((pl.col("datadate").cast(pl.Date) - pl.col("datadate_lag4").cast(pl.Date))
     .dt.total_days() / 30.44).round(0).cast(pl.Int32).alias("gap_months")
)

print("\nGap-month distribution:")
print(data2.filter(pl.col("gap_months").is_not_null())
      .group_by("gap_months").len().sort("gap_months"))

data2 = data2.filter(
    pl.col("datadate_lag4").is_not_null(),
    pl.col("gap_months") == 12,
)
add_step(7, "Less: obs without valid seasonal lag (missing lag4 row or 12-month gap)",
         data2.shape[0])


# =============================================================================
# PART 3: SUE and variables
# =============================================================================

# SUE = seasonal change in quarterly net income (ibq) scaled by prior-quarter
# market value of equity. See the R version's comments for the rationale on
# why we use dollar earnings / dollar mve instead of per-share EPS / price.

data3 = data2.with_columns(
    # Prior-quarter MVE (the SUE scaler)
    (pl.col("cshoq_lag1") * pl.col("prccq_lag1")).alias("mve_lag1"),
    # SUE
    ((pl.col("ibq") - pl.col("ibq_lag4"))
     / (pl.col("cshoq_lag1") * pl.col("prccq_lag1"))).alias("sue"),
    # Seasonal sales change
    (pl.col("saleq") - pl.col("saleq_lag4")).alias("delta_saleq"),
    # SameSign: 1 if earnings change and sales change move in same direction
    (((pl.col("ibq") - pl.col("ibq_lag4")).sign()
      == (pl.col("saleq") - pl.col("saleq_lag4")).sign())
     .cast(pl.Int32)).alias("same_sign"),
    # Loss indicator
    (pl.col("ibq") < 0).cast(pl.Int32).alias("loss"),
    # Fama-French 12 industry classification (from utils.py)
    *assign_ff12("siccd"),
    # Current MVE and log MVE
    (pl.col("cshoq") * pl.col("prccq")).alias("mve"),
    (pl.col("cshoq") * pl.col("prccq")).log().alias("log_mve"),
)

print(f"\nSUE summary: {data3['sue'].describe()}")
print(f"SameSign rate: {data3['same_sign'].mean():.3f}")


# =============================================================================
# PART 4: Sample filters and winsorization
# =============================================================================

# Penny stock filter
data4 = data3.filter(
    pl.col("prccq_lag1").is_not_null(),
    pl.col("prccq_lag1") > 1,
)
add_step(8, "Less: penny stocks (prccq_lag1 <= $1)", data4.shape[0])

# Require non-missing values for remaining key variables.
data4 = data4.filter(
    pl.col("sue").is_not_null() & pl.col("sue").is_finite(),
    pl.col("same_sign").is_not_null(),
    pl.col("mve") > 0,
    pl.col("mve_lag1") > 0,
)

print(f"\nAfter filters: {data4.shape[0]:,} rows")


# Winsorize SUE and log_mve at 1%/99%
def winsorize(col: str, lower: float = 0.01, upper: float = 0.99) -> pl.Expr:
    """Winsorize a column at the given quantiles."""
    lo = pl.col(col).quantile(lower, interpolation="lower")
    hi = pl.col(col).quantile(upper, interpolation="higher")
    return pl.col(col).clip(lo, hi).alias(col)

data4 = data4.with_columns(
    winsorize("sue"),
    winsorize("log_mve"),
)

print(f"SUE quantiles (winsorized):")
print(data4["sue"].quantile([0, 0.01, 0.25, 0.5, 0.75, 0.99, 1]))


# =============================================================================
# PART 5: Build trading calendar and event windows
# =============================================================================

# Extract distinct trading dates from the CRSP daily parquet via DuckDB.
con = duckdb.connect()
trading_dates = con.sql(f"""
    SELECT DISTINCT dlycaldt AS date
    FROM read_parquet('{raw_data_dir}/crsp-dsf-v2.parquet')
    ORDER BY date
""").pl()
con.close()

# Add a sequential trading-day number
trading_dates = trading_dates.with_row_index("td", offset=1)

print(f"Trading dates: {trading_dates.shape[0]:,}")


# Expand each event to a [-5, +5] trading day window.
firm_events = data4.select("permno", "rdq").unique()

# Map each rdq to its trading-day number using an asof join. We use
# strategy="forward" to find the nearest trading day ON OR AFTER rdq.
# This matters for weekend/holiday rdqs: a Saturday rdq maps to Monday
# (the next trading day), not Friday (the previous one). If we used
# "backward", a Friday+Saturday rdq pair for the same permno would both
# map to Friday, creating overlapping event windows.
firm_events = (firm_events
    .sort("rdq")
    .join_asof(
        trading_dates.rename({"date": "rdq_td_date"}),
        left_on="rdq",
        right_on="rdq_td_date",
        strategy="forward",
    )
)

# Create offsets [-5, +5] for each event
offsets = pl.DataFrame({"offset": list(range(-5, 6))})

event_dates = (firm_events
    .join(offsets, how="cross")
    .with_columns((pl.col("td") + pl.col("offset")).cast(pl.UInt32).alias("target_td"))
    .join(trading_dates, left_on="target_td", right_on="td", how="inner")
    .select("permno", "rdq", "date", "offset")
)

print(f"Event dates: {event_dates.shape[0]:,}")


# =============================================================================
# PART 6: Pull event-window returns from CRSP (DuckDB)
# =============================================================================

# Join event dates against the full CRSP daily parquet via DuckDB SQL.
# DuckDB reads the parquet directly and only pulls the rows we need.

start = time.time()

con = duckdb.connect()

# Register the polars DataFrame as a DuckDB view
con.register("events", event_dates.select("permno", "date", "offset"))

crsp_rets = con.sql(f"""
    SELECT ev.permno, ev.date, ev.offset,
           dsf.dlyret AS ret,
           idx.dlytotret AS vwretd
    FROM events AS ev
    INNER JOIN read_parquet('{raw_data_dir}/crsp-dsf-v2.parquet') AS dsf
      ON ev.permno = dsf.permno AND ev.date = dsf.dlycaldt
    INNER JOIN read_parquet('{raw_data_dir}/crsp-index.parquet') AS idx
      ON ev.date = idx.dlycaldt
""").pl()

con.close()

print(f"\nCRSP returns: {crsp_rets.shape[0]:,} rows, {time.time() - start:.1f}s")


# =============================================================================
# PART 7: Compute BHARs
# =============================================================================

# BHAR = Buy-and-Hold Abnormal Return
# = cumulative stock return minus cumulative market return over the window

# --- Regression BHAR: [-1, +1] ---

bhar_reg = (event_dates
    .select("permno", "rdq", "date", "offset")
    .join(crsp_rets, on=["permno", "date", "offset"], how="inner")
    .filter(pl.col("offset").is_between(-1, 1))
    .group_by("permno", "rdq")
    .agg(
        # BHAR = product(1 + ret) - product(1 + mkt_ret)
        ((1 + pl.col("ret")).product() - (1 + pl.col("vwretd")).product()).alias("bhar"),
        pl.len().alias("n_days"),
    )
)

print(f"BHAR reg: {bhar_reg.shape[0]:,} rows")


# --- Figure BHAR: cumulative at each offset [-5, +5] ---

bhar_fig = (event_dates
    .select("permno", "rdq", "date", "offset")
    .join(crsp_rets, on=["permno", "date", "offset"], how="inner")
    .sort("permno", "rdq", "offset")
    .with_columns(
        ((1 + pl.col("ret")).cum_prod().over("permno", "rdq") - 1).alias("cum_ret"),
        ((1 + pl.col("vwretd")).cum_prod().over("permno", "rdq") - 1).alias("cum_mkt"),
    )
    .with_columns(
        (pl.col("cum_ret") - pl.col("cum_mkt")).alias("bhar_cum"),
    )
)

print(f"BHAR fig: {bhar_fig.shape[0]:,} rows")


# =============================================================================
# PART 8: Create final analysis datasets
# =============================================================================

# Merge BHAR back to firm-quarter data for regressions.
regdata = (data4
    .join(bhar_reg, on=["permno", "rdq"], how="inner")
    .filter(pl.col("n_days") == 3)
)

add_step(9, "Less: obs with missing data (SUE, size, BHAR window, etc.)",
         regdata.shape[0])

print(f"\nFinal regression sample: {regdata.shape[0]:,} rows")

# Quick look at SUE vs BHAR
print("\nSUE decile means:")
print(regdata
    .with_columns(pl.col("sue").qcut(10, labels=[str(i) for i in range(1, 11)])
                  .alias("sue_decile"))
    .group_by("sue_decile")
    .agg(pl.col("bhar").mean().alias("mean_bhar"), pl.len().alias("n"))
    .sort("sue_decile"))


# Save output ------------------------------------------------------------------

# Drop the gap_months helper (only used for the 12-month verification)
regdata = regdata.drop("gap_months")

regdata.write_parquet(f"{data_dir}/regdata.parquet")
bhar_fig.write_parquet(f"{data_dir}/figure-data.parquet")
trading_dates.write_parquet(f"{data_dir}/trading-dates.parquet")

# Save .dta for Stata users (polars → pandas → .to_stata)
regdata.to_pandas().to_stata(f"{data_dir}/regdata.dta", write_index=False)

# Sample selection table
sel_df = pl.DataFrame(sample_selection)
print("\nSample selection:")
print(sel_df)
sel_df.write_parquet(f"{data_dir}/sample-selection.parquet")
# Also save as .dta so the Stata script can read it
sel_df.to_pandas().to_stata(f"{data_dir}/sample-selection.dta", write_index=False)

print("\nDone. Output saved to:", data_dir)
