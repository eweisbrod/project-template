# 4-analyze-data.py
# ===========================================================================
# Regression analysis and table output for the earnings event study.
#
# This script produces:
#   1. Sample selection table (LaTeX via great_tables)
#   2. Frequency table by decade (LaTeX via great_tables)
#   3. Descriptive statistics (LaTeX via great_tables)
#   4. Correlation matrix (LaTeX via great_tables)
#   5. Regression table with SUE x SameSign interaction (LaTeX via pyfixest)
#
# Tables 1-4 use great_tables (by Posit), which provides a clean API for
# building publication-ready tables and exporting to LaTeX. Table 5 uses
# pyfixest's native etable(type="tex") because great_tables' LaTeX export
# does not yet support row stubs/groups, which pyfixest uses for the
# regression layout.
#
# HOW TO RUN:
#   uv run src/4-analyze-data.py
# ===========================================================================


# Setup ------------------------------------------------------------------------

import os
import sys

import great_tables as gt
import polars as pl
import pandas as pd
import pyfixest as pf
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(".env", override=True)
data_dir = os.getenv("DATA_DIR")
output_dir = os.getenv("OUTPUT_DIR")

os.makedirs(output_dir, exist_ok=True)


# Read in the data from the previous step --------------------------------------

regdata = pl.read_parquet(f"{data_dir}/regdata.parquet")

# Standardize continuous controls for the interaction model.
regdata = regdata.with_columns(
    ((pl.col("log_mve") - pl.col("log_mve").mean())
     / pl.col("log_mve").std()).alias("log_mve_std")
)

print(f"regdata: {regdata.shape[0]:,} rows")


# =============================================================================
# Table 1: Sample Selection
# =============================================================================

# Read the step-by-step observation counts produced by script 2.
sample_sel = pl.read_parquet(f"{data_dir}/sample-selection.parquet")

# Compute the (Diff) column
sel_pd = sample_sel.to_pandas()
sel_pd["diff"] = sel_pd["obs"].shift(1) - sel_pd["obs"]
sel_pd["(Diff)"] = sel_pd["diff"].apply(
    lambda x: f"({x:,.0f})" if pd.notna(x) else ""
)
sel_pd["Obs"] = sel_pd["obs"].apply(lambda x: f"{x:,.0f}")

tbl = (gt.GT(sel_pd[["step", "description", "Obs", "(Diff)"]])
    .cols_label(step="Step", description="Description")
)

with open(f"{output_dir}/sample-selection-py.tex", "w", encoding="utf-8") as f:
    f.write(tbl.as_latex())
print("Table 1 (sample selection) saved")


# =============================================================================
# Table 2: Frequency by Decade
# =============================================================================

decade_data = (regdata
    .with_columns(
        pl.when(pl.col("fyearq").is_between(1970, 1979)).then(pl.lit("1970 - 1979"))
          .when(pl.col("fyearq").is_between(1980, 1989)).then(pl.lit("1980 - 1989"))
          .when(pl.col("fyearq").is_between(1990, 1999)).then(pl.lit("1990 - 1999"))
          .when(pl.col("fyearq").is_between(2000, 2009)).then(pl.lit("2000 - 2009"))
          .when(pl.col("fyearq").is_between(2010, 2019)).then(pl.lit("2010 - 2019"))
          .when(pl.col("fyearq") >= 2020).then(pl.lit("2020+"))
          .alias("Year")
    )
    .group_by("Year")
    .agg(
        pl.len().alias("Firm-Quarters"),
        pl.col("same_sign").sum().alias("SameSign Quarters"),
        pl.col("same_sign").mean().alias("Pct. SameSign"),
    )
    .sort("Year")
)

# Add total row
total = pl.DataFrame(
    {"Year": "Total",
     "Firm-Quarters": regdata.shape[0],
     "SameSign Quarters": int(regdata["same_sign"].sum()),
     "Pct. SameSign": float(regdata["same_sign"].mean())},
    schema=decade_data.schema,
)
decade_data = pl.concat([decade_data, total])

tbl = (gt.GT(decade_data.to_pandas())
    .fmt_integer(columns="Firm-Quarters", use_seps=True)
    .fmt_integer(columns="SameSign Quarters", use_seps=True)
    .fmt_percent(columns="Pct. SameSign", decimals=2)
)

with open(f"{output_dir}/freqtable-py.tex", "w", encoding="utf-8") as f:
    f.write(tbl.as_latex())
print("Table 2 (frequency by decade) saved")


# =============================================================================
# Table 3: Descriptive Statistics
# =============================================================================

descrip_vars = ["bhar", "sue", "same_sign", "loss", "log_mve"]
descrip_labels = {
    "bhar": "BHAR[-1,+1]",
    "sue": "SUE",
    "same_sign": "SameSign",
    "loss": "LOSS",
    "log_mve": "ln(MVE)",
}

stats = []
for var in descrip_vars:
    col = regdata[var].drop_nulls()
    stats.append({
        "Variable": descrip_labels[var],
        "N": col.len(),
        "Mean": col.mean(),
        "SD": col.std(),
        "Min": col.min(),
        "P25": col.quantile(0.25),
        "Median": col.median(),
        "P75": col.quantile(0.75),
        "Max": col.max(),
    })

descrip_df = pd.DataFrame(stats)

tbl = (gt.GT(descrip_df)
    .fmt_integer(columns="N", use_seps=True)
    .fmt_number(columns=["Mean", "SD", "Min", "P25", "Median", "P75", "Max"], decimals=3)
)

with open(f"{output_dir}/descrip-py.tex", "w", encoding="utf-8") as f:
    f.write(tbl.as_latex())
print("Table 3 (descriptive stats) saved")


# =============================================================================
# Table 4: Correlation Matrix
# =============================================================================

corrdata = (regdata
    .select(
        pl.col("bhar").alias("BHAR"),
        pl.col("sue").alias("SUE"),
        pl.col("same_sign").cast(pl.Float64).alias("SameSign"),
        pl.col("loss").cast(pl.Float64).alias("LOSS"),
        pl.col("log_mve").alias("SIZE"),
    )
    .to_pandas()
)

corr = corrdata.corr()
corr.insert(0, " ", corr.index)

tbl = (gt.GT(corr)
    .fmt_number(columns=["BHAR", "SUE", "SameSign", "LOSS", "SIZE"], decimals=3)
    .cols_label(**{" ": ""})
)

with open(f"{output_dir}/corrtable-py.tex", "w", encoding="utf-8") as f:
    f.write(tbl.as_latex())
print("Table 4 (correlation matrix) saved")


# =============================================================================
# Table 5: Regression Table
# =============================================================================

# pyfixest uses the same formula syntax as R's fixest:
#   "y ~ x1 * x2 | fe1 + fe2"
# Two-way clustering: vcov={"CRV1": "permno+fyearq"}

reg_pd = regdata.to_pandas()

m1 = pf.feols("bhar ~ sue", data=reg_pd)
m2 = pf.feols("bhar ~ sue * same_sign", data=reg_pd)
m3 = pf.feols("bhar ~ sue * same_sign | fyearq", data=reg_pd)
m4 = pf.feols("bhar ~ sue * same_sign | fyearq + ff12num", data=reg_pd)
m5 = pf.feols("bhar ~ sue * same_sign + sue * log_mve_std + sue * loss | fyearq + ff12num",
               data=reg_pd)

for m in [m1, m2, m3, m4, m5]:
    m.vcov({"CRV1": "permno+fyearq"})

# Generate LaTeX via pyfixest's native etable(type="tex").
# We use etable(type="tex") rather than etable(type="gt").as_latex() because
# great_tables' LaTeX export does not yet support row stubs/groups, which
# pyfixest uses to organize coefficients, FE indicators, and fit statistics.
# coef_fmt controls the display: b* adds significance stars to the coefficient,
# t:.2f formats t-statistics to 2 decimal places.
# felabels renames the raw FE variable names to plain English.
# custom_model_stats adds R² Within and Controls indicator rows.
# Labels use LaTeX math mode for variable names.
tex = pf.etable(
    [m1, m2, m3, m4, m5],
    type="tex",
    coef_fmt="b* \n (t:.2f)",
    signif_code=[0.01, 0.05, 0.1],
    keep=["sue$", "^same_sign$", "^sue:same_sign$"],
    labels={
        "sue": "$SUE$",
        "same_sign": "$SameSign$",
        "sue:same_sign": "$SUE \\times SameSign$",
    },
    # pyfixest uses slightly different internal FE names when FEs are absorbed
    # alone vs. together (e.g., "fyearq" vs "fyearq "). Map all variants.
    felabels={
        "fyearq": "Year FE",
        "fyearq ": "Year FE",
        "ff12num": "Industry FE",
        " ff12num": "Industry FE",
    },
    # Use show_fe=False and handle FE indicators via custom_model_stats,
    # because pyfixest's auto FE display duplicates "Year FE" when fyearq
    # appears in different absorb configurations across models.
    show_fe=False,
    custom_model_stats={
        "Year FE": ["", "", "Yes", "Yes", "Yes"],
        "Industry FE": ["", "", "", "Yes", "Yes"],
        "Controls": ["", "", "", "", "Yes"],
        "$R^2$ Within": [
            f"{m._r2_within:.3f}" if (m._r2_within is not None
                                      and str(m._r2_within) != "nan") else ""
            for m in [m1, m2, m3, m4, m5]
        ],
    },
    model_heads=["Base", "Interaction", "Year FE", "Two-Way FE", "Controls"],
)

with open(f"{output_dir}/regression-py.tex", "w", encoding="utf-8") as f:
    f.write(tex)
print("Table 5 (regression) saved")

# Print regression to console for review (plain text labels for markdown)
pf.etable(
    [m1, m2, m3, m4, m5],
    type="md",
    coef_fmt="b* \n (t:.2f)",
    signif_code=[0.01, 0.05, 0.1],
    keep=["sue$", "^same_sign$", "^sue:same_sign$"],
    labels={
        "sue": "SUE",
        "same_sign": "SameSign",
        "sue:same_sign": "SUE x SameSign",
    },
    felabels={
        "fyearq": "Year FE",
        "fyearq ": "Year FE",
        "ff12num": "Industry FE",
        " ff12num": "Industry FE",
    },
    show_fe=False,
    custom_model_stats={
        "Year FE": ["", "", "Yes", "Yes", "Yes"],
        "Industry FE": ["", "", "", "Yes", "Yes"],
        "Controls": ["", "", "", "", "Yes"],
        "R² Within": [
            f"{m._r2_within:.3f}" if (m._r2_within is not None
                                      and str(m._r2_within) != "nan") else ""
            for m in [m1, m2, m3, m4, m5]
        ],
    },
    model_heads=["Base", "Interaction", "Year FE", "Two-Way FE", "Controls"],
)

print(f"\nAll tables saved to: {output_dir}")
