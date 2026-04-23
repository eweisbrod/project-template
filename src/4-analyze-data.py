# 4-analyze-data.py
# ===========================================================================
# Regression analysis and table output for the earnings event study.
#
# This script produces:
#   1. Sample selection table (LaTeX)
#   2. Frequency table by decade (LaTeX)
#   3. Descriptive statistics (LaTeX)
#   4. Correlation matrix (LaTeX)
#   5. Regression table with SUE × SameSign interaction (LaTeX via pyfixest)
#
# pyfixest is modeled on R's fixest package — it uses the same formula syntax
# (e.g., "y ~ x | fe1 + fe2") and supports multi-way clustering, high-
# dimensional fixed effects, and formatted table output.
#
# HOW TO RUN:
#   uv run src/4-analyze-data.py
# ===========================================================================


# Setup ------------------------------------------------------------------------

import os
import sys

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

# Standardize continuous controls so the SUE main effect in interaction models
# is interpretable at the mean of the controls (see R version for details).
regdata = regdata.with_columns(
    ((pl.col("log_mve") - pl.col("log_mve").mean())
     / pl.col("log_mve").std()).alias("log_mve_std")
)

print(f"regdata: {regdata.shape[0]:,} rows")


# Table 1: Sample Selection ----------------------------------------------------

# Step-by-step observation counts produced by script 2.

sample_sel = pl.read_parquet(f"{data_dir}/sample-selection.parquet")

# Build LaTeX table manually (simple enough that a template library isn't needed)
lines = [
    r"\begin{tabular}[t]{clrr}",
    r"\toprule",
    r"Step & Description & Obs & (Diff)\\",
    r"\midrule",
]

obs_list = sample_sel["obs"].to_list()
for i, row in enumerate(sample_sel.iter_rows(named=True)):
    diff_str = ""
    if i > 0:
        diff = obs_list[i - 1] - row["obs"]
        diff_str = f"({diff:,})"
    lines.append(f"{row['step']} & {row['description']} & {row['obs']:,} & {diff_str}\\\\")

lines.append(r"\bottomrule")
lines.append(r"\end{tabular}")

tex = "\n".join(lines)
with open(f"{output_dir}/sample-selection-py.tex", "w") as f:
    f.write(tex)
print("Table 1 (sample selection) saved")


# Table 2: Observations by Decade ----------------------------------------------

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

# Build LaTeX
lines = [
    r"\begin{tabular}[t]{lrrr}",
    r"\toprule",
    r"Year & Firm-Quarters & SameSign Quarters & Pct. SameSign\\",
    r"\midrule",
]
for row in decade_data.iter_rows(named=True):
    lines.append(
        f"{row['Year']} & {row['Firm-Quarters']:,} & "
        f"{row['SameSign Quarters']:,} & {row['Pct. SameSign']:.2%}\\\\"
    )
lines.append(r"\bottomrule")
lines.append(r"\end{tabular}")

with open(f"{output_dir}/freqtable-py.tex", "w") as f:
    f.write("\n".join(lines))
print("Table 2 (frequency by decade) saved")


# Table 3: Descriptive Statistics ----------------------------------------------

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
        "N": f"{col.len():,}",
        "Mean": f"{col.mean():.3f}",
        "SD": f"{col.std():.3f}",
        "Min": f"{col.min():.3f}",
        "P25": f"{col.quantile(0.25):.3f}",
        "Median": f"{col.median():.3f}",
        "P75": f"{col.quantile(0.75):.3f}",
        "Max": f"{col.max():.3f}",
    })

descrip_df = pd.DataFrame(stats)

lines = [
    r"\begin{tabular}[t]{lrrrrrrrr}",
    r"\toprule",
    " & ".join(descrip_df.columns) + r"\\",
    r"\midrule",
]
for _, row in descrip_df.iterrows():
    lines.append(" & ".join(str(v) for v in row) + r"\\")
lines.append(r"\bottomrule")
lines.append(r"\end{tabular}")

with open(f"{output_dir}/descrip-py.tex", "w") as f:
    f.write("\n".join(lines))
print("Table 3 (descriptive stats) saved")


# Table 4: Correlation Matrix --------------------------------------------------

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

lines = [
    r"\begin{tabular}[t]{l" + "r" * len(corr.columns) + "}",
    r"\toprule",
    " & " + " & ".join(corr.columns) + r"\\",
    r"\midrule",
]
for var in corr.index:
    vals = " & ".join(f"{corr.loc[var, c]:.3f}" for c in corr.columns)
    lines.append(f"{var} & {vals}\\\\")
lines.append(r"\bottomrule")
lines.append(r"\end{tabular}")

with open(f"{output_dir}/corrtable-py.tex", "w") as f:
    f.write("\n".join(lines))
print("Table 4 (correlation matrix) saved")


# Table 5: Regression Table ----------------------------------------------------

# The main event study regression:
#   BHAR = b1*SUE + b2*SameSign + b3*SUE×SameSign + controls + FE
#
# pyfixest uses the same formula syntax as R's fixest:
#   "y ~ x1 * x2 | fe1 + fe2"
#   vcov={"CRV1": "permno+fyearq"} for two-way clustering

# Convert to pandas for pyfixest (it requires pandas DataFrames)
reg_pd = regdata.to_pandas()

# Fit models — same 5 specifications as the R version
m1 = pf.feols("bhar ~ sue", data=reg_pd)
m2 = pf.feols("bhar ~ sue * same_sign", data=reg_pd)
m3 = pf.feols("bhar ~ sue * same_sign | fyearq", data=reg_pd)
m4 = pf.feols("bhar ~ sue * same_sign | fyearq + ff12num", data=reg_pd)
m5 = pf.feols("bhar ~ sue * same_sign + sue * log_mve_std + sue * loss | fyearq + ff12num",
               data=reg_pd)

# Apply two-way clustering to all models
for m in [m1, m2, m3, m4, m5]:
    m.vcov({"CRV1": "permno+fyearq"})

# Generate LaTeX regression table via pyfixest's etable()
# keep= shows only the coefficients we want (hides controls)
tex = pf.etable(
    [m1, m2, m3, m4, m5],
    type="tex",
    coef_fmt="b \n (t)",
    signif_code=[0.01, 0.05, 0.1],
    keep=["sue$", "^same_sign$", "^sue:same_sign$"],
    labels={
        "sue": "SUE",
        "same_sign": "SameSign",
        "sue:same_sign": "SUE x SameSign",
    },
    model_heads=["Base", "Interaction", "Year FE", "Two-Way FE", "Controls"],
)

with open(f"{output_dir}/regression-py.tex", "w") as f:
    f.write(tex)
print("Table 5 (regression) saved")

# Also print to console (etable type="md" prints directly, returns None)
pf.etable(
    [m1, m2, m3, m4, m5],
    type="md",
    coef_fmt="b \n (t)",
    signif_code=[0.01, 0.05, 0.1],
    keep=["sue$", "^same_sign$", "^sue:same_sign$"],
    labels={
        "sue": "SUE",
        "same_sign": "SameSign",
        "sue:same_sign": "SUE x SameSign",
    },
    model_heads=["Base", "Interaction", "Year FE", "Two-Way FE", "Controls"],
)

print(f"\nAll tables saved to: {output_dir}")
