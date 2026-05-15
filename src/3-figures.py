# ==============================================================================
# 3-figures.py
#
# Purpose:
#   Produce publication-ready figures for the earnings event study using
#   plotnine (Grammar of Graphics in Python — nearly identical syntax to
#   ggplot2 in R, making the two implementations easy to compare).
#
# Inputs (from DATA_DIR):
#   regdata.parquet
#   figure-data.parquet
#
# Outputs (to OUTPUT_DIR):
#   ff12_fig.{pdf,png}      Bar chart: SameSign frequency by FF12 industry
#   size_year.{pdf,png}     Multi-line time series: SameSign by size quintile
#   corr_fig.{pdf,png}      Correlation matrix heatmap (seaborn)
#   car_fig.{pdf,png}       Event-study CAR plot, four lines (SUE × SameSign)
#   erc_year.{pdf,png}      Year-by-year ERC with confidence bands
#
# Notes:
#   - Run via `uv run src/3-figures.py`.
#   - PDF for LaTeX, PNG for Word / PowerPoint. Both are saved every run.
# ==============================================================================


# Setup ------------------------------------------------------------------------

import os
import sys

import numpy as np
import polars as pl
import statsmodels.api as sm
from plotnine import *
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(".env", override=True)
data_dir = os.getenv("DATA_DIR")
output_dir = os.getenv("OUTPUT_DIR")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)


# Read in the data from the previous step --------------------------------------

# regdata: one row per firm-quarter, with SUE, BHAR, controls
regdata = pl.read_parquet(f"{data_dir}/regdata.parquet")

# figdata: one row per firm-quarter-offset, for the CAR event window plot
figdata = pl.read_parquet(f"{data_dir}/figure-data.parquet")

print(f"regdata: {regdata.shape[0]:,} rows")
print(f"figdata: {figdata.shape[0]:,} rows")


# Figure 1: SameSign Frequency by Industry -------------------------------------

# Horizontal bar chart showing how often earnings and sales move in the same
# direction (SameSign) within each FF12 industry. Higher rates suggest
# earnings changes are more often "confirmed" by revenue movements.

fig1_data = (regdata
    .group_by("FF12")
    .agg(pl.col("same_sign").mean().alias("pct_same_sign"))
    .sort("pct_same_sign")
    # plotnine respects categorical ordering for the axis
    .with_columns(
        pl.col("FF12").cast(pl.Categorical).alias("FF12")
    )
    .to_pandas()
)

# Reorder the categories by pct_same_sign
import pandas as pd
fig1_data["FF12"] = pd.Categorical(
    fig1_data["FF12"],
    categories=fig1_data.sort_values("pct_same_sign")["FF12"],
    ordered=True,
)

fig1 = (
    ggplot(fig1_data, aes(x="FF12", y="pct_same_sign"))
    + geom_col(fill="#0051ba")
    + coord_flip()
    + scale_y_continuous(
        name="Freq. of SameSign (Earnings & Sales Same Direction)",
        labels=lambda x: [f"{v:.0%}" for v in x],
    )
    + scale_x_discrete(name="Fama-French Industry")
    + theme_bw(base_family="serif")
)

fig1.save(f"{output_dir}/ff12_fig.pdf", width=7, height=6)
fig1.save(f"{output_dir}/ff12_fig.png", width=4.2, height=3.6, dpi=150)
print("Figure 1 saved")


# Figure 2: SameSign Frequency by Size Quintile Over Time ----------------------

# Multi-line time series showing SameSign frequency by size quintile over time.

fig2_data = (regdata
    .with_columns(
        pl.col("mve")
        .qcut(5, labels=["1", "2", "3", "4", "5"])
        .over("fyearq")
        .cast(pl.String)
        .alias("size_qnt")
    )
    .filter(pl.col("size_qnt").is_not_null())
    .group_by("fyearq", "size_qnt")
    .agg(pl.col("same_sign").mean().alias("pct_same_sign"))
    .sort("fyearq", "size_qnt")
    .to_pandas()
)

fig2 = (
    ggplot(fig2_data, aes(x="fyearq", y="pct_same_sign", color="size_qnt"))
    + geom_line()
    + geom_point()
    + scale_y_continuous(
        name="Freq. of SameSign",
        labels=lambda x: [f"{v:.0%}" for v in x],
    )
    + scale_x_continuous(name="Year", breaks=range(1970, 2026, 5))
    + scale_color_discrete(name="Size Quintile")
    + theme_bw(base_family="serif")
)

fig2.save(f"{output_dir}/size_year.pdf", width=7, height=6)
fig2.save(f"{output_dir}/size_year.png", width=7, height=6, dpi=150)
print("Figure 2 saved")


# Figure 3: Correlation Matrix -------------------------------------------------

# Heatmap of pairwise correlations. We use seaborn here (not plotnine)
# because correlation matrix heatmaps are cleaner with seaborn's heatmap().

import matplotlib.pyplot as plt
import seaborn as sns

corrdata = (regdata
    .select(
        pl.col("bhar").alias("BHAR"),
        pl.col("sue").alias("SUE"),
        pl.col("same_sign").alias("SameSign"),
        pl.col("loss").alias("LOSS"),
        pl.col("log_mve").alias("SIZE"),
    )
    .to_pandas()
)

correlation = corrdata.corr()

fig3, ax = plt.subplots(figsize=(7, 6))
sns.heatmap(
    correlation,
    annot=True,
    fmt=".2f",
    cmap=sns.diverging_palette(0, 240, as_cmap=True),
    center=0,
    square=True,
    linewidths=0.5,
    ax=ax,
)
ax.set_title("")
plt.tight_layout()
fig3.savefig(f"{output_dir}/corr_fig.pdf")
fig3.savefig(f"{output_dir}/corr_fig.png", dpi=150)
plt.close(fig3)
print("Figure 3 saved")


# Figure 4: Event Study CAR Plot ----------------------------------------------

# Cumulative abnormal returns over [-5, +5] trading days around earnings
# announcements, split by SUE direction × SameSign.

car_data = (figdata
    .join(
        regdata.select("permno", "rdq", "same_sign", "sue"),
        on=["permno", "rdq"],
        how="inner",
    )
    .with_columns(
        pl.when(pl.col("sue") > 0).then(pl.lit("Positive SUE"))
          .when(pl.col("sue") < 0).then(pl.lit("Negative SUE"))
          .otherwise(pl.lit(None))
          .alias("sue_dir"),
        pl.when(pl.col("same_sign") == 1).then(pl.lit("Same Sign"))
          .otherwise(pl.lit("Diff Sign"))
          .alias("same_sign_lbl"),
    )
    .filter(pl.col("sue_dir").is_not_null())
)

car_summary = (car_data
    .group_by("sue_dir", "same_sign_lbl", "offset")
    .agg(
        pl.col("bhar_cum").mean().alias("mean_bhar"),
        (pl.col("bhar_cum").std() / pl.col("bhar_cum").len().sqrt()).alias("se"),
        pl.len().alias("n"),
    )
    .sort("sue_dir", "same_sign_lbl", "offset")
    .to_pandas()
)

# Create a group column for the interaction
car_summary["group"] = car_summary["sue_dir"] + " / " + car_summary["same_sign_lbl"]

fig4 = (
    ggplot(car_summary, aes(x="offset", y="mean_bhar",
                            color="sue_dir", linetype="same_sign_lbl",
                            group="group"))
    + geom_hline(yintercept=0, linetype="dashed", color="gray")
    + geom_line(size=0.8)
    + geom_point(size=2)
    + scale_x_continuous(
        name="Trading Days Relative to Announcement",
        breaks=range(-5, 6),
    )
    + scale_y_continuous(
        name="Cumulative BHAR",
        labels=lambda x: [f"{v:.1%}" for v in x],
    )
    + scale_color_manual(
        name="Earnings Surprise",
        values={"Positive SUE": "#0051ba", "Negative SUE": "#c41230"},
    )
    + scale_linetype_manual(
        name="SameSign",
        values={"Same Sign": "solid", "Diff Sign": "dashed"},
    )
    + theme_bw(base_family="serif")
    + theme(legend_position="bottom")
)

fig4.save(f"{output_dir}/car_fig.pdf", width=7, height=6)
fig4.save(f"{output_dir}/car_fig.png", width=7, height=6, dpi=150)
print("Figure 4 saved")


# Figure 5: Year-by-Year ERC with Confidence Bands ----------------------------

# The earnings response coefficient (ERC) — slope of BHAR on SUE — estimated
# separately for each year, split by SameSign. Confidence bands show the
# precision of each year's estimate.

reg_pd = regdata.select("fyearq", "same_sign", "bhar", "sue").to_pandas()

# Fit OLS by (fyearq, same_sign) and extract SUE coefficient + CI
erc_rows = []
for (year, ss), grp in reg_pd.groupby(["fyearq", "same_sign"]):
    X = sm.add_constant(grp["sue"])
    model = sm.OLS(grp["bhar"], X).fit()
    ci = model.conf_int().loc["sue"]
    erc_rows.append({
        "fyearq": year,
        "same_sign": ss,
        "estimate": model.params["sue"],
        "conf_low": ci[0],
        "conf_high": ci[1],
    })

ercdata = pd.DataFrame(erc_rows)
ercdata["same_sign_lbl"] = ercdata["same_sign"].map(
    {0: "Different Sign", 1: "Same Sign"}
)

fig5 = (
    ggplot(ercdata, aes(x="fyearq", y="estimate"))
    + geom_ribbon(aes(ymin="conf_low", ymax="conf_high", group="same_sign_lbl"),
                  fill="#CCCCCC", alpha=0.5)
    + geom_line(aes(color="same_sign_lbl"))
    + geom_point(aes(color="same_sign_lbl"))
    + scale_x_continuous(name="Year", breaks=range(1970, 2026, 5))
    + scale_y_continuous(name="ERC (Coefficient on SUE)")
    + scale_color_manual(
        name=" ",
        values={"Same Sign": "#0051ba", "Different Sign": "#c41230"},
    )
    + theme_bw(base_family="serif")
    + theme(legend_position="bottom")
)

fig5.save(f"{output_dir}/erc_year.pdf", width=7, height=6)
fig5.save(f"{output_dir}/erc_year.png", width=7, height=6, dpi=150)
print("Figure 5 saved")

print(f"\nAll figures saved to: {output_dir}")
