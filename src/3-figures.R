# ==============================================================================
# 3-figures.R
#
# Purpose:
#   Produce publication-ready figures for the earnings event study using
#   ggplot2.
#
# Inputs (from DATA_DIR):
#   regdata.parquet
#   figure-data.parquet
#
# Outputs (to OUTPUT_DIR):
#   ff12_fig.{pdf,png}      Bar chart: SameSign frequency by FF12 industry
#   size_year.{pdf,png}     Multi-line time series: SameSign by size quintile
#   corr_fig.{pdf,png}      Correlation matrix heatmap (corrplot)
#   car_fig.{pdf,png}       Event-study CAR plot over the [-5,+5] day window
#   erc_year.{pdf,png}      Year-by-year ERC with confidence bands
#
# Notes:
#   - PDF for LaTeX, PNG for Word / PowerPoint. Both are saved every run.
#   - Demonstrates: geom_col + coord_flip; multi-line time series with
#     grouped aesthetics; corrplot heatmap; nest_by + broom::tidy for
#     year-by-year coefficient extraction with geom_ribbon for the CIs.
# ==============================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, lubridate, glue, arrow, forcats, corrplot,
               fixest, broom,
               tidyverse)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")


# Read in the data from the previous step --------------------------------------

# regdata: one row per firm-quarter, with SUE, BHAR, controls
regdata <- read_parquet(glue("{data_dir}/regdata.parquet"))

# figdata: one row per firm-quarter-offset, for the CAR event window plot
figdata <- read_parquet(glue("{data_dir}/figure-data.parquet"))

nrow(regdata)
nrow(figdata)


# Figure 1: SameSign Frequency by Industry -------------------------------------

# Horizontal bar chart showing how often earnings and sales move in the same
# direction (our key interaction variable, SameSign) within each FF12 industry.
# Higher SameSign rates suggest earnings changes are more often "confirmed" by
# revenue movements — relevant background for the SUE × SameSign regression.
#
# This teaches:
#   - geom_col() for bar charts from summarized data
#   - coord_flip() to make horizontal bars (easier to read industry names)
#   - fct_reorder() to sort bars by value instead of alphabetical order
#   - scales::percent for axis formatting
#   - custom fill color

fig <- regdata |>
  group_by(FF12) |>
  summarize(pct_same_sign = mean(same_sign, na.rm = TRUE)) |>
  # Reorder industries by % SameSign so the chart is sorted, not alphabetical
  mutate(FF12 = fct_reorder(factor(FF12), pct_same_sign)) |>
  ggplot(aes(x = FF12, y = pct_same_sign)) +
  geom_col(fill = "#0051ba") +
  # Fill color = Kansas Blue: https://brand.ku.edu/guidelines/design/color
  scale_y_continuous(name = "Freq. of SameSign (Earnings & Sales Same Direction)",
                     labels = scales::percent) +
  scale_x_discrete(name = "Fama-French Industry") +
  coord_flip() +
  # base_family = "serif" sets font to Times New Roman (or similar serif font)
  theme_bw(base_family = "serif")

# Look at it in R
fig

# For LaTeX output you might want to output to PDF
ggsave(glue("{output_dir}/ff12_fig.pdf"), fig, width = 7, height = 6)

# For Word output you might want to output to an image such as .png
ggsave(glue("{output_dir}/ff12_fig.png"), fig, width = 4.2, height = 3.6)


# Figure 2: SameSign Frequency by Size Quintile Over Time ---------------------

# Multi-line time series showing SameSign frequency by size quintile over time.
# Looking at how SameSign rates differ across firm size and across years gives
# a feel for how much variation our interaction variable has in the panel.
#
# This teaches:
#   - ntile() to create quantile groups
#   - Grouped aesthetics (color + linetype mapped to the same variable)
#   - Giving two scales the same name so they share one legend

fig <- regdata |>
  group_by(fyearq) |>
  # Create size quintiles within each year
  mutate(size_qnt = factor(ntile(mve, 5))) |>
  group_by(fyearq, size_qnt) |>
  summarize(pct_same_sign = mean(same_sign, na.rm = TRUE), .groups = "drop") |>
  filter(!is.na(pct_same_sign)) |>
  ggplot(aes(x = fyearq, y = pct_same_sign, color = size_qnt, linetype = size_qnt)) +
  geom_line() + geom_point() +
  scale_y_continuous(name = "Freq. of SameSign", labels = scales::percent) +
  scale_x_continuous(name = "Year", breaks = seq(1970, 2025, 5)) +
  # If you give these two scales the same name, they share one legend
  scale_color_discrete(name = "Size Quintile") +
  scale_linetype_discrete(name = "Size Quintile") +
  theme_bw(base_family = "serif")

fig

ggsave(glue("{output_dir}/size_year.pdf"), fig, width = 7, height = 6)
ggsave(glue("{output_dir}/size_year.png"), fig, width = 7, height = 6)


# Figure 3: Correlation Matrix -------------------------------------------------

# Heatmap of pairwise correlations using the corrplot package.
# This is NOT ggplot — corrplot uses base R graphics. It's included here
# because correlation matrices are standard in empirical papers and corrplot
# makes them look nice with minimal code.
#
# Note the pdf() / dev.off() pattern: corrplot writes to the active graphics
# device, so we open a PDF device first, draw the plot, then close it.

corrdata <- regdata |>
  select(
    `BHAR`     = bhar,
    `SUE`      = sue,
    `SameSign` = same_sign,
    `LOSS`     = loss,
    `SIZE`     = log_mve
  )

corrdata

correlation <- cor(corrdata, use = "pairwise.complete.obs")
col2 <- colorRampPalette(c('red', 'white', 'blue'))

# Save to PDF
pdf(file = glue("{output_dir}/corr_fig.pdf"))
corrplot(correlation, method = 'square',
         addCoef.col = 'black',
         diag = FALSE,
         tl.col = 'black',
         type = 'full',
         tl.cex = 1,
         tl.srt = 0,
         tl.offset = 1,
         number.cex = 0.7,
         cl.ratio = 0.1,
         cl.pos = "r",
         col = col2(20),
         win.asp = .8)
dev.off()


# Figure 4: Event Study CAR Plot ----------------------------------------------

# The classic event study figure: cumulative abnormal returns over the
# [-5, +5] trading day window around earnings announcements.
#
# We split by SameSign (our interaction variable) to show that the market
# reacts more strongly when earnings and sales move in the same direction.
#
# This teaches:
#   - Working with event-time data (offset on x-axis)
#   - geom_hline() for a zero reference line
#   - Grouping by a categorical variable to compare subsamples
#   - Computing means and confidence intervals by group

# Merge SUE and SameSign from regdata into the figure data, then cross them
# into the four cells our regression estimates: SUE direction × SameSign.
car_data <- figdata |>
  inner_join(
    regdata |> select(permno, rdq, same_sign, sue),
    by = c("permno", "rdq")
  ) |>
  mutate(
    sue_dir = case_when(
      sue > 0 ~ "Positive SUE",
      sue < 0 ~ "Negative SUE",
      TRUE    ~ NA_character_
    ),
    same_sign_lbl = if_else(same_sign == 1L, "Same Sign", "Diff Sign")
  ) |>
  filter(!is.na(sue_dir))

# Average cumulative BHAR at each offset, by SUE direction × SameSign
car_summary <- car_data |>
  group_by(sue_dir, same_sign_lbl, offset) |>
  summarize(
    mean_bhar = mean(bhar_cum, na.rm = TRUE),
    se = sd(bhar_cum, na.rm = TRUE) / sqrt(n()),
    n = n(),
    .groups = "drop"
  )

# Color = SUE direction (substantive sign of the surprise);
# Linetype = SameSign (whether sales confirm earnings).
# If SameSign matters, the gap between solid and dashed within each color
# should widen as we move away from the announcement.
fig <- car_summary |>
  ggplot(aes(x = offset, y = mean_bhar,
             color = sue_dir, linetype = same_sign_lbl,
             group = interaction(sue_dir, same_sign_lbl))) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2) +
  scale_x_continuous(name = "Trading Days Relative to Announcement",
                     breaks = -5:5) +
  scale_y_continuous(name = "Cumulative BHAR", labels = scales::percent) +
  scale_color_manual(name = "Earnings Surprise",
                     values = c("Positive SUE" = "#0051ba",
                                "Negative SUE" = "#c41230")) +
  scale_linetype_manual(name = "SameSign",
                        values = c("Same Sign" = "solid",
                                   "Diff Sign" = "dashed")) +
  theme_bw(base_family = "serif") +
  theme(legend.position = "bottom")

fig

ggsave(glue("{output_dir}/car_fig.pdf"), fig, width = 7, height = 6)
ggsave(glue("{output_dir}/car_fig.png"), fig, width = 7, height = 6)


# Figure 5: Year-by-Year ERC with Confidence Bands ----------------------------

# This plots the earnings response coefficient (ERC) — the slope of
# BHAR on SUE — estimated separately for each year. Confidence bands
# show the precision of the estimate in each year.
#
# This teaches:
#   - nest_by() to split data into groups and fit models to each
#   - broom::tidy() to extract regression coefficients as a tidy data.frame
#   - geom_ribbon() for confidence interval bands
#   - The concept of time-varying coefficients (how the ERC changes over time)

# We split by SameSign to see if the ERC differs when sales confirm earnings

ercdata <- regdata |>
  # nest_by creates a grouped data frame with a list-column of data
  nest_by(fyearq, same_sign) |>
  # Fit a simple regression of BHAR on SUE within each year × SameSign group
  mutate(
    fit = list(lm(bhar ~ sue, data = data))
  ) |>
  # Extract coefficients with confidence intervals
  reframe(broom::tidy(fit, conf.int = TRUE)) |>
  # Drop the intercept — we only want to plot the SUE coefficient
  filter(term != "(Intercept)")

# can also use this setup to do Fama-Macbeth regressions, etc.
# can also use pmg package for Fama-Macbeth

fig <- ercdata |>
  mutate(same_sign = factor(same_sign, labels = c("Different Sign", "Same Sign"))) |>
  ggplot(aes(x = fyearq, y = estimate)) +
  geom_ribbon(aes(ymin = conf.low, ymax = conf.high, group = same_sign),
              fill = "grey80") +
  geom_line(aes(color = same_sign)) +
  geom_point(aes(color = same_sign)) +
  scale_x_continuous(name = "Year", breaks = seq(1970, 2025, 5)) +
  scale_y_continuous(name = "ERC (Coefficient on SUE)") +
  scale_color_manual(name = "",
                     values = c("Same Sign" = "#0051ba",
                                "Different Sign" = "#c41230")) +
  theme_bw(base_family = "serif") +
  theme(legend.position = "bottom")

fig

ggsave(glue("{output_dir}/erc_year.pdf"), fig, width = 7, height = 6)
ggsave(glue("{output_dir}/erc_year.png"), fig, width = 7, height = 6)
