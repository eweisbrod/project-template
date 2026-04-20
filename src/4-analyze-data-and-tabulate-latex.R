# 4-analyze-data-and-tabulate-latex.R
# ===========================================================================
# Regression analysis and LaTeX table output for the earnings event study.
#
# This script demonstrates:
#   1. Sample selection table (step-by-step obs counts from script 2)
#   2. Manual frequency table with kableExtra (observation counts by decade)
#   3. Descriptive statistics with modelsummary's datasummary()
#   4. Correlation matrix with datasummary_correlation()
#   5. Regression table with fixest + modelsummary (interaction terms, FE,
#      clustered SEs, dynamic FE indicators, controls row, custom formatting)
#
# Output: LaTeX .tex files that can be \input{} into an Overleaf document.
# See 4-analyze-data-and-tabulate-word.R for Word/Office output instead.
# ===========================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, modelsummary, sjlabelled, kableExtra, tinytable,
               formattable, lubridate, glue, arrow, fixest,
               tidyverse)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")

# This option prevents modelsummary from adding extra formatting to numbers
options(modelsummary_format_numeric_latex = "plain")


# Read in the data from the previous step --------------------------------------

regdata <- read_parquet(glue("{data_dir}/regdata.parquet")) |>
  # Standardize continuous controls for the interaction model below.
  #
  # Why standardize?
  #  - In a model with SUE x control interactions, the SUE main-effect
  #    coefficient is the ERC *evaluated at the value of the control where
  #    the control equals zero*. If the control is on a raw scale (e.g.,
  #    log market value around 6-10), "control = 0" is outside the data
  #    and the SUE coefficient is not meaningful. Standardizing the control
  #    (mean 0, sd 1) makes "control = 0" equal to the sample mean, so SUE
  #    measures the ERC for an average-size firm.
  #  - It also puts interaction coefficients on a per-standard-deviation
  #    scale, so SUE x log_mve_std and SUE x loss are comparable in
  #    magnitude.
  #
  # Binary controls (loss, same_sign) are NOT standardized — 0/1 is already
  # a meaningful scale and standardizing them would just re-scale the
  # coefficient without improving interpretation.
  #
  # standardize() is defined in utils.R -- it is simply
  #   (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
  # We use the custom helper rather than base::scale() because scale()
  # returns a 1-column matrix (not a numeric vector), which is awkward
  # inside a mutate() and usually needs an as.numeric() wrapper.
  mutate(log_mve_std = standardize(log_mve)) |>
  # Add variable labels for use in tables (LaTeX math notation)
  sjlabelled::var_labels(
    bhar        = "$BHAR_{[-1,+1]}$",
    sue         = "$SUE$",
    same_sign   = "$SameSign$",
    loss        = "$LOSS$",
    log_mve     = "$\\ln(MVE)$",
    log_mve_std = "$\\ln(MVE)^{std}$"
  )

nrow(regdata)


# Table 1: Sample Selection ----------------------------------------------------

# Step-by-step observation counts produced by script 2. The (Diff) column is
# the number of observations dropped at each filter, computed as lag(obs) - obs.

sample_selection <- read_parquet(glue("{data_dir}/sample-selection.parquet"))

sample_table <- sample_selection |>
  mutate(
    diff = dplyr::lag(obs) - obs,
    Obs  = formattable::comma(obs, digits = 0),
    `(Diff)` = if_else(is.na(diff), "",
                       paste0("(", formattable::comma(diff, digits = 0), ")"))
  ) |>
  transmute(Step = as.character(step),
            Description = description,
            Obs,
            `(Diff)`)

sample_table

kbl(sample_table, format = "latex", booktabs = TRUE,
    linesep = "", align = "clrr", escape = TRUE) |>
  save_kable(glue("{output_dir}/sample-selection-r.tex"))


# Table 2: Observations by Decade ----------------------------------------------

# Goal: show how to export a basic manual table into a LaTeX paper.
# We group quarterly observations by decade and count firms and SameSign events
# (quarters where the earnings change and sales change move in the same
# direction — our key interaction variable in Table 5).

table2 <- regdata |>
  mutate(Year = case_when(
    fyearq %in% 1970:1979 ~ "1970 - 1979",
    fyearq %in% 1980:1989 ~ "1980 - 1989",
    fyearq %in% 1990:1999 ~ "1990 - 1999",
    fyearq %in% 2000:2009 ~ "2000 - 2009",
    fyearq %in% 2010:2019 ~ "2010 - 2019",
    fyearq >= 2020 ~ "2020+")
  ) |>
  group_by(Year) |>
  summarize(`Firm-Quarters`     = formattable::comma(n(), digits = 0),
            `SameSign Quarters` = formattable::comma(sum(same_sign, na.rm = TRUE), digits = 0),
            `Pct. SameSign`     = formattable::percent(mean(same_sign, na.rm = TRUE), digits = 2))

# Add a total row
totalrow <- regdata |>
  summarize(`Firm-Quarters`     = formattable::comma(n(), digits = 0),
            `SameSign Quarters` = formattable::comma(sum(same_sign, na.rm = TRUE), digits = 0),
            `Pct. SameSign`     = formattable::percent(mean(same_sign, na.rm = TRUE), digits = 2)) |>
  mutate(Year = "Total")

table2 <- bind_rows(table2, totalrow)

# Look at the data frame
table2

# Export to LaTeX using kableExtra
kbl(table2, format = "latex", booktabs = TRUE, linesep = "") |>
  save_kable(glue("{output_dir}/freqtable-r.tex"))

# Alternative: tinytable version
tt(table2) |>
  format_tt(escape = TRUE) |>
  save_tt(glue("{output_dir}/freqtable-tiny-r.tex"), overwrite = TRUE)


# Table 3: Descriptive Statistics ----------------------------------------------

# Custom number format for the descriptive table
my_fmt <- function(x) formattable::comma(x, digits = 3)

# Custom N function: counts non-missing, formatted with commas, no decimals
NN <- function(x) {
  out <- if (is.logical(x) && all(is.na(x))) length(x) else sum(!is.na(x))
  formattable::comma(out, digits = 0)
}

# Select and label the variables for the table
descripdata <- regdata |>
  select(bhar, sue, same_sign, loss, log_mve) |>
  label_to_colnames()

# datasummary: variables on the left of ~, statistics on the right
datasummary(All(descripdata) ~ (N = NN) +
              (Mean + SD + Min + P25 + Median + P75 + Max) * Arguments(fmt = my_fmt),
            data = descripdata,
            escape = FALSE,
            output = glue("{output_dir}/descrip-r.tex"))


# Table 4: Correlation Matrix --------------------------------------------------

# Pearson above diagonal, Spearman below
datasummary_correlation(descripdata, method = "pearspear")

# Save to LaTeX
datasummary_correlation(descripdata,
                        method = "pearspear",
                        output = glue("{output_dir}/corrtable-r.tex"),
                        escape = FALSE)


# Table 5: Regression Table ----------------------------------------------------

# The main event study regression:
#   BHAR = b1*SUE + b2*SameSign + b3*SUE×SameSign + controls + FE
#
# We build up the specification across columns to show the contribution of
# each component: base ERC, interaction, fixed effects, controls.
# fixest::feols handles the fixed effects and clustering.
# fixef.rm = "singletons" removes singleton FE for comparability with Stata's reghdfe.

models <- list(
  "Base" = feols(bhar ~ sue,
                 regdata, fixef.rm = "singletons"),
  "Interaction" = feols(bhar ~ sue * same_sign,
                        regdata, fixef.rm = "singletons"),
  "Year FE" = feols(bhar ~ sue * same_sign | fyearq,
                     regdata, fixef.rm = "singletons"),
  "Two-Way FE" = feols(bhar ~ sue * same_sign | fyearq + ff12num,
                        regdata, fixef.rm = "singletons"),
  "Controls" = feols(bhar ~ sue * same_sign + sue * log_mve_std + sue * loss |
                       fyearq + ff12num,
                     regdata, fixef.rm = "singletons")
)


# Coefficient map: controls the order and labels of displayed coefficients.
# Coefficients not listed here are omitted from the table (e.g., controls).
# Note the LaTeX math notation for labels.
cm <- c(
  "sue"            = "$SUE$",
  "same_sign"      = "$SameSign$",
  "sue:same_sign"  = "$SUE \\times SameSign$"
)


# Custom N formatter with commas
nobs_fmt <- function(x) {
  out <- formattable::comma(x, digits = 0)
  paste0("\\multicolumn{1}{c}{", out, "}")
}

# Custom function to detect whether controls are in the model
# (adapted from modelsummary documentation)
glance_custom.fixest <- function(x, ...) {
  controls <- c("log_mve_std", "loss")
  if (all(controls %in% names(coef(x)))) {
    data.frame(Controls = "X")
  } else {
    data.frame(Controls = "")
  }
}

# Goodness-of-fit statistics to display below the coefficients
gm <- list(
  list("raw" = "FE: fyearq",  "clean" = "Year FE",      "fmt" = NULL),
  list("raw" = "FE: ff12num", "clean" = "Industry FE",   "fmt" = NULL),
  list("raw" = "Controls",    "clean" = "Controls",      "fmt" = NULL),
  list("raw" = "nobs",        "clean" = "N",             "fmt" = nobs_fmt),
  list("raw" = "r.squared",   "clean" = "$R^2$",         "fmt" = 3),
  list("raw" = "r2.within",   "clean" = "$R^2$ Within",  "fmt" = 3)
)


# Preview the table in the console
panel <- modelsummary(models,
                      # Cluster standard errors by permno and fiscal year-quarter
                      vcov = ~ permno + fyearq,
                      # t-statistics in parentheses below coefficients
                      statistic = "statistic",
                      # Significance stars
                      stars = c('*' = .1, '**' = .05, '***' = .01),
                      estimate = "{estimate}{stars}",
                      coef_map = cm,
                      gof_map = gm,
                      escape = FALSE,
                      booktabs = TRUE)

# Look at it
panel


# Save to LaTeX
modelsummary(models,
             vcov = ~ permno + fyearq,
             statistic = "statistic",
             stars = c('\\sym{*}' = .1, '\\sym{**}' = .05, '\\sym{***}' = .01),
             estimate = "{estimate}{stars}",
             coef_map = cm,
             gof_map = gm,
             output = glue("{output_dir}/regression-r.tex"),
             escape = FALSE)
