# 4-analyze-data-and-tabulate-word.R
# ===========================================================================
# Regression analysis and Word/Office table output for the earnings event study.
#
# Note: Some formatting is only done in the LaTeX version. Review the LaTeX
# version as well for additional / advanced formatting options.
#
# This script creates the same tables as the LaTeX version but outputs them
# as flextables, then combines everything into a Word document using the
# officer package. Flextables can also be output to HTML, PowerPoint,
# markdown, etc. — see https://ardata-fr.github.io/flextable-book/
# ===========================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, modelsummary, sjlabelled, formattable, flextable,
               equatags, officer, lubridate, glue, arrow, fixest,
               tidyverse)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")


# Read in the data from the previous step --------------------------------------

regdata <- read_parquet(glue("{data_dir}/regdata.parquet")) |>
  # Standardize the continuous control (log_mve) so that in the SUE x control
  # interaction model the SUE main effect is the ERC at the sample mean of
  # the control, and so the interaction coefficients are on a per-standard-
  # deviation scale. Binary controls (loss, same_sign) are not standardized.
  # standardize() is a helper from utils.R — it just computes
  #   (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
  # We use it instead of base::scale() because scale() returns a 1-column
  # matrix rather than a plain numeric vector, which is awkward inside
  # mutate(). See the LaTeX version of this script for a longer write-up.
  mutate(log_mve_std = standardize(log_mve)) |>
  # Variable labels for Word tables (no LaTeX math notation needed here)
  sjlabelled::var_labels(
    bhar        = "BHAR_{[-1,+1]}",
    sue         = "SUE",
    same_sign   = "SameSign",
    loss        = "LOSS",
    log_mve     = "ln(MVE)",
    log_mve_std = "ln(MVE) std"
  )


# Table 1: Sample Selection ----------------------------------------------------

# Step-by-step observation counts produced by script 2.

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

ftable1 <- flextable(sample_table) |>
  set_table_properties(layout = "autofit") |>
  fit_to_width(max_width = 6.5)

ftable1


# Table 2: Observations by Decade ----------------------------------------------

# See the LaTeX version for detailed comments on each step.

t2 <- regdata |>
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

totalrow <- regdata |>
  summarize(`Firm-Quarters`     = formattable::comma(n(), digits = 0),
            `SameSign Quarters` = formattable::comma(sum(same_sign, na.rm = TRUE), digits = 0),
            `Pct. SameSign`     = formattable::percent(mean(same_sign, na.rm = TRUE), digits = 2)) |>
  mutate(Year = "Total")

t2 <- bind_rows(t2, totalrow)
t2

# Turn it into a flextable
ftable2 <- flextable(t2) |> autofit()
ftable2


# Table 3: Descriptive Statistics ----------------------------------------------

my_fmt <- function(x) formattable::comma(x, digits = 3)

NN <- function(x) {
  out <- if (is.logical(x) && all(is.na(x))) length(x) else sum(!is.na(x))
  formattable::comma(out, digits = 0)
}

descripdata <- regdata |>
  select(bhar, sue, same_sign, loss, log_mve) |>
  label_to_colnames()

# Output as flextable, with equation-formatted variable names in column 1
ftable3 <- datasummary(All(descripdata) ~ (N = NN) +
                          Mean * Arguments(fmt = my_fmt) +
                          SD * Arguments(fmt = my_fmt) +
                          Min * Arguments(fmt = my_fmt) +
                          P25 * Arguments(fmt = my_fmt) +
                          Median * Arguments(fmt = my_fmt) +
                          P75 * Arguments(fmt = my_fmt) +
                          Max * Arguments(fmt = my_fmt),
                        escape = FALSE,
                        output = 'flextable',
                        data = descripdata) |>
  fit_to_width(max_width = 6.5)

# TIP: To render variable names with subscripts/math in Word, you can use
# flextable's as_equation() with the equatags package. This requires
# equatags and xslt to be installed. Example:
#   ftable3 |> compose(j = 1,
#     value = as_paragraph(as_equation(` `, width = 1.5, height = 0.2)))

ftable3


# Table 4: Correlation Matrix --------------------------------------------------

datasummary_correlation(descripdata, method = "pearspear")

ftable4 <- datasummary_correlation(descripdata,
                                    method = "pearspear",
                                    output = "flextable") |>
  fit_to_width(max_width = 6.5)

ftable4


# Table 5: Regression Table ----------------------------------------------------

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

# Coefficient map — plain text labels for Word output.
# For LaTeX-style math in Word, see the equatags tip above.
cm <- c(
  "sue"            = "SUE",
  "same_sign"      = "SameSign",
  "sue:same_sign"  = "SUE x SameSign"
)

nobs_fmt <- function(x) formattable::comma(x, digits = 0)

# Custom function to detect controls
glance_custom.fixest <- function(x, ...) {
  controls <- c("log_mve_std", "loss")
  if (all(controls %in% names(coef(x)))) {
    data.frame(Controls = "X")
  } else {
    data.frame(Controls = "")
  }
}

gm <- list(
  list("raw" = "FE: fyearq",  "clean" = "Year FE",     "fmt" = NULL),
  list("raw" = "FE: ff12num", "clean" = "Industry FE",  "fmt" = NULL),
  list("raw" = "Controls",    "clean" = "Controls",     "fmt" = NULL),
  list("raw" = "nobs",        "clean" = "N",            "fmt" = nobs_fmt),
  list("raw" = "r.squared",   "clean" = "R-squared",    "fmt" = 3),
  list("raw" = "r2.within",   "clean" = "R-sq Within",  "fmt" = 3)
)

# Create flextable
ftable5 <- modelsummary(models,
                         vcov = ~ permno + fyearq,
                         statistic = "statistic",
                         stars = c('*' = .1, '**' = .05, '***' = .01),
                         estimate = "{estimate}{stars}",
                         coef_map = cm,
                         gof_map = gm,
                         output = "flextable") |>
  fit_to_width(max_width = 6.5) |>
  autofit()

ftable5


# Combine everything into a Word document --------------------------------------

# The officer package lets you build Word documents programmatically.
# You can add headings, paragraphs, tables (flextables), images, and page breaks.

read_docx() |>
  body_add_par("Sample Selection", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ftable1) |>
  body_add_break(pos = "after") |>
  body_add_par("Sample Frequency", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ftable2) |>
  body_add_break(pos = "after") |>
  body_add_par("Descriptive Statistics", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ftable3) |>
  body_add_break(pos = "after") |>
  body_add_par("Correlation Matrix", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ftable4) |>
  body_add_break(pos = "after") |>
  body_add_par("Regression Table", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ftable5) |>
  body_add_break(pos = "after") |>
  body_add_par("Figure: Losses by Industry", style = "heading 1") |>
  body_add_par("") |>
  body_add_img(glue("{output_dir}/ff12_fig.png"),
               height = 3.6, width = 4.2, style = "centered") |>
  body_add_break(pos = "after") |>
  body_add_par("Figure: Event Study CAR", style = "heading 1") |>
  body_add_par("") |>
  body_add_img(glue("{output_dir}/car_fig.png"),
               height = 3.6, width = 4.2, style = "centered") |>
  print(target = glue("{output_dir}/tables-r.docx"))

# Since these are flextables, they can also be output to html, ppt, markdown, etc.
# https://ardata-fr.github.io/flextable-book/
