# 4-analyze-data.R
# ===========================================================================
# Regression analysis and table output for the earnings event study.
#
# This script produces all tables in BOTH LaTeX and Word formats from a
# single run. Models are fit once; each table is then exported twice:
#   - LaTeX .tex files (for Overleaf / pdflatex)
#   - A combined Word .docx file (via flextable + officer)
#
# Tables:
#   1. Sample selection (step-by-step obs counts from script 2)
#   2. Frequency table by decade (SameSign counts)
#   3. Descriptive statistics (modelsummary's datasummary)
#   4. Correlation matrix (Pearson above, Spearman below)
#   5. Regression table (SUE × SameSign interaction, FE, clustered SEs)
#
# HOW TO RUN:
#   Open in RStudio and run interactively, or:
#   Rscript src/4-analyze-data.R
# ===========================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, modelsummary, sjlabelled, kableExtra, tinytable,
               formattable, flextable, officer, lubridate, glue, arrow, fixest,
               tidyverse)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")

# Prevent modelsummary from adding extra formatting to LaTeX numbers
options(modelsummary_format_numeric_latex = "plain")


# Read in the data from the previous step --------------------------------------

regdata <- read_parquet(glue("{data_dir}/regdata.parquet")) |>
  # Standardize continuous controls for the interaction model. In a model with
  # SUE × control interactions, the SUE main effect is the ERC evaluated at
  # control = 0. Standardizing (mean 0, sd 1) makes that "at the sample mean."
  # standardize() is from utils.R: (x - mean) / sd. We use it instead of
  # base::scale() because scale() returns a matrix, not a vector.
  mutate(log_mve_std = standardize(log_mve)) |>
  sjlabelled::var_labels(
    bhar        = "$BHAR_{[-1,+1]}$",
    sue         = "$SUE$",
    same_sign   = "$SameSign$",
    loss        = "$LOSS$",
    log_mve     = "$\\ln(MVE)$",
    log_mve_std = "$\\ln(MVE)^{std}$"
  )

nrow(regdata)


# =============================================================================
# Table 1: Sample Selection
# =============================================================================

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

# --- LaTeX ---
kbl(sample_table, format = "latex", booktabs = TRUE,
    linesep = "", align = "clrr", escape = TRUE) |>
  save_kable(glue("{output_dir}/sample-selection-r.tex"))

# --- Word (flextable) ---
ft_sample <- flextable(sample_table) |>
  set_table_properties(layout = "autofit") |>
  fit_to_width(max_width = 6.5)


# =============================================================================
# Table 2: Observations by Decade
# =============================================================================

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

totalrow <- regdata |>
  summarize(`Firm-Quarters`     = formattable::comma(n(), digits = 0),
            `SameSign Quarters` = formattable::comma(sum(same_sign, na.rm = TRUE), digits = 0),
            `Pct. SameSign`     = formattable::percent(mean(same_sign, na.rm = TRUE), digits = 2)) |>
  mutate(Year = "Total")

table2 <- bind_rows(table2, totalrow)
table2

# --- LaTeX ---
kbl(table2, format = "latex", booktabs = TRUE, linesep = "") |>
  save_kable(glue("{output_dir}/freqtable-r.tex"))

# --- Word ---
ft_freq <- flextable(table2) |> autofit()


# =============================================================================
# Table 3: Descriptive Statistics
# =============================================================================

#' Default number formatter for descriptive-stat tables: 3-digit comma format.
#' Used as the `fmt` argument to `datasummary()` so all numeric stats
#' (mean, SD, min, max, etc.) print with the same precision and
#' thousands separators.
my_fmt <- function(x) formattable::comma(x, digits = 3)

#' Non-missing-count formatter for descriptive-stat tables.
#' Returns the count as an integer with thousands separators, formatted
#' to slot into a `datasummary()` `(N = NN)` column. Special-case: if
#' the column is a logical of all-NA (a `datasummary` quirk for
#' character columns), report the column length instead.
NN <- function(x) {
  out <- if (is.logical(x) && all(is.na(x))) length(x) else sum(!is.na(x))
  formattable::comma(out, digits = 0)
}

descripdata <- regdata |>
  select(bhar, sue, same_sign, loss, log_mve) |>
  label_to_colnames()

# --- LaTeX ---
datasummary(All(descripdata) ~ (N = NN) +
              (Mean + SD + Min + P25 + Median + P75 + Max) * Arguments(fmt = my_fmt),
            data = descripdata,
            escape = FALSE,
            output = glue("{output_dir}/descrip-r.tex"))

# --- Word ---
ft_descrip <- datasummary(All(descripdata) ~ (N = NN) +
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


# =============================================================================
# Table 4: Correlation Matrix
# =============================================================================

# --- LaTeX ---
datasummary_correlation(descripdata,
                        method = "pearspear",
                        output = glue("{output_dir}/corrtable-r.tex"),
                        escape = FALSE)

# --- Word ---
ft_corr <- datasummary_correlation(descripdata,
                                    method = "pearspear",
                                    output = "flextable") |>
  fit_to_width(max_width = 6.5)


# =============================================================================
# Table 5: Regression Table
# =============================================================================

# Fit all 5 model specifications (done once, used for both LaTeX and Word)
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

#' S3 method that adds a "Controls" indicator row to modelsummary tables.
#'
#' `modelsummary` calls `glance_custom()` on each model to gather extra
#' goodness-of-fit columns. Defining `glance_custom.fixest` (the
#' fixest-specific S3 method) lets us inject a custom row that says "X"
#' when the model's coefficients include our control set and is blank
#' otherwise. The rest of the gof formatting is configured via `gm`
#' below.
#'
#' @param x A fitted `fixest` model (typically from `feols()`).
#' @param ... Ignored; required for the S3 method signature.
#' @return One-row data frame with column `Controls`, value `"X"` or `""`.
glance_custom.fixest <- function(x, ...) {
  controls <- c("log_mve_std", "loss")
  if (all(controls %in% names(coef(x)))) {
    data.frame(Controls = "X")
  } else {
    data.frame(Controls = "")
  }
}

# Goodness-of-fit statistics
gm <- list(
  list("raw" = "FE: fyearq",  "clean" = "Year FE",      "fmt" = NULL),
  list("raw" = "FE: ff12num", "clean" = "Industry FE",   "fmt" = NULL),
  list("raw" = "Controls",    "clean" = "Controls",      "fmt" = NULL),
  list("raw" = "nobs",        "clean" = "N",             "fmt" = function(x) formattable::comma(x, digits = 0)),
  list("raw" = "r.squared",   "clean" = "$R^2$",         "fmt" = 3),
  list("raw" = "r2.within",   "clean" = "$R^2$ Within",  "fmt" = 3)
)

# --- LaTeX ---
# Coefficient map with LaTeX math notation
cm_latex <- c(
  "sue"            = "$SUE$",
  "same_sign"      = "$SameSign$",
  "sue:same_sign"  = "$SUE \\times SameSign$"
)

modelsummary(models,
             vcov = ~ permno + fyearq,
             statistic = "statistic",
             stars = c('\\sym{*}' = .1, '\\sym{**}' = .05, '\\sym{***}' = .01),
             estimate = "{estimate}{stars}",
             coef_map = cm_latex,
             gof_map = gm,
             output = glue("{output_dir}/regression-r.tex"),
             escape = FALSE)

# --- Word ---
# Coefficient map with plain text labels
cm_word <- c(
  "sue"            = "SUE",
  "same_sign"      = "SameSign",
  "sue:same_sign"  = "SUE x SameSign"
)

# Update gof_map: remove LaTeX math from R² labels for Word
gm_word <- gm
gm_word[[5]]$clean <- "R-squared"
gm_word[[6]]$clean <- "R-sq Within"

ft_reg <- modelsummary(models,
                       vcov = ~ permno + fyearq,
                       statistic = "statistic",
                       stars = c('*' = .1, '**' = .05, '***' = .01),
                       estimate = "{estimate}{stars}",
                       coef_map = cm_word,
                       gof_map = gm_word,
                       output = "flextable") |>
  fit_to_width(max_width = 6.5) |>
  autofit()

# Preview
ft_reg


# =============================================================================
# Assemble Word document
# =============================================================================

# The officer package builds Word documents programmatically: headings,
# paragraphs, tables (flextables), images, and page breaks.

read_docx() |>
  body_add_par("Sample Selection", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ft_sample) |>
  body_add_break(pos = "after") |>
  body_add_par("Sample Frequency", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ft_freq) |>
  body_add_break(pos = "after") |>
  body_add_par("Descriptive Statistics", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ft_descrip) |>
  body_add_break(pos = "after") |>
  body_add_par("Correlation Matrix", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ft_corr) |>
  body_add_break(pos = "after") |>
  body_add_par("Regression Table", style = "heading 1") |>
  body_add_par("") |>
  body_add_flextable(value = ft_reg) |>
  body_add_break(pos = "after") |>
  body_add_par("Figure: SameSign by Industry", style = "heading 1") |>
  body_add_par("") |>
  body_add_img(glue("{output_dir}/ff12_fig.png"),
               height = 3.6, width = 4.2, style = "centered") |>
  body_add_break(pos = "after") |>
  body_add_par("Figure: Event Study CAR", style = "heading 1") |>
  body_add_par("") |>
  body_add_img(glue("{output_dir}/car_fig.png"),
               height = 3.6, width = 4.2, style = "centered") |>
  print(target = glue("{output_dir}/tables-r.docx"))

cat("All tables saved to:", output_dir, "\n")
cat("  LaTeX: sample-selection-r.tex, freqtable-r.tex, descrip-r.tex,\n")
cat("         corrtable-r.tex, regression-r.tex\n")
cat("  Word:  tables-r.docx\n")
