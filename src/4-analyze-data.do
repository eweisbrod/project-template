/******************************************************************************
 * 4-analyze-data.do
 * Regression analysis and table output for the earnings event study.
 *
 * This script produces all tables in BOTH LaTeX (.tex) and Word (.rtf) formats
 * from a single run. It reads the .dta files created by Python script 2:
 *   - regdata.dta            (main analysis dataset)
 *   - sample-selection.dta   (step-by-step sample counts from script 2)
 *
 * Tables:
 *   1. Sample selection (LaTeX + RTF)
 *   2. Frequency table by decade (LaTeX + RTF)
 *   3. Descriptive statistics (LaTeX + RTF)
 *   4. Correlation matrix (LaTeX + RTF)
 *   5. Regression table with SUE x SameSign interaction (LaTeX + RTF)
 *
 * FIRST-TIME SETUP:
 *   Install required packages (uncomment and run once per computer):
 *     ssc install estout
 *     ssc install reghdfe
 *     ssc install projectpaths
 *     net install doenv, from("https://github.com/vikjam/doenv/raw/master/")
 *
 *   Register this project with projectpaths (replace path with YOUR clone path):
 *     project_paths_list, add project(project-template) path("C:/_git/project-template")
 *
 * HOW TO RUN:
 *   do src/4-analyze-data.do
 *
 * The same .env file works across R, Python, and Stata — setup.py creates it.
 ******************************************************************************/


* Setup ----------------------------------------------------------------------

// Navigate to the project root and load .env
project_paths_list, project(project-template) cd
doenv using ".env"
local data_dir   "`r(DATA_DIR)'"
local output_dir "`r(OUTPUT_DIR)'"

// Open a log so the run produces log/4-analyze-data.log alongside the
// .Rout / .log files from the R and Python halves of the pipeline. `cap`
// = capture; ignores the error if log/ already exists or another log
// session is open.
cap mkdir "log"
cap log close
log using "log/4-analyze-data.log", replace text

display "Using data directory: `data_dir'"
display "Using output directory: `output_dir'"


/******************************************************************************
 * Table 1: Sample Selection
 ******************************************************************************/

// The sample-selection.dta was saved by Python script 2 alongside the parquet
use "`data_dir'/sample-selection.dta" , clear

// Compute the (Diff) column as the drop from the previous step
gen diff = obs[_n-1] - obs

// Format obs and diff with commas; wrap diff in parentheses for display
gen str obs_fmt  = string(obs, "%12.0gc")
gen str diff_fmt = cond(missing(diff), "", "(" + string(diff, "%12.0gc") + ")")

// Build the table using Stata's `collect` framework (Stata 17+).
// `collect get` stores row-level values tagged by step; `collect layout`
// arranges them; `collect export` writes to .tex and .docx from the same
// stored collection.
collect clear
forvalues i = 1/`=_N' {
    local stp = step[`i']
    collect get description = description[`i'] ///
                obs         = obs_fmt[`i'] ///
                diff        = diff_fmt[`i'], ///
                tags(step[`stp'])
}
collect label levels result description "Description" obs "Obs" diff "(Diff)"
collect label dim step "Step"
collect layout (step) (result[description obs diff])

// --- LaTeX ---
collect export "`output_dir'/sample-selection-stata.tex", replace tableonly

// --- Word (docx) ---
collect export "`output_dir'/sample-selection-stata.docx", replace


/******************************************************************************
 * Load main analysis dataset
 ******************************************************************************/

use "`data_dir'/regdata.dta" , clear


* Generate variables --------------------------------------------------------

// Encode gvkey for fixed effects (gvkey is a character variable)
encode gvkey, generate(firm_fe)

// Standardize continuous controls for the interaction model.
// In a model with SUE x control interactions, the SUE main effect is the ERC
// at control = 0. Standardizing (mean 0, sd 1) makes that "at the sample mean."
// Binary controls (loss, same_sign) are NOT standardized.
egen log_mve_std = std(log_mve)

// Decade categorical variable for the frequency table
gen decade = 1 if fyearq >= 1970 & fyearq <= 1979
replace decade = 2 if fyearq >= 1980 & fyearq <= 1989
replace decade = 3 if fyearq >= 1990 & fyearq <= 1999
replace decade = 4 if fyearq >= 2000 & fyearq <= 2009
replace decade = 5 if fyearq >= 2010 & fyearq <= 2019
replace decade = 6 if fyearq >= 2020
label define decade_lbl 1 "1970 - 1979" 2 "1980 - 1989" 3 "1990 - 1999" ///
    4 "2000 - 2009" 5 "2010 - 2019" 6 "2020+"
label values decade decade_lbl


* Label variables -----------------------------------------------------------

label var bhar        "BHAR[-1,+1]"
label var sue         "SUE"
label var same_sign   "SameSign"
label var loss        "LOSS"
label var log_mve     "ln(MVE)"
label var log_mve_std "ln(MVE) std"

// Define controls as a global macro (keeps later commands short).
// We use # (interaction only) instead of ## (full factorial) because sue
// is already in the model from c.sue##i.same_sign. Using ## would add
// redundant sue main effects that Stata keeps as zero coefficients.
global controls log_mve_std c.sue#c.log_mve_std i.loss c.sue#i.loss


/******************************************************************************
 * Table 2: Observations by Decade (Firm-Quarters, SameSign counts)
 ******************************************************************************/

eststo clear
estpost tabulate decade same_sign

// --- Preview ---
esttab, cell(b(fmt(%12.0gc)) rowpct(fmt(2) par)) ///
    collabels("") unstack noobs nonumber nomtitle ///
    eqlabels(, lhs("Year"))

// --- LaTeX ---
esttab using "`output_dir'/freqtable-stata.tex", replace compress booktabs ///
    cell(b(fmt(%12.0gc)) rowpct(fmt(2) par)) ///
    collabels("") unstack noobs nonumber nomtitle ///
    eqlabels(, lhs("Year")) ///
    substitute(\_ _)

// --- Word (RTF) ---
esttab using "`output_dir'/freqtable-stata.rtf", replace ///
    cell(b(fmt(%12.0gc)) rowpct(fmt(2) par)) ///
    collabels("") unstack noobs nonumber nomtitle ///
    eqlabels(, lhs("Year"))


/******************************************************************************
 * Table 3: Descriptive Statistics
 ******************************************************************************/

eststo clear
estpost summarize bhar sue same_sign loss log_mve, detail

// NOTE: All cell statistics must be in ONE quoted string to produce a
// horizontal (single-row-per-variable) layout. Multiple quoted strings
// in cells() create stacked rows — that's an estout convention.
local dcells "count(fmt(%12.0gc)) mean(fmt(%9.3fc)) sd(fmt(%9.3fc)) min(fmt(%9.3fc)) p25(fmt(%9.3fc)) p50(fmt(%9.3fc)) p75(fmt(%9.3fc)) max(fmt(%9.3fc))"

// --- Preview ---
esttab ., replace noobs nonumbers label cells("`dcells'") compress

// --- LaTeX ---
esttab using "`output_dir'/descrip-stata.tex", replace compress booktabs ///
    cells("`dcells'") ///
    nomtitles nonumbers noobs label ///
    substitute(\_ _)

// --- Word (RTF) ---
esttab using "`output_dir'/descrip-stata.rtf", replace ///
    cells("`dcells'") compress ///
    nomtitles nonumbers noobs label


/******************************************************************************
 * Table 4: Correlation Matrix
 ******************************************************************************/

eststo clear
estpost correlate bhar sue same_sign loss log_mve, matrix

// --- LaTeX ---
esttab using "`output_dir'/corrtable-stata.tex", replace compress booktabs ///
    unstack not noobs nonumbers nomtitles ///
    cells("b(fmt(3))") ///
    substitute(\_ _)

// --- Word (RTF) ---
esttab using "`output_dir'/corrtable-stata.rtf", replace ///
    unstack not noobs nonumbers nomtitles ///
    cells("b(fmt(3))")


/******************************************************************************
 * Table 5: Regression Table
 ******************************************************************************/

// 5 specifications building up from base ERC to full interaction model
// reghdfe handles high-dimensional fixed effects; vce(cluster ...) for two-way
// clustering by permno and fyearq.

eststo clear
eststo m1, title("Base"): ///
    reghdfe bhar sue, vce(cluster permno fyearq) noabsorb
eststo m2, title("Interaction"): ///
    reghdfe bhar c.sue##i.same_sign, vce(cluster permno fyearq) noabsorb
eststo m3, title("Year FE"): ///
    reghdfe bhar c.sue##i.same_sign, vce(cluster permno fyearq) absorb(fyearq)
eststo m4, title("Two-Way FE"): ///
    reghdfe bhar c.sue##i.same_sign, vce(cluster permno fyearq) absorb(fyearq ff12num)
eststo m5, title("Controls"): ///
    reghdfe bhar c.sue##i.same_sign $controls, vce(cluster permno fyearq) absorb(fyearq ff12num)

// Add a Controls indicator row via estadd (estadd is simpler than indicate()
// for compound coefficient names from interaction operators).
estadd local controls "":    m1 m2 m3 m4
estadd local controls "Yes": m5

// estfe adds FE indicator rows to stored estimates. It reads reghdfe's
// absorbed FE metadata and generates the indicate() string automatically.
// We also label _cons as "Constant" so it shows which models estimate one.
// Must rerun estfe before each esttab call — it stores results in r().
// http://scorreia.com/software/reghdfe/faq.html

// Drop baseline categories (0*), constant, and control coefficients.
// Only sue, 1.same_sign, and 1.same_sign#c.sue remain displayed.
local droplist 0* log_mve_std c.sue#c.log_mve_std 1.loss 1.loss#c.sue _cons

// --- Preview ---
estfe . m*, labels(fyearq "Year FE" ff12num "Industry FE" _cons "Constant")
esttab, ///
    drop(`droplist') ///
    mtitles label ///
    title("Regression of BHAR on SUE x SameSign") ///
    varlabels(1.same_sign "SameSign" 1.same_sign#c.sue "SUE x SameSign") ///
    indicate(`r(indicate_fe)') ///
    b(3) t(2) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    stats(controls N r2 r2_within, fmt(%s %12.0gc 3 3) ///
          labels("Controls" "N" "R-squared" "R-sq Within"))

// --- LaTeX ---
estfe . m*, labels(fyearq "Year FE" ff12num "Industry FE" _cons "Constant")
esttab using "`output_dir'/regression-stata.tex", replace compress booktabs ///
    substitute(\_ _) ///
    drop(`droplist') ///
    mtitles label nolegend nonotes ///
    title("Regression of BHAR on SUE x SameSign") ///
    varlabels(1.same_sign "\$SameSign\$" ///
              1.same_sign#c.sue "\$SUE \times SameSign\$") ///
    indicate(`r(indicate_fe)') ///
    b(3) t(2) ///
    star(\$^{*}\$ 0.10 \$^{**}\$ 0.05 \$^{***}\$ 0.01) ///
    stats(controls N r2 r2_within, fmt(%s %12.0gc 3 3) ///
          labels("Controls" "N" "\$R^2\$" "\$R^2\$ Within"))

// --- Word (RTF) ---
estfe . m*, labels(fyearq "Year FE" ff12num "Industry FE" _cons "Constant")
esttab using "`output_dir'/regression-stata.rtf", replace ///
    drop(`droplist') ///
    mtitles label nolegend nonotes ///
    title("Regression of BHAR on SUE x SameSign") ///
    varlabels(1.same_sign "SameSign" 1.same_sign#c.sue "SUE x SameSign") ///
    indicate(`r(indicate_fe)') ///
    b(3) t(2) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    stats(controls N r2 r2_within, fmt(%s %12.0gc 3 3) ///
          labels("Controls" "N" "R-squared" "R-sq Within"))


display "All tables saved to: `output_dir'"
display "  LaTeX: sample-selection-stata.tex, freqtable-stata.tex,"
display "         descrip-stata.tex, corrtable-stata.tex, regression-stata.tex"
display "  Word:  matching .rtf files"

log close
