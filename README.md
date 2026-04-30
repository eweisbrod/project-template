# Your Project Name

> **This is a template.** Click "Use this template" on GitHub to create your
> own project, then customize this README for your research.

## About This Template

This is a **swiss-army-knife project template** for empirical
Accounting/Finance research using data from WRDS. The repository ships with
a complete pipeline implemented in **R**, **Python**, and **Stata** in
parallel, with parquet files as the common interchange format between
languages. At setup time you pick which language(s) you want and the
template prunes the irrelevant files for you.

The pipeline (in each chosen language):

1. **Download** raw data from WRDS to parquet files
2. **Transform** — merge databases, create variables, compute BHARs
3. **Figures** — publication-ready plots
4. **Analyze** — regression and descriptive tables (LaTeX, Word, RTF)
5. **Data provenance** — sample-identifiers + SHA256 hashes for raw and
   derived files (the JAR Data and Code Sharing Policy artifacts)

The included example is an **earnings announcement event study**: it
downloads quarterly fundamentals and daily stock returns, computes
standardized unexpected earnings (SUE), and tests whether the market
reacts more strongly to earnings changes when sales move in the same
direction (SUE x SameSign interaction).

For more context on the pedagogical design, see the
[parent project](https://github.com/eweisbrod/example-project).

### Language combinations

When `project_setup()` runs the first time, you'll be asked which
combination you want. Pick from:

| # | Combination | Use case |
|---|---|---|
| 1 | Full R | Single-language R project |
| 2 | Full Python | Single-language Python project |
| 3 | Python + R | Parallel figures / tables in both languages |
| 4 | Python + Stata | Python pipeline, Stata tables |
| 5 | R + Stata | R pipeline, Stata tables |
| 6 | All three | Demo / cross-language comparison (default) |

`project_setup()` in `utils.py` only offers Python-inclusive combos (2,
3, 4, 6); the R version in `utils.R` only offers R-inclusive combos (1,
3, 5, 6). Run `1-download-data.{R,py}` in whichever language you intend
to keep — that determines which setup function fires and which combo
options are available.

After you confirm, the prompt offers to delete the files for the
languages you didn't pick. You can decline if you want to keep them
around for reference; otherwise your repo ends up containing exactly the
scripts you'll use, in exactly the language(s) you chose.

## Quick Start

1. Click **"Use this template"** on GitHub to create your own repo, then
   clone it.
2. Open `src/1-download-data.R` in RStudio (or `src/1-download-data.py`
   in VS Code) and run it interactively. The first call to
   `project_setup()` at the top of the file will walk you through
   first-time setup: pick a language combination, enter your data and
   output directories, and store your WRDS credentials in your OS
   keyring. Setup is finished once `.env` is on disk; from then on the
   `project_setup()` call is an instant no-op on every subsequent run.
3. Run the rest of the pipeline either step-by-step or via run-all:
   - Python: `uv run src/run-all.py`
   - R: open `src/run-all.R` in RStudio and run it (Ctrl+A, Ctrl+Enter)

## Project Structure

```
your-project-name/
├── .example-env            # Template for user's .env (copy and edit)
├── .gitignore              # Polyglot ignores (R + Python + Stata)
├── AGENTS.md               # AI assistant context (see below)
├── CLAUDE.md               # Claude Code config
├── README.md               # This file
├── pyproject.toml           # Python dependencies (managed by uv)
├── output/                 # Tables and figures (gitignored contents)
├── src/
│   ├── setup.py                      # One-time setup (creates .env, stores creds)
│   ├── utils.py                      # Python helpers (download_wrds, FF12 codes)
│   ├── utils.R                       # R helpers (winsorize, FF industries, etc.)
│   │
│   ├── 1-download-data.py            # Download from WRDS to parquet (Python)
│   ├── 2-transform-data.py           # Merge, create variables, BHARs (Python)
│   │
│   ├── 3-figures.py                  # Figures via plotnine (Python)
│   ├── 3-figures.R                   # Figures via ggplot2 (R)
│   │
│   ├── 4-analyze-data.py             # Tables via pyfixest (Python, LaTeX)
│   ├── 4-analyze-data.R              # Tables via fixest/modelsummary (R, LaTeX + Word)
│   └── 4-analyze-data.do             # Tables via reghdfe/esttab (Stata, LaTeX + RTF)
└── LICENSE
```

### Language Roles

| Script | Python | R | Stata |
|--------|--------|---|-------|
| 1. Download | `1-download-data.py` | — | — |
| 2. Transform | `2-transform-data.py` | — | — |
| 3. Figures | `3-figures.py` | `3-figures.R` | — |
| 4. Analysis | `4-analyze-data.py` | `4-analyze-data.R` | `4-analyze-data.do` |

Scripts 1-2 are Python only — they download from WRDS and save parquet files
(plus `.dta` for Stata). Scripts 3-4 have parallel implementations: pick
whichever language you prefer. All read the same data files.

## Prerequisites

- **Python** (>= 3.12) — [python.org](https://www.python.org/downloads/)
- **uv** — [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/)
- **WRDS account** — [wrds-www.wharton.upenn.edu](https://wrds-www.wharton.upenn.edu/)

Optional (for R or Stata scripts):
- **R** (>= 4.0) and **RStudio** — [posit.co](https://posit.co/download/rstudio-desktop/)
- **Stata** (>= 17, for the `collect` framework used in the bundled
  `4-analyze-data.do`) with `reghdfe`, `estout`, `projectpaths`, and
  `doenv` packages installed. Stata does not add itself to PATH on
  install; if `stata` doesn't run from a fresh shell, either add the
  Stata bin directory to PATH or set `STATA_BIN=path/to/stata.exe` in
  `.env` so `batch_run_stata()` can find it. Default install paths
  (`C:/Program Files/Stata*/Stata*-64.exe` on Windows, the standard
  Mac and Linux locations) are auto-detected.

## Configuration

### Setting up `.env`

Run `uv run src/setup.py` to create your `.env` file interactively. Or
copy `.example-env` to `.env` and edit manually:

```
RAW_DATA_DIR=D:/Dropbox/your-project-name/raw
DATA_DIR=D:/Dropbox/your-project-name/derived
OUTPUT_DIR=output
```

- **`RAW_DATA_DIR`** — raw WRDS pulls (CCM link, CRSP stocknames, Compustat
  fundq, CRSP daily returns, market index). These files are large and slow
  to refresh, so the download script (`1-download-data.py`) skips any file
  that already exists. Delete a file to force a re-pull. This split makes
  re-runs and replications cheap and reproducible — a downstream user can
  rerun scripts 2-4 against the original analyst's preserved raw inputs
  without hitting WRDS.
- **`DATA_DIR`** — derived files produced by scripts 2-4
  (`regdata.parquet`, `regdata.dta`, `figure-data.parquet`,
  `trading-dates.parquet`, `sample-selection.*`, `sample-identifiers.*`).
  Safe to delete and regenerate.
- **`OUTPUT_DIR`** — tables and figures, defaults to `output/`.
- Use **forward slashes** (`/`) even on Windows.
- `.env` is gitignored so each collaborator has their own copy.
- `.env` files are strict KEY=VALUE — no `#` comments or blank-line
  headers (env parsing tooling chokes on them).

### WRDS Credentials

WRDS credentials are stored securely using the `keyring` package (not in
code or `.env`). Both Python and R read from the same keyring entries
(`service="wrds"`, keys `"username"` and `"password"`), so you only need
to set them once via `setup.py`.

### Stata Setup

Stata requires a one-time registration of the project directory:

```stata
ssc install estout
ssc install reghdfe
ssc install projectpaths
net install doenv, from("https://github.com/vikjam/doenv/raw/master/")
project_paths_list, add project(project-template) path("C:/_git/project-template")
```

## Logging and Reproducibility

Every numbered pipeline script writes a per-script log to `log/` in the
project root. The file format and the underlying mechanism vary by
language, but the visual shape (command echoed, output interleaved,
plain text) is consistent across all four:

| Language | Mechanism | Output file |
|---|---|---|
| R | `batch_run()` → `R CMD BATCH` | `log/<script>.Rout` |
| Python | `batch_run()` → `run_with_echo.py` | `log/<script>.log` |
| Stata | `log using` at top of .do file + `batch_run_stata()` from R/Python | `log/<script>-stata.log` |
| SAS | `batch_run_sas()` from R/Python | `log/<script>-sas.log` |

The `-stata` / `-sas` suffixes avoid collisions with same-stem Python
`.log` files in mixed-language combos.

A pipeline run writes one log per numbered step:

- `1-download-data.{Rout,log}`
- `2-transform-data.{Rout,log}`
- `3-figures.{Rout,log}`
- `4-analyze-data.{Rout,log}`
- `5-data-provenance.{Rout,log}`

Step 5 also exports `sample-identifiers.{parquet,csv}` (gvkey, permno,
rdq, datadate, fyearq, fqtr) into `DATA_DIR` and prints an inventory of
every file in `RAW_DATA_DIR`, `DATA_DIR`, and `OUTPUT_DIR` with mtime,
size, and SHA256 hash.

Implementation: `batch_run()` lives in both `utils.py` and `utils.R`.
The Python version shells out to `run_with_echo.py`, a ~30-line
AST-walking wrapper that prints each top-level statement before
`exec()`-ing it. The R version calls `R CMD BATCH` natively. Stata's
`log using` is built into the .do file's preamble. SAS isn't currently
in this template's pipeline (no `*-data.sas` files) but the same pattern
applies if you add one.

The intent is to satisfy the Journal of Accounting Research Data and
Code Sharing Policy, which expects (i) the code that converts raw data
into the final dataset, (ii) a comprehensive log file documenting
end-to-end execution, and (iii) identifiers of the final-sample
observations.

`batch_run()` is also useful for one-off pulls: write a small
`pulls/<date>-<topic>.py`, then call
`batch_run("pulls/<date>-<topic>.py")` from a Python shell. The sibling
`.log` lands next to the script.

## Output

Output goes to the directory specified by `OUTPUT_DIR` in your `.env` file.

- **LaTeX**: `*-r.tex`, `*-py.tex`, `*-stata.tex` files for Overleaf
- **Word**: `tables-r.docx` (from R) and `*-stata.rtf` (from Stata)
- **Figures**: `*.pdf` (LaTeX) and `*.png` (Word/PowerPoint)

## About AGENTS.md and CLAUDE.md

- **`AGENTS.md`** is part of an emerging open standard for giving AI coding
  assistants context about a project. Tools like GitHub Copilot, Cursor, and
  Windsurf read it automatically.
- **`CLAUDE.md`** tells Claude Code to read `AGENTS.md`.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE)
for details.
