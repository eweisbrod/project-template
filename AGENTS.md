# AGENTS.md - AI Assistant Context for project-template

> **What is this file?** AGENTS.md is an emerging open standard for providing
> AI coding assistants with project context. It is supported by Claude Code,
> Cursor, Windsurf, GitHub Copilot, and other AI tools. Think of it as a
> README for AI -- it tells any AI assistant how your project is structured,
> what conventions to follow, and what pitfalls to avoid. See the
> [README section on AGENTS.md](#about-agentsmd-and-claudemd) for more details.

## Project Overview

This is a **polyglot project template** (Python + R + Stata) for
Accounting/Finance empirical research. It demonstrates an earnings
announcement event study: downloading data from WRDS (Compustat quarterly +
CRSP daily), merging databases, computing earnings surprises and abnormal
returns, producing publication-ready figures, and outputting formatted
tables to LaTeX, MS Word, and RTF.

Scripts 1-2 (download + transform) are **Python only**. Scripts 3-4
(figures + analysis) have **parallel implementations** in Python, R, and
Stata. All languages share the same data files (parquet for Python/R, .dta
for Stata) and the same `.env` configuration.

For a pure R version, see
[project-template-r](https://github.com/eweisbrod/project-template-r).

**This is a public teaching repository.** Code quality, readability, and
extensive comments matter more than efficiency. When making changes, preserve
the teaching style and add comments explaining *why*, not just *what*.

## Project Structure

```
project-template/
├── .env.example            # Template for user's .env
├── .gitignore              # Polyglot ignores (R + Python + Stata)
├── AGENTS.md               # This file
├── CLAUDE.md               # Claude Code config (imports AGENTS.md)
├── README.md               # Main documentation
├── pyproject.toml          # Python dependencies (managed by uv)
├── output/                 # Tables and figures (gitignored contents)
├── src/
│   ├── setup.py                      # One-time setup (creates .env, stores creds)
│   ├── utils.py                      # Python helpers (download_wrds, FF12)
│   ├── utils.R                       # R helpers (winsorize, FF industries, etc.)
│   │
│   ├── 1-download-data.py            # Download from WRDS (Python only)
│   ├── 2-transform-data.py           # Merge + variables + BHARs (Python only)
│   │
│   ├── 3-figures.py                  # Figures (plotnine)
│   ├── 3-figures.R                   # Figures (ggplot2)
│   │
│   ├── 4-analyze-data.py             # Tables (pyfixest, LaTeX)
│   ├── 4-analyze-data.R              # Tables (fixest/modelsummary, LaTeX + Word)
│   └── 4-analyze-data.do             # Tables (reghdfe/esttab, LaTeX + RTF)
└── LICENSE
```

### Script Execution Order

1. `1-download-data.py` (requires WRDS credentials, ~20 min)
2. `2-transform-data.py` (~2 min)
3. `3-figures.py` or `3-figures.R` (pick one)
4. `4-analyze-data.py` or `4-analyze-data.R` or `4-analyze-data.do` (pick one)

Run Python scripts with `uv run src/script.py`. R scripts with
`Rscript src/script.R`. Stata scripts with `do src/4-analyze-data.do`.

## Key Conventions

### Environment and Paths

- **All paths come from the `.env` file.** Never hardcode local paths.
- Run `uv run src/setup.py` to create `.env` and store WRDS credentials.
- The `.env` file uses **forward slashes** even on Windows:
  `DATA_DIR=D:/Dropbox/your-project-name`
- Two key variables: `DATA_DIR` (raw/processed data) and `OUTPUT_DIR`
  (tables and figures).
- Python: `load_dotenv(".env", override=True); data_dir = os.getenv("DATA_DIR")`
- R: `load_dot_env(".env"); data_dir <- Sys.getenv("DATA_DIR")`
- Stata: `doenv using ".env"; local data_dir "\`r(DATA_DIR)'"`

### Credentials

WRDS credentials are stored via the `keyring` package in the OS credential
store (Windows Credential Manager / macOS Keychain). Both Python and R
read the same keyring entries: service `"wrds"`, keys `"username"` and
`"password"`. Stata uses `.env` + `doenv` for paths but does not access
WRDS directly (it reads .dta files produced by Python).

### Python Code Style

- **uv** manages the virtual environment and dependencies (`pyproject.toml`)
- **polars** for all data manipulation (not pandas). Arrow-native, zero-copy
  with DuckDB, types don't corrupt on parquet round-trips.
- **DuckDB** for querying parquet files and SQL joins
- **psycopg2** for WRDS PostgreSQL connection (chunked server-side cursors)
- **plotnine** for figures (ggplot2 syntax in Python)
- **pyfixest** for regressions (fixest syntax: `"y ~ x | fe1 + fe2"`)
- `sys.stdout.reconfigure(encoding="utf-8")` at the top of each script
  (Windows terminal encoding fix)
- `load_dotenv(".env", override=True)` — the `override=True` is important
  so the `.env` file wins over any system-level environment variables.

### R Code Style

- **`pacman::p_load()`** auto-installs missing packages
- `tidyverse` loaded last to avoid conflicts
- Native pipe `|>` preferred over `%>%`
- `fixest::feols` for regressions (with `fixef.rm = "singletons"`)
- `modelsummary` for regression tables; `kableExtra` for LaTeX; `flextable`
  + `officer` for Word output

### Stata Code Style

- **`reghdfe`** for fixed effects regressions
- **`estout`/`esttab`** for table output (LaTeX + RTF)
- **`projectpaths`** + **`doenv`** for portable path management
- `estadd local` for indicator rows (Controls, FE)
- `estfe` for automatic FE indicator rows (reghdfe integration)
- Triple-slash `///` for line continuation

### Data and Variables

- **WRDS** (Wharton Research Data Services) is the data source
- `gvkey` = firm ID, `permno` = security ID, `rdq` = announcement date
- `sue` = (ibq - ibq_lag4) / mve_lag1 — seasonal earnings change scaled
  by prior-quarter market value (no per-share split adjustment needed)
- `same_sign` = 1 if earnings change and sales change have same sign
- `bhar` = buy-and-hold abnormal return ([-1, +1] trading days)
- `loss` = 1 if quarterly income (ibq) < 0
- `FF12` / `ff12num` = Fama-French 12 industry classification
- Winsorization at 1%/99% (see `winsorize_x` in utils.R, `winsorize` in
  `2-transform-data.py`)
- Financials (SIC 60-69) and utilities (SIC 49) excluded

### Output Files

Each language appends a suffix to output filenames:
- Python: `*-py.tex`
- R: `*-r.tex`, `tables-r.docx`
- Stata: `*-stata.tex`, `*-stata.rtf`, `sample-selection-stata.docx`

## Guidelines for AI Assistants

- Read files before editing. Preserve teaching comments.
- Each Python script starts with `load_dotenv(".env", override=True)`.
- Each R script starts with `load_dot_env(".env")` and `source("src/utils.R")`.
- Do not store WRDS credentials in variables — call `keyring` inline.
- When editing Python, use polars (not pandas) for data work.
- When editing Stata, follow the `projectpaths` + `doenv` pattern.

## Common Pitfalls

- **polars vs pandas**: This template uses polars. Don't add pandas imports
  for data work — only for the `.to_pandas()` bridge to pyfixest/`.to_stata()`.
- **DuckDB DECIMAL precision**: PostgreSQL NUMERIC columns can have varying
  precision across batches. The `_normalize_arrow_schema()` function in
  utils.py casts these to float64 before writing to parquet.
- **Trading day mapping**: Use `join_asof(strategy="forward")` (nearest
  trading day on or after rdq), not `"backward"`. Weekend rdqs should map
  to Monday, not Friday, to avoid overlapping event windows.
- **CRSP v2 column names**: `dsf_v2` uses `dlycaldt`/`dlyret` (not `date`/`ret`).
- **CCM link filtering**: Apply Gow & Ding filters at merge time, not download.
- **Forward slashes in .env**: Always use `/` not `\`, even on Windows.
- **`override=True` in `load_dotenv()`**: Without this, system-level env
  vars with the same name silently override the `.env` file.
