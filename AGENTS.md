# AGENTS.md - AI Assistant Context for project-template-r

> **What is this file?** AGENTS.md is an emerging open standard for providing
> AI coding assistants with project context. It is supported by Claude Code,
> Cursor, Windsurf, GitHub Copilot, and other AI tools. Think of it as a
> README for AI -- it tells any AI assistant how your project is structured,
> what conventions to follow, and what pitfalls to avoid. See the
> [README section on AGENTS.md](#about-agentsmd-and-claudemd) for more details.

## Project Overview

This is an **R project template** for Accounting/Finance empirical research.
It demonstrates an earnings announcement event study: downloading data from
WRDS (Compustat quarterly + CRSP daily), merging databases, computing
earnings surprises and abnormal returns, producing publication-ready figures,
and outputting formatted tables to LaTeX and MS Word.

**This is a public teaching repository.** Code quality, readability, and
extensive comments matter more than efficiency. When making changes, preserve
the teaching style and add comments explaining *why*, not just *what*.

## Project Structure

```
project-template-r/
├── .gitignore              # R and project-specific ignores
├── AGENTS.md               # This file - AI assistant context
├── CLAUDE.md               # Claude Code config (imports AGENTS.md)
├── README.md               # Main documentation
├── output/                 # Tables and figures (gitignored contents)
├── src/
│   ├── setup.R                       # One-time project setup (creates .env)
│   ├── utils.R                       # Helper functions (winsorize, FF industries,
│   │                                 #   download_wrds, trading_day_window)
│   ├── 1-download-data.R             # Download from WRDS to parquet files
│   ├── 2-transform-data.R            # Merge, create variables, compute BHARs
│   ├── 3-figures.R                   # Publication-ready figures
│   ├── 4-analyze-data-and-tabulate-latex.R   # Tables for LaTeX output
│   ├── 4-analyze-data-and-tabulate-word.R    # Tables for Word output
│   └── run-all.R                             # Master script with logging
└── LICENSE
```

### Script Execution Order

Scripts are numbered and should be run in order:
1. `1-download-data.R` (requires WRDS credentials)
2. `2-transform-data.R`
3. `3-figures.R`
4. `4-analyze-data-and-tabulate-latex.R` or `4-analyze-data-and-tabulate-word.R`

Every R script loads `.env` via `dotenv` and sources `utils.R` at the top.
Packages are auto-installed via `pacman::p_load()` -- no separate install step needed.
See `1-download-data.R` for detailed comments on `.env` setup and keyring
credential storage.

## Key Conventions

### Environment and Paths

- **All paths come from the `.env` file** via the `dotenv` R package.
  Never hardcode local paths in scripts.
- Run `src/setup.R` to create `.env`, or create it manually.
- The `.env` file uses **forward slashes** even on Windows:
  `DATA_DIR=D:/Dropbox/your-project-name`
- Two key variables: `DATA_DIR` (raw/processed data) and `OUTPUT_DIR`
  (tables and figures, defaults to `output/`)
- R scripts load paths with: `load_dot_env(".env");
  data_dir <- Sys.getenv("DATA_DIR"); output_dir <- Sys.getenv("OUTPUT_DIR")`

### R Code Style

- **`pacman::p_load()` is used instead of `library()`** -- it auto-installs
  missing packages, so users don't need a separate install step. Each script
  starts with `if (!require("pacman")) install.packages("pacman")` followed
  by `pacman::p_load(...)`.
- `tidyverse` is always loaded last to avoid package conflicts
- The native pipe `|>` is preferred over `%>%`
- `glue("{data_dir}/filename")` is used for dynamic file paths
- Variable labels use LaTeX math notation (e.g., `$SUE$`, `$BHAR_{[-1,+1]}$`)
- The `formattable` package handles number formatting in tables
- `modelsummary` is used for regression tables; `kableExtra` for LaTeX;
  `flextable` + `officer` for Word output
- `fixest::feols` for fixed effects regressions (with `fixef.rm = "singletons"`)

### Data and Variables

- **WRDS** (Wharton Research Data Services) is the data source
- **Compustat fundq**: `gvkey` (firm ID), `datadate` (fiscal quarter end),
  `rdq` (earnings announcement date), `epspiq` (EPS), `saleq` (sales),
  `ajexq` (split adjustment factor), `prccq` (quarter-end price)
- **CRSP dsf_v2**: `permno` (security ID), `dlycaldt` (date), `dlyret` (return)
- **CCM link**: maps `gvkey` to `permno` via date-range matching
- **CRSP stocknames_v2**: SIC codes by permno with date ranges
- `sue` = standardized unexpected earnings (seasonal random walk, price-scaled,
  split-adjusted using `ajexq`)
- `same_sign` = 1 if earnings change and sales change have the same sign
- `bhar` = buy-and-hold abnormal return (stock return minus VW market return)
- `loss` = binary indicator (1 if quarterly income < 0)
- `FF12` = Fama-French 12 industry classification
- Winsorization at 1%/99% is the default (see `winsorize_x` in utils.R)
- Financial firms (SIC 60-69) and utilities (SIC 49) are excluded

### Download Methods

Script 1 demonstrates two WRDS download methods:
- **`collect()`**: simplest, loads entire result into R memory. Fine for small tables.
- **Chunked `ParquetFileWriter`**: streams rows in batches via server-side cursor.
  Peak RAM proportional to batch size, not table size. Used for large tables.
- **`download_wrds()` function** (utils.R): wraps the chunked method with
  auto batch sizing based on `max_ram_mb` target (default 8 GB).

### Merge Methods

Script 2 demonstrates multiple merge approaches:
- **dbplyr over DuckDB**: dplyr syntax executed by DuckDB on parquet files.
  Data only enters R memory on `collect()`. Used for CCM + stocknames merge.
- **DuckDB SQL**: raw SQL with `read_parquet()` for joining large parquet files.
  Used for CRSP returns merge (~8M events against ~100M row CRSP daily).
- **dplyr in R**: standard in-memory joins for small datasets.

### Output

- LaTeX output goes to `{output_dir}/*.tex`
- Word output goes to `{output_dir}/*.docx`
- Figures go to `{output_dir}/*.pdf` (LaTeX) or `*.png` (Word)
- The LaTeX template on Overleaf reads the `.tex` files directly

## Guidelines for AI Assistants

- When editing R scripts, always read the file first to understand context.
- Preserve extensive teaching comments -- this is a pedagogical repository.
- Each R script loads `.env` and sets `data_dir` and `output_dir` at the top via `dotenv`.
- Do not create new files unless necessary. Prefer editing existing files.
- Do not remove comments or teaching notes from the code.
- Do not store WRDS credentials in variables -- feed `keyring::key_get()` directly
  into function calls.

## Common Pitfalls

- **dbplyr vs dplyr**: In dbplyr contexts (before `collect()`), code runs on
  the database server. Use `is.na()` not `is.null()` for NULL checks. Some R
  functions need `sql()` wrappers.
- **Stock split adjustment**: When comparing EPS across quarters, use `ajexq`
  ratios to put values on a common share basis. See script 2 for details.
- **CRSP v2 column names**: `dsf_v2` uses `dlycaldt`/`dlyret` (not `date`/`ret`).
  Market returns are in `inddlyseriesdata` with `indno = 1000200` (not `dsi`/`vwretd`).
- **CCM link filtering**: Apply Gow & Ding filters (`linktype`, `linkprim`) at
  merge time, not download time, so the raw parquet stays reusable.
- **Duplicate announcements**: Some firms report multiple quarters on the same
  `rdq`. Dedup by keeping the most recent `datadate` per `permno + rdq`.
- **FF49 industry codes**: The upper bound for "Restaurants, Hotels, Motels"
  is SIC 7996, and for "Almost Nothing" is SIC 3999. Check `utils.R`.
- **Forward slashes in .env**: Always use `/` not `\` in paths, even on Windows.
- **fixest API**: Use `fixef.rm = "singletons"` (not the old `"both"`).
