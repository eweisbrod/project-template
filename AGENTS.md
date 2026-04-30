# AGENTS.md - AI Assistant Context for project-template

> **What is this file?** AGENTS.md is an emerging open standard for providing
> AI coding assistants with project context. It is supported by Claude Code,
> Cursor, Windsurf, GitHub Copilot, and other AI tools. Think of it as a
> README for AI -- it tells any AI assistant how your project is structured,
> what conventions to follow, and what pitfalls to avoid. See the
> [README section on AGENTS.md](#about-agentsmd-and-claudemd) for more details.

## Project Overview

This is a **swiss-army-knife project template** for Accounting/Finance
empirical research. The repository ships with full R, full Python, and
Stata implementations of every numbered pipeline step. The first time
`1-download-data.R` or `1-download-data.py` is run, `project_setup()`
(defined in the corresponding `utils.{R,py}`) prompts the user for a
language combination, paths, and WRDS credentials, then writes `.env`
and (optionally) prunes the files for the languages that weren't
picked. Six combos are supported: Full R, Full Python, Python + R,
Python + Stata, R + Stata, and All three (demo mode). The R
`project_setup()` only offers R-inclusive combos and the Python one
only offers Python-inclusive, so neither version ever deletes itself
or the file that called it.

The `.env` file's existence is the "setup is done" flag —
`project_setup()` short-circuits to a no-op on every subsequent run.
There is no separate setup script to run; setup is just a function call
at the top of script 1.

The example pipeline is an earnings announcement event study: download
data from WRDS (Compustat quarterly + CRSP daily), merge databases,
compute earnings surprises and abnormal returns, produce publication-
ready figures, and output formatted tables to LaTeX, MS Word, and RTF.

Numbered scripts (1–5) exist in **all three languages in parallel**:
`1-download-data.{R,py}`, `2-transform-data.{R,py}`, `3-figures.{R,py}`,
`4-analyze-data.{R,py,do}`, `5-data-provenance.{R,py}`. Languages share
the same data layer (parquet for Python/R, .dta for Stata) and the same
`.env` configuration.

**This is a public teaching repository.** Code quality, readability, and
extensive comments matter more than efficiency. When making changes,
preserve the teaching style and add comments explaining *why*, not just
*what*.

## Project Structure

```
project-template/
├── .example-env            # Template for user's .env (copy to .env, fill in)
├── .gitignore              # Polyglot ignores (R + Python + Stata)
├── AGENTS.md               # This file
├── CLAUDE.md               # Claude Code config (imports AGENTS.md)
├── README.md               # Main documentation
├── pyproject.toml          # Python dependencies (managed by uv)
├── output/                 # Tables and figures (gitignored contents)
├── src/
│   ├── utils.py                      # Python helpers (download_parquet, batch_run, project_setup, FF12)
│   ├── utils.R                       # R helpers (winsorize, FF industries, batch_run, project_setup)
│   ├── run_with_echo.py              # AST-echo wrapper batch_run() shells out to
│   │
│   ├── 1-download-data.py            # Download from WRDS (Python)
│   ├── 1-download-data.R             # Download from WRDS (R)
│   ├── 2-transform-data.py           # Merge + variables + BHARs (Python)
│   ├── 2-transform-data.R            # Merge + variables + BHARs (R)
│   │
│   ├── 3-figures.py                  # Figures (plotnine)
│   ├── 3-figures.R                   # Figures (ggplot2)
│   │
│   ├── 4-analyze-data.py             # Tables (pyfixest, LaTeX)
│   ├── 4-analyze-data.R              # Tables (fixest/modelsummary, LaTeX + Word)
│   ├── 4-analyze-data.do             # Tables (reghdfe/esttab, LaTeX + RTF)
│   ├── 5-data-provenance.py          # Sample IDs + file SHA256 (Python)
│   ├── 5-data-provenance.R           # Sample IDs + file SHA256 (R)
│   ├── run-all.py                    # Master: batch_run() each script (Python)
│   └── run-all.R                     # Master: batch_run() each script (R)
└── LICENSE
```

After running `setup.{py,R}` and choosing a language combo, the file list
above will be pruned to whichever languages were kept.

### Script Execution Order

Scripts are numbered and should be run in order. Each numbered step has
parallel implementations in the chosen languages — pick whichever your
combo kept (or run `run-all.{py,R}` to do all five steps in sequence).

1. `1-download-data.{py,R}` (requires WRDS credentials, ~20 min)
2. `2-transform-data.{py,R}` (~2 min)
3. `3-figures.{py,R}`
4. `4-analyze-data.{py,R,do}`
5. `5-data-provenance.{py,R}` (sample-identifiers + file hashes)

Run Python scripts with `uv run src/script.py`. R scripts with
`Rscript src/script.R`. Stata scripts with `do src/4-analyze-data.do`.

## Key Conventions

### Environment and Paths

- **All paths come from the `.env` file.** Never hardcode local paths.
- Run `uv run src/setup.py` to create `.env` (or copy `.example-env` and
  edit by hand) and to store WRDS credentials in the OS keyring.
- The `.env` file uses **forward slashes** even on Windows.
- Three key variables:
  - `RAW_DATA_DIR` -- raw WRDS pulls (CCM link, CRSP stocknames, Compustat
    fundq, CRSP daily, market index). Treated as read-only inputs. Script 1
    skips any file that already exists, so re-runs and replications don't
    re-pull. Delete a file to refresh.
  - `DATA_DIR` -- derived parquets / .dta produced by scripts 2-4
    (`regdata.parquet`, `figure-data.parquet`, `trading-dates.parquet`,
    `sample-selection.parquet`, `sample-identifiers.*`). Replication runs
    typically wipe this folder and rerun scripts 2-4 against the preserved
    `RAW_DATA_DIR`.
  - `OUTPUT_DIR` -- tables and figures, defaults to `output/`.
- Python: `load_dotenv(".env", override=True); raw_data_dir = os.getenv("RAW_DATA_DIR"); data_dir = os.getenv("DATA_DIR")`
- R: `load_dot_env(".env"); raw_data_dir <- Sys.getenv("RAW_DATA_DIR"); data_dir <- Sys.getenv("DATA_DIR")`
- Stata: `doenv using ".env"; local raw_data_dir "\`r(RAW_DATA_DIR)'"; local data_dir "\`r(DATA_DIR)'"`
- `.env` files are strict KEY=VALUE -- no `#` comments, no headers (env
  parsing tooling chokes on them).

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

### Per-script logs across all four languages

Every numbered pipeline script writes a per-script log to `log/` in the
project root, regardless of which language it's implemented in. The
file format and the underlying mechanism are different in each
language, but the visual shape (command echoed, output interleaved,
plain text) is consistent so a reviewer comfortable with one can read
all four:

| Language | Mechanism | Output file |
|---|---|---|
| R | `batch_run()` calls `R CMD BATCH` (utils.R) | `log/<script>.Rout` |
| Python | `batch_run()` calls `run_with_echo.py` (utils.py) | `log/<script>.log` |
| Stata | `log using "log/<script>.log"` at top of .do file | `log/<script>.log` |
| SAS | Built-in `proc printto`, or `sas -log log/<script>.log` from CLI | `log/<script>.log` |

R's `batch_run()` and Python's `batch_run()` both spawn a fresh child
process so the log captures a clean session (no leakage from the
parent). Stata's `log using` runs in the same Stata session — fine
because Stata's natural workflow is one .do file per session anyway.
SAS isn't currently in the polyglot template's pipeline (no
`*-data.sas` files), but the same pattern would apply if added.

### Logging via batch_run (run_with_echo)

`run-all.py` runs each numbered script through `batch_run()` (defined in
`utils.py`), which spawns a fresh Python subprocess via `run_with_echo.py`
and writes a sibling `.log` file. Each `.log` contains:

- "Started: ..." timestamp at the top.
- Every top-level statement of the script echoed with `>>> ` / `... `
  continuations (REPL-style).
- `print()` output and warnings interleaved with the statements -- the
  same SAS-log shape produced by R CMD BATCH for R, and by SAS / Stata's
  native log behavior.

`run_with_echo.py` is a ~30-line wrapper that uses `ast.parse()` to walk
top-level statements in the target script, prints each one with the REPL
prefix, then `exec()`s it in a shared namespace. Comments and blank lines
are preserved verbatim. The wrapper writes to stdout; `batch_run()`
redirects that stdout to the .log file.

Each pipeline run drops five `.log` files into `log/`:
`1-download-data.log`, `2-transform-data.log`, `3-figures.log`,
`4-analyze-data.log`, and `5-data-provenance.log`. Together with the
exported `sample-identifiers.{parquet,csv}` (in `DATA_DIR`) these are the
JAR Data and Code Sharing Policy artifacts.

The R-only sister template uses an equivalent `batch_run()` that calls
`R CMD BATCH` instead of `run_with_echo.py`. SAS and Stata produce the
same SAS-log shape natively. All four pipeline languages emit visually
consistent per-script logs.

`batch_run()` is also useful for one-off pulls: write a small
`pulls/<date>-<topic>.py`, then call `batch_run("pulls/<date>-<topic>.py")`
from a Python shell. The sibling `.log` lands next to the script.

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
