# Your Project Name

> **This is a template.** Click "Use this template" on GitHub to create your
> own project, then customize this README for your research.

## About This Template

This polyglot project template provides a complete pipeline for empirical
Accounting/Finance research using data from WRDS. It demonstrates the same
analysis implemented in **Python**, **R**, and **Stata**, with parquet files
as the common interchange format between languages.

The pipeline:

1. **Download** data from WRDS (Python)
2. **Transform** data — merge databases, create variables, compute BHARs (Python)
3. **Figures** — publication-ready plots (Python via plotnine, or R via ggplot2)
4. **Tables** — regression tables and descriptive stats (Python via pyfixest, R via fixest/modelsummary, or Stata via reghdfe/esttab)

The included example uses an **earnings announcement event study** to
demonstrate each step: it downloads quarterly fundamentals and daily stock
returns, computes standardized unexpected earnings (SUE), and tests whether
the market reacts more strongly to earnings changes when sales move in the
same direction (SUE x SameSign interaction).

For a pure R version of this template, see
[project-template-r](https://github.com/eweisbrod/project-template-r).
For more context on the pedagogical design, see the
[parent project](https://github.com/eweisbrod/example-project).

## Quick Start

1. Click **"Use this template"** on GitHub to create your own repo
2. Clone your new repo
3. Run setup:
   ```bash
   uv run src/setup.py
   ```
   This creates your `.env` file and stores WRDS credentials. (Requires
   [uv](https://docs.astral.sh/uv/getting-started/installation/) to be
   installed.)
4. Run the pipeline:
   ```bash
   uv run src/1-download-data.py     # ~20 min (WRDS download)
   uv run src/2-transform-data.py    # ~2 min
   uv run src/3-figures.py           # or: Rscript src/3-figures.R
   uv run src/4-analyze-data.py      # or: Rscript src/4-analyze-data.R
                                     # or: stata -b do src/4-analyze-data.do
   ```

## Project Structure

```
your-project-name/
├── .env.example            # Template for user's .env (copy and edit)
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
- **Stata** (16+) with `reghdfe`, `estout`, `projectpaths`, `doenv` packages

## Configuration

### Setting up `.env`

Run `uv run src/setup.py` to create your `.env` file interactively. Or
copy `.env.example` to `.env` and edit manually:

```
DATA_DIR=D:/Dropbox/your-project-name
OUTPUT_DIR=D:/Dropbox/your-project-name/output
```

- **`DATA_DIR`** — where raw and processed data files are stored (outside
  the project folder, e.g., a shared Dropbox folder)
- **`OUTPUT_DIR`** — where tables and figures are saved
- Use **forward slashes** (`/`) even on Windows
- The `.env` file is gitignored so each collaborator has their own copy

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
