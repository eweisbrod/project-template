# Your Project Name

> **This is a template.** Click "Use this template" on GitHub to create your
> own project, then customize this README for your research.

## About This Template

This R project template provides a complete pipeline for empirical
Accounting/Finance research using data from WRDS. It covers:

1. Downloading data from WRDS (Compustat + CRSP)
2. Merging databases and creating variables
3. Creating publication-ready figures
4. Producing formatted tables for LaTeX or MS Word

The included example uses an **earnings announcement event study** to
demonstrate each step: it downloads quarterly fundamentals and daily stock
returns, computes standardized unexpected earnings (SUE), and tests whether
the market reacts more strongly to earnings changes when sales move in the
same direction. For more context on the pedagogical design behind this
template, see the [parent project](https://github.com/eweisbrod/example-project).

## Quick Start

1. Click **"Use this template"** on GitHub to create your own repo
2. Clone your new repo and open it in RStudio
3. Run `src/setup.R` to create your `.env` file (it will prompt you for paths
   and WRDS credentials)
4. Run scripts in order: `1-download-data.R` through
   `4-analyze-data-and-tabulate-*.R`

Packages are auto-installed via `pacman::p_load()` -- no separate install
step needed.

## Project Structure

```
your-project-name/
├── .gitignore              # R and project-specific ignores
├── AGENTS.md               # AI assistant context (see below)
├── CLAUDE.md               # Claude Code config (imports AGENTS.md)
├── README.md               # This file
├── output/                 # Tables and figures (gitignored contents)
├── src/
│   ├── setup.R                       # One-time project setup (creates .env)
│   ├── utils.R                       # Helper functions (winsorize, FF industries,
│   │                                 #   download_wrds, trading_day_window)
│   ├── 1-download-data.R             # Download from WRDS to parquet files
│   ├── 2-transform-data.R            # Merge databases, create variables, BHARs
│   ├── 3-figures.R                   # Publication-ready figures
│   ├── 4-analyze-data-and-tabulate-latex.R   # Tables for LaTeX output
│   ├── 4-analyze-data-and-tabulate-word.R    # Tables for Word output
│   └── run-all.R                             # Master script with logging
└── LICENSE
```

### Script Execution Order

Scripts are numbered and should be run in order. Every script loads `.env`
via `dotenv` and sources `utils.R` at the top.

| Script | Purpose |
|--------|---------|
| `1-download-data.R` | Download Compustat, CRSP, CCM link to parquet files |
| `2-transform-data.R` | Merge databases, create SUE, compute BHARs |
| `3-figures.R` | Generate publication-ready figures |
| `4-analyze-data-and-tabulate-latex.R` | Regression tables for LaTeX |
| `4-analyze-data-and-tabulate-word.R` | Regression tables for Word/Office |
| `run-all.R` | Master script: runs everything with logging |

You can run scripts individually (line by line in RStudio) or use `run-all.R`
to run the full pipeline and produce a log file for replication purposes.

## Prerequisites

- **R** (>= 4.0) -- <https://cran.rstudio.com/>
- **RStudio** -- <https://posit.co/download/rstudio-desktop/>
- **Git** -- <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>
- **WRDS account** -- <https://wrds-www.wharton.upenn.edu/>

## Configuration

### Setting up `.env`

Run `src/setup.R` to create your `.env` file interactively. Or create it
manually in the project root:

```
DATA_DIR=D:/Dropbox/your-project-name
OUTPUT_DIR=output
```

- **`DATA_DIR`** -- where raw and processed data files are stored. This
  should typically be outside the project folder (e.g., a shared Dropbox
  folder) since Git is designed for code, not data.
- **`OUTPUT_DIR`** -- where tables and figures are saved. Defaults to the
  `output/` folder in the project. You might change this to a folder synced
  with Overleaf, for example.
- Use **forward slashes** (`/`) even on Windows
- The `.env` file is gitignored so each collaborator has their own copy
- Each script reads this via `dotenv::load_dot_env(".env")`

### WRDS Credentials

WRDS credentials are stored securely using the `keyring` package (not in
code or `.env`). See the detailed setup comments at the top of
`src/1-download-data.R`.

## Output

Output goes to the directory specified by `OUTPUT_DIR` in your `.env` file.
By default this is the `output/` folder in the project (gitignored).

- **LaTeX**: `*.tex` files that can be read directly by an Overleaf project.
  Example Overleaf template: <https://www.overleaf.com/read/ctmwnmdcypzh>
- **Word**: `*.docx` files with all tables and figures
- **Figures**: `*.pdf` (LaTeX) or `*.png` (Word)

## Customizing for Your Project

1. Run `src/setup.R` or update `.env` with your paths
2. Edit `src/1-download-data.R` to download the data you need
3. Adapt `src/2-transform-data.R` for your variables and sample filters
4. Modify the figure and table scripts for your analyses
5. Update this README with your project's description

## About AGENTS.md and CLAUDE.md

You may notice two AI-related files in this repo:

- **`AGENTS.md`** is part of an emerging open standard for giving AI coding
  assistants context about a project. Tools like GitHub Copilot, Cursor, and
  Windsurf read it automatically. It describes the project structure,
  conventions, and common pitfalls so that AI tools can assist more
  effectively. Even if you don't use AI tools, it serves as useful project
  documentation.

- **`CLAUDE.md`** is a one-line file that tells Claude Code (Anthropic's CLI
  tool) to read `AGENTS.md`. Claude Code reads `CLAUDE.md` rather than
  `AGENTS.md` directly, so this file bridges the two. If you want to add
  Claude-specific instructions, you can add them below the `@AGENTS.md`
  import line.

This layered approach means project conventions are written once in
`AGENTS.md` and shared across all AI tools.

For more details, see:
- [Linux Foundation AI Agent Configuration](https://www.linuxfoundation.org/press/linux-foundation-launches-open-standard-for-configuring-ai-coding-agents)
- [Claude Code documentation](https://docs.anthropic.com/en/docs/claude-code/memory)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE)
for details.
