"""run_with_echo.py — execute a Python script and print each statement before
running it, producing a log file in the visual style of a SAS log,
Stata log, or R `.Rout` file.

Usage:
    uv run python run_with_echo.py path/to/script.py > path/to/script.log 2>&1

The wrapper parses the target script with `ast`, walks its top-level
statements, and for each one:
  - prints any blank lines or comments that fall *between* statements
    (so the source's structure is preserved);
  - prints the statement itself with `>>> ` on its first line and `... `
    on continuation lines (matching Python's REPL convention);
  - compiles the statement and exec()s it in a single shared namespace,
    so anything stdout-printed by the statement appears immediately after
    the echoed source — exactly the SAS/Stata/.Rout interleaving.

Limitations:
  - `__name__ == "__main__"` is set to True so scripts that gate code on
    that block still execute.
  - Tracebacks for exceptions point at the wrapper's compile step rather
    than the original line; for that reason we re-compile each statement
    with the original filename so traceback line numbers stay accurate.
  - Output that the script writes via `\\r` (carriage-return progress
    bars) will collapse into one final line in the log, which is fine
    for a saved log but unlike the live console.
"""

import ast
import sys
from pathlib import Path


def run_with_echo(script_path: str) -> None:
    src_text = Path(script_path).read_text(encoding="utf-8")
    src_lines = src_text.splitlines()
    tree = ast.parse(src_text, filename=script_path)

    # Single namespace so variables defined by one statement are visible to
    # later ones — without this we'd lose names between exec() calls.
    namespace: dict = {"__name__": "__main__", "__file__": script_path}

    cursor = 0  # 0-indexed: index of the next source line we have not yet echoed

    for node in tree.body:
        # ast linenos are 1-indexed and inclusive; convert to 0-indexed slice
        start = node.lineno - 1
        end = (node.end_lineno or node.lineno)  # 1-indexed inclusive -> exclusive

        # Print any comment / blank lines that appeared between statements.
        # Emit them verbatim (no prefix) so the log reads like the source file.
        for line in src_lines[cursor:start]:
            print(line)

        # Echo the statement itself, with REPL-style prefixes on each line.
        for i, line in enumerate(src_lines[start:end]):
            prefix = ">>> " if i == 0 else "... "
            print(f"{prefix}{line}")

        # Compile and execute.
        module = ast.Module(body=[node], type_ignores=[])
        code = compile(module, script_path, "exec")
        exec(code, namespace)

        # Flush so output is interleaved with prints in the right order
        # when the wrapper is itself piped to a file.
        sys.stdout.flush()

        cursor = end

    # Trailing comments / blank lines after the last statement.
    for line in src_lines[cursor:]:
        print(line)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("usage: python run_with_echo.py <script.py>\n")
        sys.exit(2)

    sys.stdout.reconfigure(encoding="utf-8")
    run_with_echo(sys.argv[1])
