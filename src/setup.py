# setup.py — One-time project setup
# ===========================================================================
# Run this script to create your .env file and store your WRDS credentials.
#
# HOW TO RUN:
#   uv run src/setup.py
#
# This script:
#   1. Creates a .env file with your DATA_DIR and OUTPUT_DIR paths
#   2. Creates the directories if they don't exist
#   3. Stores your WRDS username and password securely in your operating
#      system's credential store (Windows Credential Manager, macOS Keychain)
#
# You only need to run this once per computer. If you need to change paths
# later, edit .env directly. To update WRDS credentials, re-run this script.
#
# Credentials are stored via the keyring package, which uses the same OS
# credential store across Python and R. Both languages read from the same
# keyring entries, so you only need to set credentials once.
# ===========================================================================

import os
from pathlib import Path

import keyring


def main():
    print("=" * 60)
    print("Project Setup")
    print("=" * 60)

    # --- Check for existing .env ---

    env_path = Path(".env")
    if env_path.exists():
        print(f"\nNOTE: .env file already exists:")
        print(env_path.read_text())
        resp = input("Overwrite? (y/n): ").strip().lower()
        if resp != "y":
            print("Keeping existing .env.")
        else:
            _create_env(env_path)
    else:
        _create_env(env_path)

    # --- WRDS credentials ---

    print("\n--- WRDS Credentials ---")
    print("These are stored securely in your OS credential store")
    print("(Windows Credential Manager / macOS Keychain).")
    print("They are NOT saved in any file.\n")

    # Check for existing credentials
    existing_user = keyring.get_password("wrds", "username")
    if existing_user:
        print(f"Existing WRDS username found: {existing_user}")
        resp = input("Update credentials? (y/n): ").strip().lower()
        if resp != "y":
            print("Keeping existing credentials.")
            _verify_credentials()
            return

    username = input("Enter your WRDS username: ").strip()
    password = input("Enter your WRDS password: ").strip()

    keyring.set_password("wrds", "username", username)
    keyring.set_password("wrds", "password", password)

    print(f"\nWRDS credentials stored for: {username}")

    _verify_credentials()

    print("\n--- Done! ---")
    print("You're ready to go. Run the scripts in order:")
    print("  uv run src/1-download-data.py")
    print("  uv run src/2-transform-data.py")
    print("  Rscript src/3-figures.R   (or: uv run src/3-figures.py)")
    print("  Rscript src/4-analyze-data-and-tabulate-latex.R")
    print("    (or: uv run src/4-tabulate.py, or: stata -b do src/4-tabulate.do)")


def _create_env(env_path: Path):
    """Prompt for paths and write the .env file."""
    print("\n--- Data Directory ---")
    print("Where should raw and processed data files be stored?")
    print("This should be OUTSIDE the project folder (e.g., a Dropbox folder).")
    print("Use forward slashes (/) even on Windows.")
    data_dir = input("DATA_DIR: ").strip()
    # Convert backslashes to forward slashes (common on Windows)
    data_dir = data_dir.replace("\\", "/")

    print("\n--- Output Directory ---")
    print("Where should tables and figures be saved?")
    output_dir = input("OUTPUT_DIR (press Enter for 'output'): ").strip()
    output_dir = output_dir.replace("\\", "/")
    if not output_dir:
        output_dir = "output"

    env_content = (
        f"DATA_DIR={data_dir}\n"
        f"OUTPUT_DIR={output_dir}\n"
    )
    env_path.write_text(env_content, encoding="utf-8")
    print(f"\n.env file created:")
    print(env_content)

    # Create directories
    for d in [data_dir, output_dir]:
        Path(d).mkdir(parents=True, exist_ok=True)
        print(f"  Directory ready: {d}")


def _verify_credentials():
    """Verify stored credentials can be retrieved."""
    user = keyring.get_password("wrds", "username")
    pw = keyring.get_password("wrds", "password")
    if user and pw:
        print(f"  Verified: username='{user}', password=[stored]")
    else:
        print("  WARNING: credentials could not be verified!")


if __name__ == "__main__":
    main()
