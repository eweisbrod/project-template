# setup.R — One-time project setup
# ===========================================================================
# Run this script INTERACTIVELY in your R console (line by line, not source).
# It creates your .env file and stores your WRDS credentials.
#
# You only need to run this once per computer. If you need to change paths
# later, just edit the .env file directly, or re-run this script.
# ===========================================================================


# Check if .env already exists -------------------------------------------------

# If you already have a .env file, this will warn you before overwriting.
if (file.exists(".env")) {
  message("NOTE: .env file already exists. Running this script will overwrite it.")
  message("Current contents:")
  cat(readLines(".env"), sep = "\n")
  message("\nContinue running line by line to overwrite, or stop here.")
}


# Set your DATA_DIR path ------------------------------------------------------

# DATA_DIR is where your raw and processed data files will be stored.
# It should NOT be inside the Git project folder — Git is for code, not data.
# A shared folder (e.g., Dropbox) works well for collaborating with coauthors.
#
# Use forward slashes (/) even on Windows.
# Example: D:/Dropbox/my-project/data

# Run this line — a prompt will appear in the console. Type your path and hit Enter.
data_dir <- readline(prompt = "Enter DATA_DIR path: ")

# Automatically convert backslashes to forward slashes (common on Windows)
data_dir <- gsub("\\\\", "/", data_dir)

# Check what you entered
data_dir


# Set your OUTPUT_DIR path -----------------------------------------------------

# OUTPUT_DIR is where tables and figures will be saved.
# The default is the 'output' folder inside this project (already gitignored).
# You might change this to a folder synced with Overleaf, for example.
#
# Press Enter at the prompt to accept the default ("output"), or type a path.

output_dir <- readline(prompt = "Enter OUTPUT_DIR path (or press Enter for default): ")
output_dir <- gsub("\\\\", "/", output_dir)
if (output_dir == "") output_dir <- "output"

# Check what you entered
output_dir


# Write the .env file ----------------------------------------------------------

# The .env file is a simple text file with KEY=VALUE pairs. It is gitignored,
# so each collaborator has their own local copy with their own paths.
# All scripts read it via dotenv::load_dot_env(".env").

env_lines <- c(
  "# Local environment configuration",
  "# Created by setup.R — edit as needed",
  "# Use forward slashes (/) even on Windows",
  "",
  paste0("DATA_DIR=", data_dir),
  paste0("OUTPUT_DIR=", output_dir)
)

writeLines(env_lines, ".env")

# Confirm it was written
message(".env file created:")
cat(readLines(".env"), sep = "\n")


# Create the directories if they don't exist -----------------------------------

if (!dir.exists(data_dir)) {
  dir.create(data_dir, recursive = TRUE)
  message("Created data directory: ", data_dir)
}

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  message("Created output directory: ", output_dir)
}


# Set up WRDS credentials (keyring) -------------------------------------------

# The download scripts store your WRDS username and password securely in your
# operating system's credential store (Windows Credential Manager, macOS
# Keychain, etc.) using the keyring package. This is more secure than putting
# passwords in .env files or in your code.
#
# Run the two lines below. Each will pop up a prompt asking you to enter a value.
# You only need to do this once per computer.

if (!require("keyring")) install.packages("keyring")

# Run this line — enter your WRDS username when prompted
keyring::key_set("wrds_user")

# Run this line — enter your WRDS password when prompted
keyring::key_set("wrds_pw")

# Verify they were saved
message("WRDS username stored: ", keyring::key_get("wrds_user"))
if (nchar(keyring::key_get("wrds_pw")) > 0) {
  message("WRDS password stored: [hidden]")
} else {
  message("WARNING: WRDS password appears empty. Re-run keyring::key_set('wrds_pw')")
}

# If you need to update your credentials later (e.g., after a password change),
# just re-run the key_set lines above.


# Done! ------------------------------------------------------------------------

# You're ready to go. Run the scripts in order:
#   src/1-download-data.R
#   src/2-transform-data.R
#   src/3-figures.R
#   src/4-analyze-data-and-tabulate-latex.R  (or the word version)
