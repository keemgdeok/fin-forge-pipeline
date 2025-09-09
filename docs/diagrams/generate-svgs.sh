#!/bin/bash

# ==============================================================================
#           Mermaid Markdown to SVG Conversion Script (v4 - Final)
# ==============================================================================
#
# Description:
#   This script robustly converts all .md files in its subdirectories to .svg files.
#   It changes into each file's directory before conversion to ensure output files
#   are created in the correct location, fixing issues with `mmdc` ignoring output paths.
#
# Pre-requisites:
#   - `@mermaid-js/mermaid-cli` must be installed globally.
#   - `puppeteer-config.json` MUST be in the SAME directory as this script.
#
# Usage:
#   Run this script from the project's root directory.
#   Example: ./docs/diagrams/generate-svgs.sh
#
# ==============================================================================

# Get the absolute path to the directory where this script is located.
# This ensures that paths work correctly regardless of where the script is called from.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PUPPETEER_CONFIG_ABSOLUTE="$SCRIPT_DIR/puppeteer-config.json"

# --- Main Logic ---
echo "🚀 Starting Mermaid to SVG conversion (Robust overwrite mode)..."

if [ ! -f "$PUPPETEER_CONFIG_ABSOLUTE" ]; then
    echo "❌ Error: Puppeteer config file not found at '${PUPPETEER_CONFIG_ABSOLUTE}'."
    exit 1
fi

# Find all .md files and loop through them
find "$SCRIPT_DIR" -type f -name "*.md" | while read -r mdfile_absolute; do
    
    # Use a subshell (...) to change directory temporarily.
    # This is a safe way to run commands in a different folder without affecting the main script.
    (
        TARGET_DIR=$(dirname "$mdfile_absolute")
        MD_FILENAME=$(basename "$mdfile_absolute")
        SVG_FILENAME="${MD_FILENAME%.md}.svg"

        # Change to the target directory
        cd "$TARGET_DIR" || exit 1

        echo "  - Converting in [$TARGET_DIR]:"
        echo "    $MD_FILENAME  ->  $SVG_FILENAME"

        # Force remove the existing SVG file to ensure overwrite
        rm -f "$SVG_FILENAME"
        
        # Run the mmdc command with the absolute path to the config file
        # and simple filenames for input/output.
        mmdc -p "$PUPPETEER_CONFIG_ABSOLUTE" -i "$MD_FILENAME" -o "$SVG_FILENAME"
    )
done

echo ""
echo "✅ All diagrams converted successfully!"
