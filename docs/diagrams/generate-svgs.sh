#!/bin/bash

# ==============================================================================
#           Mermaid Markdown to SVG Conversion Script (v2)
# ==============================================================================
#
# Description:
#   This script converts all .md files in its subdirectories into .svg files.
#   It is designed to be self-contained within the `docs/diagrams` directory.
#
# Pre-requisites:
#   - `@mermaid-js/mermaid-cli` must be installed globally.
#   - `puppeteer-config.json` MUST be located in the SAME directory as this script.
#
# Usage:
#   Run this script from the project's root directory.
#   Example: ./docs/diagrams/generate-svgs.sh
#
# ==============================================================================

# --- Configuration ---
# Get the directory where this script is located.
SCRIPT_DIR=$(dirname "$0")

# Set the path for the Puppeteer config file to be in the SAME directory as the script.
PUPPETEER_CONFIG="$SCRIPT_DIR/puppeteer-config.json"

# Target width for generated SVGs (helps GitHub preview render larger images)
MERMAID_WIDTH=${MERMAID_WIDTH:-2400}


# --- Main Logic ---
echo "🚀 Starting Mermaid to SVG conversion..."

# Check if the puppeteer config file exists in the script's directory
if [ ! -f "$PUPPETEER_CONFIG" ]; then
    echo "❌ Error: Puppeteer config file not found at '${PUPPETEER_CONFIG}'."
    echo "Please make sure 'puppeteer-config.json' is in the same folder as this script."
    exit 1
fi

# Find all .md files in the subdirectories and loop through them
find "$SCRIPT_DIR" -type f -name "*.md" | while read -r mdfile; do
    svgfile="${mdfile%.md}.svg"
    echo "  - Converting: $mdfile  ->  $svgfile"
    
    # Run the mmdc command with the config file path and desired width
    mmdc -p "$PUPPETEER_CONFIG" -i "$mdfile" -o "$svgfile" -w "$MERMAID_WIDTH"
done

echo ""
echo "✅ All diagrams converted successfully!"
