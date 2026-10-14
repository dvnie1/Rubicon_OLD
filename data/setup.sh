#!/bin/bash

# Configuration
REPO_URL="https://github.com/gregrahn/ssb-kit.git"
REPO_DIR="ssb_generator"
SCALE_FACTORS=(10)
PYTHON_CONVERTER="./convert_to_binary.py"
CSV_DIR="csv"
BIN_DIR="binary"
SCHEMA_FILE="schema.txt"

# Loop through scale factors
for SF in "${SCALE_FACTORS[@]}"; do
    CSV_PATH="${CSV_DIR}/sf${SF}"
    BIN_PATH="${BIN_DIR}/sf${SF}"

    # If binary data already exists, skip everything
    if [ -d "$BIN_PATH" ] && find "$BIN_PATH" -mindepth 1 -type f -print -quit | grep -q .; then
        echo "Binary data for scale factor $SF already exists. Skipping all steps for sf${SF}."
        continue
    fi

    echo "=== Processing scale factor $SF ==="

    # Clone repo if not already cloned
    if [ ! -d "$REPO_DIR" ]; then
        echo "Cloning SSB repository..."
        git clone "$REPO_URL" "$REPO_DIR" || { echo "Clone failed"; exit 1; }
    else
        echo "SSB repository already cloned."
    fi

    cd "$REPO_DIR" || { echo "Failed to enter $REPO_DIR"; exit 1; }

    # Build if not already built
    if [ ! -f "./dbgen" ]; then
        echo "Building SSB..."
        cd ./dbgen || { echo "Failed to enter $REPO_DIR"; exit 1; }
        make MACHINE=LINUX DATABASE=POSTGRESQL || { echo "Build failed"; exit 1; }
    else
        echo "SSB already built."
    fi

    # Generate CSV data
    mkdir -p "../../$CSV_PATH"
    echo "Generating CSV data for scale factor $SF..."
    ./dbgen -s "$SF" -T a -f -v
    mv *.tbl "../../$CSV_PATH"

    cd ../..  # Go back out of $REPO_DIR

    # Convert to binary
    echo "Converting CSV to binary for scale factor $SF..."
    mkdir -p "$BIN_PATH"
    uv run "$PYTHON_CONVERTER" --input "$CSV_PATH" --output "$BIN_PATH" --schema "$SCHEMA_FILE" || { echo "Python conversion failed"; exit 1; }

    cp "$SCHEMA_FILE" "$BIN_PATH/"

    echo "=== Done with scale factor $SF ==="
done

# Cleanup CSV data
echo "Cleaning up CSV data for scale factor $SF..."
rm -rf "$CSV_DIR"

# Remove generator (repo) only if this is the last SF being processed
echo "Cleaning up SSB generator..."
rm -rf "$REPO_DIR"

echo "All tasks completed."