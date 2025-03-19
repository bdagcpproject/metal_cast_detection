#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <interval_in_seconds>"
    exit 1
fi

# Read the input interval (in seconds)
INTERVAL=$1

# Define source directory
SRC_DIR=~/OneDrive/Documents/Programming/metal_cast_detection/notebooks/model_training/datasets/metal_casting_2/test/

while true; do
    # Find all JPEG files, pick one randomly
    SELECTED_FILE=$(find "$SRC_DIR" -type f -name "*.jpeg" | shuf -n 1)

    # Check if a file was found
    if [[ -n "$SELECTED_FILE" ]]; then
        # Extract just the filename
        FILE_NAME=$(basename "$SELECTED_FILE")

        # Define destination in GCS
        GCS_BUCKET="gs://metal_casting_images/raw/$FILE_NAME"

        # Upload to GCS
        gsutil cp "$SELECTED_FILE" "$GCS_BUCKET"

        echo "Uploaded $SELECTED_FILE to $GCS_BUCKET"
    else
        echo "No JPEG files found!"
    fi

    # Wait for the user-defined interval
    sleep "$INTERVAL"
done