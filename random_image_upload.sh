#!/bin/bash

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <interval_in_seconds> <number_of_images>"
    exit 1
fi

INTERVAL=$1
NUM_IMAGES=$2
SRC_DIR=~/OneDrive/Documents/Programming/metal_cast_detection/notebooks/model_training/datasets/metal_casting_2/test/
GCS_BUCKET="gs://metal_casting_images/raw/"

while true; do
    # Improved file selection with proper null-terminated handling
    mapfile -d '' -t SELECTED_FILES < <(
        find "$SRC_DIR" -type f -name "*.jpeg" -print0 |
        shuf -z -n "$NUM_IMAGES"
    )

    if [ ${#SELECTED_FILES[@]} -gt 0 ]; then
        # Use parallel/multi-threaded upload (-m)
        echo "Starting parallel upload of ${#SELECTED_FILES[@]} files..."
        gsutil -m cp "${SELECTED_FILES[@]}" "$GCS_BUCKET"
        
        # Print results with bullet points
        echo -e "\nSuccessfully uploaded:"
        printf '  • %s\n' "${SELECTED_FILES[@]}"
    else
        echo "No JPEG files found!"
    fi

    # Sleep for the specified interval
    echo "Sleeping for $INTERVAL seconds..."
    sleep "$INTERVAL"
done