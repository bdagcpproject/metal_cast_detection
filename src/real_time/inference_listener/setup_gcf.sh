#!/bin/bash
# This script sets up the Google Cloud Function for the inference listener.

gcloud functions deploy inference_listener-2 \
    --gen2 \
    --region=us-central1 \
    --runtime=python311 \
    --source=. \
    --entry-point=subscribe \
    --trigger-topic=incoming-image-topic \
    --clear-max-instances \
    --cpu=2 \
    --memory=8Gi