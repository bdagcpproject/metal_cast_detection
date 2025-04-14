#!/bin/bash
gcloud functions deploy batch_metric_analytic \
    --gen2 \
    --region=us-central1 \
    --runtime=python311 \
    --source=. \
    --entry-point=subscribe \
    --trigger-topic=metric-calc-topic \
    --memory=1Gi