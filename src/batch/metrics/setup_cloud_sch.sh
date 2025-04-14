#!/bin/bash
PROJECT_ID=cast-defect-detection
TOPIC_NAME=metric-calc-topic

gcloud scheduler jobs create pubsub trigger-metrics-calculation \
  --schedule "15 * * * *" \
  --topic ${TOPIC_NAME} \
  --message-body "Run metrics" \
  --location us-central1 \
  --project ${PROJECT_ID} 