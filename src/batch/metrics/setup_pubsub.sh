#!/bin/bash
PROJECT_ID=cast-defect-detection
TOPIC_NAME=metric-calc-topic
SUB_NAME=metric-calc-subscription

# Create Pub/Sub topic and subscription
gcloud pubsub topics create ${TOPIC_NAME} --project=${PROJECT_ID}
gcloud pubsub subscriptions create ${SUB_NAME} --topic=${TOPIC_NAME} --project=${PROJECT_ID}