#!/bin/bash
PROJECT_ID=cast-defect-detection
TOPIC_NAME=incoming-image-topic
SUB_NAME=incoming-image-subscription

# Create Pub/Sub topic and subscription
gcloud pubsub topics create ${TOPIC_NAME} --project=${PROJECT_ID}
gcloud pubsub subscriptions create ${SUB_NAME} --topic=${TOPIC_NAME} --project=${PROJECT_ID}