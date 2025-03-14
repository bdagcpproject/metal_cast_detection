#!/bin/bash
# Replace YOUR_PROJECT_ID with your actual project id.
PROJECT_ID=cast-defect-detection
REGION=us-central1
ZONE=us-central1-a

TOPIC_NAME=incoming-image-topic
BUCKET_NAME_IMAGE=metal_casting_images
OBJECT_PREFIX_IMAGE=raw
BUCKET_NAME_MODEL=metal_casting_model

# Create Pub/Sub topic and subscription
gcloud pubsub topics create ${TOPIC_NAME} --project=${PROJECT_ID}
gcloud pubsub subscriptions create incoming-image-subscription --topic=${TOPIC_NAME} --project=${PROJECT_ID}

# Create Cloud Storage bucket
gsutil mb -l ${REGION} gs://${BUCKET_NAME_IMAGE}/
gsutil mb -l ${REGION} gs://${BUCKET_NAME_MODEL}/

# Create pub-sub notification based on bucket and object prefix only for image ingestion listener.
gcloud storage buckets notifications create gs://${BUCKET_NAME_IMAGE}/ --topic=${TOPIC_NAME} --object-prefix=${OBJECT_PREFIX_IMAGE}/

# Create Cloud Scheduler job to trigger the batch ETL pipeline every 15 minutes.
# Here we simulate a job that publishes a message to a Pub/Sub topic (which you could have a Cloud Function or similar subscribe to)
# gcloud scheduler jobs create pubsub batch-etl-job \
#   --schedule "*/15 * * * *" \
#   --topic image-topic \
#   --message-body '{"trigger": "batch_etl"}' \
#   --project=${PROJECT_ID}

# echo "Deployment complete."