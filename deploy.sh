#!/bin/bash
# Replace YOUR_PROJECT_ID with your actual project id.
PROJECT_ID=cast-defect-detection
REGION=us-central1
ZONE=us-central1-a

TOPIC_NAME=incoming-image-topic
BUCKET_NAME=metal_casting_images

# Create Pub/Sub topic (and optionally subscription)
gcloud pubsub topics create ${TOPIC_NAME} --project=${PROJECT_ID}
gcloud pubsub subscriptions create incoming-image-subscription --topic=${TOPIC_NAME} --project=${PROJECT_ID}

# Create Cloud Storage bucket
gsutil mb -l ${REGION} gs://${BUCKET_NAME}/
gcloud storage buckets notifications create gs://${BUCKET_NAME}/ --topic=${TOPIC_NAME} 

# Create BigQuery dataset (and later, tables should be created as needed)
# bq --location=${REGION} mk --dataset ${PROJECT_ID}:predictions_dataset

# (Optional) Create BigQuery tables using DDL or via the console.
# Example: Create table inference_results with columns: image_id (STRING), detections (RECORD), inference_time (TIMESTAMP)

# Create a Dataproc cluster for the inference service
# gcloud dataproc clusters create inference-cluster \
#   --region=${REGION} --zone=${ZONE} \
#   --master-machine-type=n1-standard-2 \
#   --num-workers=2 \
#   --project=${PROJECT_ID}

# Create Cloud Scheduler job to trigger the batch ETL pipeline every 15 minutes.
# Here we simulate a job that publishes a message to a Pub/Sub topic (which you could have a Cloud Function or similar subscribe to)
# gcloud scheduler jobs create pubsub batch-etl-job \
#   --schedule "*/15 * * * *" \
#   --topic image-topic \
#   --message-body '{"trigger": "batch_etl"}' \
#   --project=${PROJECT_ID}

# echo "Deployment complete."
