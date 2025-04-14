#!/bin/bash
REGION=us-central1

TOPIC_NAME=incoming-image-topic
BUCKET_NAME_IMAGE=metal_casting_images
OBJECT_PREFIX_IMAGE=raw
BUCKET_NAME_MODEL=metal_casting_model

# Create Cloud Storage bucket
gsutil mb -l ${REGION} gs://${BUCKET_NAME_IMAGE}/
gsutil mb -l ${REGION} gs://${BUCKET_NAME_MODEL}/

# Create pub-sub notification based on bucket and object prefix only for image ingestion listener.
gcloud storage buckets notifications create gs://${BUCKET_NAME_IMAGE}/ --topic=${TOPIC_NAME} --object-prefix=${OBJECT_PREFIX_IMAGE}/