from google.cloud import storage
from google.cloud import bigquery
import base64
import json
import os
import uuid
from cloudevents.http import CloudEvent
import functions_framework
from ultralytics import YOLO
from datetime import datetime

# Set your project and dataset
project_id = "cast-defect-detection"

# bq dataset and table
dataset_id = "cast_defect_detection"
table_id = "inference_results"
bq_table_id = f"{project_id}.{dataset_id}.{table_id}"

# GCS buckets for image and model
bucket_name_image = "metal_casting_images"
bucket_name_model = "metal_casting_model"
model_file_name = "v0.pt"

# Insert new inference result to bq
def update_bq_record(res_image_path, raw_image_path, model_ver, pred_class, pred_confidence, pred_speed, res_insert_datetime):
    """Inserts a new inference result with a unique UUID as res_id."""
    # # Step 1: Get the highest res_id
    # get_max_res_id_query = f"SELECT MAX(res_id) AS max_res_id FROM `{bq_table_id}`"
    # try:
    #     client = bigquery.Client()
    #     query_job = client.query(get_max_res_id_query)
    #     result = list(query_job.result())
    #     new_res_id = result[0]["max_res_id"] + 1 if result and result[0][0] is not None else 0
    # except Exception as e:
    #     print(f"Error fetching max res_id: {e}")
    #     new_res_id = 0  # Default to 0 if table is empty or not found
    
    # Generate a unique res_id using UUID
    new_res_id = str(uuid.uuid4())  # Convert UUID to string

    # Step 2: Insert new row with the new incremented res_id
    insert_query = f"""
    INSERT INTO `{bq_table_id}` (res_id, res_image_path, raw_image_path, model_ver, pred_class, 
                              pred_confidence, pred_speed, res_insert_datetime)
    VALUES ('{new_res_id}', '{res_image_path}', '{raw_image_path}', '{model_ver}', 
            '{pred_class}', {pred_confidence}, {pred_speed}, '{res_insert_datetime}');
    """

    client = bigquery.Client()    
    query_job = client.query(insert_query)
    query_job.result()  # Wait for completion

    print(f"Inserted new record with res_id: {new_res_id}")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Cloud Storage bucket after verifying it exists."""
    if not os.path.exists(source_file_name):
        raise FileNotFoundError(f"Error: Source file '{source_file_name}' not found.")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        public_url = f"https://storage.googleapis.com/{bucket_name}/{destination_blob_name}"
        print(f"Uploaded storage object. Public url is {public_url}")
        return public_url  # Return the gs:// path

    except Exception as e:
        print(f"Error uploading file: {e}")
        return None  # Return None if an error occurs

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from a Cloud Storage bucket after verifying the blob exists."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    if not blob.exists(storage_client):
        raise FileNotFoundError(f"Error: Blob '{source_blob_name}' not found in bucket '{bucket_name}'.")

    try:
        blob.download_to_filename(destination_file_name)
        gs_path = f"gs://{bucket_name}/{source_blob_name}"
        print(f"Downloaded storage object from {gs_path} to local file {destination_file_name}.")
        return gs_path  # Return the gs:// path

    except Exception as e:
        print(f"Error downloading file: {e}")
        return None  # Return None if an error occurs
    
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """Inference image when new images ingested to GCS and insert inference result to BQ"""
    decoded_message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    message = json.loads(decoded_message)

    # Download image
    download_file_name = message["name"].split("/")[-1]
    raw_image_path = download_blob(message["bucket"], message["name"], download_file_name)

    # Download and load model
    download_blob(bucket_name_model, model_file_name, model_file_name)
    model = YOLO(model_file_name)

    # Inference image
    results = model.predict(download_file_name, save = True, show_conf = True)

    # Print, upload and store inference result
    if results:
        for res in results:
            #Upload result image to GCS
            result_filename = res.path.split("/")[-1]
            res.save(filename=result_filename)
            destination_blob_name = 'result/' + result_filename
            res_image_path = upload_blob(bucket_name_image, result_filename, destination_blob_name)

            #Retrieve result class and confidence score
            pred_class_index = res.probs.top1  # Get the index of the top prediction
            pred_class_name = res.names[pred_class_index]  # Get the top prediction class
            pred_confidence = res.probs.data[pred_class_index].item()  # Get confidence score
            print(json.dumps({"class": pred_class_name, "confidence": pred_confidence}))

            #Write result to BQ table
            update_bq_record(
                res_image_path=res_image_path,
                raw_image_path=raw_image_path,
                model_ver=model_file_name.split(".")[0],
                pred_class=pred_class_name,
                pred_confidence=pred_confidence,
                pred_speed=round(sum(res.speed.values())/1000, 3),
                res_insert_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )