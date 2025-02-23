from google.cloud import storage, pubsub_v1, bigquery

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    return blob.public_url

def publish_message(topic_name, message, project_id="YOUR_PROJECT_ID"):
    """Publishes a message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message.encode("utf-8"))
    return future.result()

def insert_bigquery_row(dataset_id, table_id, rows_to_insert):
    """Inserts rows into a BigQuery table."""
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print("Errors while inserting rows: {}".format(errors))
    return errors
