from google.cloud import bigquery
from datetime import datetime, timedelta
import uuid
from cloudevents.http import CloudEvent
import functions_framework

# Set your project
project_id = "cast-defect-detection"

# bq dataset
dataset_id = "cast_defect_detection"

# bq tables
res_table_id = "inference_results"
inf_metric_table_id = "inference_metrics"
conf_metric_table_id = "confidencescore_metrics"
pred_class_table_id = "prediction_class_metrics"

def insert_metrics(project_id, dataset_id, table_id, data_dicts, bq_client):
    """Batch insert multiple dictionaries into BigQuery."""
    if bq_client is None:
        bq_client = bigquery.Client(project=project_id)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    errors = bq_client.insert_rows_json(table_ref, data_dicts)
    
    if errors:
        raise RuntimeError(f"Errors occurred during insertion: {errors}")
    else:
        print(f"Successfully inserted {len(data_dicts)} rows into {table_ref}")

def get_metrics(project_id, dataset_id, table_id, bq_client, past_days):
    """
    Reads past X days of rows from BigQuery and aggregates the output y to statistical output.
    Returns a dictionary with the aggregated metrics.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=project_id)

    # Calculate the date X days ago
    x_days_ago = datetime.now() - timedelta(days=past_days)
    x_ago_str = x_days_ago.strftime("%Y-%m-%d %H:%M:%S")

    # Construct the query to read data
    query = f"""
        SELECT res_id, pred_class, pred_confidence, pred_Speed, res_insert_datetime
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE res_insert_datetime >= '{x_ago_str}'
    """
    res_df = bq_client.query_and_wait(query).to_dataframe()

    print(f"Fetched {len(res_df)} rows from BQ {project_id}.{dataset_id}.{table_id}")

    # Calculate the date X days ago
    curr_datetime = datetime.now()
    curr_datetime_str = curr_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # Calculate the aggregated metrics
    if not res_df.empty:
        inference_time_metrics = {
            "id": str(uuid.uuid4()),
            "inference_time_min": res_df["pred_Speed"].min(),
            "inference_time_med": res_df["pred_Speed"].median(),
            "inference_time_mean": res_df["pred_Speed"].mean(),
            "inference_time_max": res_df["pred_Speed"].max(),
            "insert_datetime": curr_datetime_str
        }

        confidence_score_metrics = {
            "id": str(uuid.uuid4()),
            "confidence_score_min": res_df["pred_confidence"].min(),
            "confidence_score_med": res_df["pred_confidence"].median(),
            "confidence_score_mean": res_df["pred_confidence"].mean(),
            "confidence_score_max": res_df["pred_confidence"].max(),
            "insert_datetime": curr_datetime_str    
        }

        pred_class_metrics = {
            "id": str(uuid.uuid4()),
            "pred_class_pass_freq": res_df["pred_class"].value_counts().to_dict()["OK"],  # Most common class
            "pred_class_fail_freq": res_df["pred_class"].value_counts().to_dict()["Defect"],  # Count of each class
            "insert_datetime": curr_datetime_str
        }

    else:
        inference_time_metrics = {
            "id": str(uuid.uuid4()),
            "inference_time_min": None,
            "inference_time_med": None,
            "inference_time_mean": None,
            "inference_time_max": None,
            "insert_datetime": None
        }

        confidence_score_metrics = {
            "id": str(uuid.uuid4()),
            "confidence_score_min": None,
            "confidence_score_med": None,
            "confidence_score_mean": None,
            "confidence_score_max": None,
            "insert_datetime": None
        }

        pred_class_metrics = {
            "id": str(uuid.uuid4()),
            "pred_class_mean": None,
            "pred_class_count": {},
            "insert_datetime": None
        }

    return inference_time_metrics, confidence_score_metrics, pred_class_metrics


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """ The actual message content from Scheduler isn't used here, the trigger itself is the signal to run. """
    
    print(f"Received event ID: {cloud_event['id']} - Triggering metrics calculation.")

    bq_client = bigquery.Client(project=project_id)
    try:
        metrics = get_metrics(project_id, dataset_id, res_table_id, bq_client, 45)

        # Check if metrics were successfully calculated (handles empty DataFrame case)
        if metrics[0].get("insert_datetime"): # Check if a valid timestamp was set
            insert_metrics (project_id, dataset_id, inf_metric_table_id, [metrics[0]], bq_client)
            insert_metrics (project_id, dataset_id, conf_metric_table_id, [metrics[1]], bq_client)
            insert_metrics (project_id, dataset_id, pred_class_table_id, [metrics[2]], bq_client)
            print("Metrics calculation and insertion successful.")
        else:
            print("No recent data found or metrics calculation skipped.")

    except Exception as e:
        print(f"Error during metrics processing: {e}")