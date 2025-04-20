from google.cloud import bigquery
from datetime import datetime, timedelta
from pytz import timezone
import uuid
from cloudevents.http import CloudEvent
import functions_framework

# Config
project_id = "cast-defect-detection"
dataset_id = "cast_defect_detection"
res_table_id = "inference_results"
inf_metric_table_id = "inference_metrics"
conf_metric_table_id = "confidencescore_metrics"
pred_class_table_id = "prediction_class_metrics"

# Timezone setting
SGT = timezone("Asia/Singapore")

def insert_metrics(table_id, data_dicts, bq_client):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    errors = bq_client.insert_rows_json(table_ref, data_dicts)
    if errors:
        raise RuntimeError(f"Insertion errors: {errors}")
    print(f"Inserted {len(data_dicts)} rows into {table_ref}")

def delete_existing_metrics(table_id, bq_client, agg_start, agg_end):
    query = f"""
        DELETE FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE aggregation_start = @agg_start AND aggregation_end = @agg_end
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("agg_start", "DATETIME", agg_start),
            bigquery.ScalarQueryParameter("agg_end", "DATETIME", agg_end),
        ]
    )
    bq_client.query(query, job_config=job_config).result()

def get_week_start(date: datetime) -> datetime:
    """Returns the previous Sunday 00:00 in SGT timezone."""
    offset = (date.weekday() + 1) % 7  # Sunday = 0
    sunday = date - timedelta(days=offset)
    return sunday.replace(hour=0, minute=0, second=0, microsecond=0)

def get_week_ranges(start: datetime, end: datetime):
    """Yield weekly ranges (Sunday to Sunday) in SGT."""
    current = get_week_start(start)
    while current < end:
        yield (current, current + timedelta(days=7))
        current += timedelta(days=7)

def aggregate_weekly_metrics(bq_client, agg_start, agg_end):
    # Format as UTC for BigQuery compatibility
    agg_start_utc = agg_start.astimezone(timezone("UTC"))
    agg_end_utc = agg_end.astimezone(timezone("UTC"))

    query = f"""
        SELECT res_id, pred_class, pred_confidence, pred_Speed, res_insert_datetime
        FROM `{project_id}.{dataset_id}.{res_table_id}`
        WHERE res_insert_datetime >= '{agg_start_utc}'
          AND res_insert_datetime < '{agg_end_utc}'
    """
    df = bq_client.query(query).result().to_dataframe()
    print(f"Fetched {len(df)} rows from {agg_start} to {agg_end} (SGT)")

    if df.empty:
        return None, None, None

    vc = df["pred_class"].value_counts().to_dict()
    now = datetime.now(SGT).strftime("%Y-%m-%d %H:%M:%S")

    inf_metrics = {
        "id": str(uuid.uuid4()),
        "inference_time_min": df["pred_Speed"].min(),
        "inference_time_med": df["pred_Speed"].median(),
        "inference_time_mean": df["pred_Speed"].mean(),
        "inference_time_max": df["pred_Speed"].max(),
        "insert_datetime": now,
        "aggregation_start": agg_start.strftime("%Y-%m-%d %H:%M:%S"),
        "aggregation_end": agg_end.strftime("%Y-%m-%d %H:%M:%S"),
    }

    conf_metrics = {
        "id": str(uuid.uuid4()),
        "confidence_score_min": df["pred_confidence"].min(),
        "confidence_score_med": df["pred_confidence"].median(),
        "confidence_score_mean": df["pred_confidence"].mean(),
        "confidence_score_max": df["pred_confidence"].max(),
        "insert_datetime": now,
        "aggregation_start": agg_start.strftime("%Y-%m-%d %H:%M:%S"),
        "aggregation_end": agg_end.strftime("%Y-%m-%d %H:%M:%S"),
    }

    class_metrics = {
        "id": str(uuid.uuid4()),
        "pred_class_pass_freq": vc.get("OK", 0),
        "pred_class_fail_freq": vc.get("Defect", 0),
        "insert_datetime": now,
        "aggregation_start": agg_start.strftime("%Y-%m-%d %H:%M:%S"),
        "aggregation_end": agg_end.strftime("%Y-%m-%d %H:%M:%S"),
    }

    return inf_metrics, conf_metrics, class_metrics


# Triggered from a message on a Cloud Pub/Sub topic.

@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """ The actual message content from Scheduler isn't used here, the trigger itself is the signal to run. """
    print(f"Triggered by event ID: {cloud_event['id']}")
    bq_client = bigquery.Client(project=project_id)

    try:
        # Step 1: Get oldest record
        oldest_query = f"""
            SELECT MIN(res_insert_datetime) AS min_datetime
            FROM `{project_id}.{dataset_id}.{res_table_id}`
        """
        result = list(bq_client.query(oldest_query).result())[0]
        min_datetime = result.min_datetime

        if not min_datetime:
            print("No data in inference_results table.")
            return

        # Convert to timezone-aware SGT
        if min_datetime.tzinfo is None:
            min_datetime = min_datetime.replace(tzinfo=timezone("UTC")).astimezone(SGT)
        else:
            min_datetime = min_datetime.astimezone(SGT)

        # Step 2: Weekly aggregation from oldest to current week
        now_sgt = datetime.now(SGT)
        for agg_start, agg_end in get_week_ranges(min_datetime, now_sgt):
            print(f"Processing week: {agg_start} to {agg_end} (SGT)")

            inf_metrics, conf_metrics, class_metrics = aggregate_weekly_metrics(
                bq_client, agg_start, agg_end
            )

            if inf_metrics:
                delete_existing_metrics(inf_metric_table_id, bq_client, agg_start, agg_end)
                delete_existing_metrics(conf_metric_table_id, bq_client, agg_start, agg_end)
                delete_existing_metrics(pred_class_table_id, bq_client, agg_start, agg_end)

                insert_metrics(inf_metric_table_id, [inf_metrics], bq_client)
                insert_metrics(conf_metric_table_id, [conf_metrics], bq_client)
                insert_metrics(pred_class_table_id, [class_metrics], bq_client)
                print("Inserted metrics.")
            else:
                print("No data for this week. Skipping...")

    except Exception as e:
        print(f"Error during metrics processing: {e}")
        raise
