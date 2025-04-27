from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import uuid
from cloudevents.http import CloudEvent
import functions_framework
import pandas as pd

# Config
project_id = "cast-defect-detection"
dataset_id = "cast_defect_detection"
res_table_id = "inference_results"
inf_metric_table_id = "inference_metrics"
conf_metric_table_id = "confidencescore_metrics"
pred_class_table_id = "prediction_class_metrics"

UTC = timezone.utc

def upsert_metrics(table_id, metric, bq_client):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Check if record exists
    check_query = f"""
        SELECT COUNT(*) as count
        FROM `{table_ref}`
        WHERE aggregation_start = '{metric["aggregation_start"]}' AND aggregation_end = '{metric["aggregation_end"]}'
    """
    result = list(bq_client.query(check_query).result())[0]

    if result.count > 0:
        print(f"Updating existing row in {table_id} for week {metric['aggregation_start']} to {metric['aggregation_end']}")
        update_query = f"""
            UPDATE `{table_ref}`
            SET
              {', '.join(f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" for k, v in metric.items() if k not in ['id', 'aggregation_start', 'aggregation_end'])}
            WHERE aggregation_start = '{metric["aggregation_start"]}' AND aggregation_end = '{metric["aggregation_end"]}'
        """
        bq_client.query(update_query).result()

    else:
        print(f"Inserting new row into {table_id} for week {metric['aggregation_start']} to {metric['aggregation_end']}")
        bq_client.insert_rows_json(table_ref, [metric])

def get_week_start(date):
    offset = (date.weekday() + 1) % 7  # Sunday = 0
    sunday = date - timedelta(days=offset)
    return sunday.replace(hour=0, minute=0, second=0, microsecond=0)

def get_week_ranges(start, end):
    current = get_week_start(start)
    while current < end:
        yield (current, current + timedelta(days=7))
        current += timedelta(days=7)

def aggregate_weekly_metrics(bq_client, agg_start, agg_end):
    agg_start_str = agg_start.strftime("%Y-%m-%d %H:%M:%S")
    agg_end_str = agg_end.strftime("%Y-%m-%d %H:%M:%S")

    result_query = f"""
        SELECT res_id, pred_class, pred_confidence, pred_speed
        FROM `{project_id}.{dataset_id}.{res_table_id}`
        WHERE res_insert_datetime >= '{agg_start_str}'
          AND res_insert_datetime < '{agg_end_str}'
    """

    df = bq_client.query(result_query).result().to_dataframe()
    print(f"Fetched {len(df)} rows from {agg_start_str} to {agg_end_str} (UTC)")

    if df.empty:
        print(f"⚠️ No data found between {agg_start_str} and {agg_end_str}. Skipping this week.")
        return None, None, None

    vc = df["pred_class"].value_counts().to_dict()
    insert_time = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    common_fields = {
        "insert_datetime": insert_time,
        "aggregation_start": agg_start.strftime("%Y-%m-%d %H:%M:%S"),
        "aggregation_end": agg_end.strftime("%Y-%m-%d %H:%M:%S"),
    }

    inf_metrics = {
        "id": str(uuid.uuid4()),
        "inference_time_min": df["pred_speed"].min(),
        "inference_time_med": df["pred_speed"].median(),
        "inference_time_mean": df["pred_speed"].mean(),
        "inference_time_max": df["pred_speed"].max(),
        **common_fields
    }

    conf_metrics = {
        "id": str(uuid.uuid4()),
        "confidence_score_min": df["pred_confidence"].min(),
        "confidence_score_med": df["pred_confidence"].median(),
        "confidence_score_mean": df["pred_confidence"].mean(),
        "confidence_score_max": df["pred_confidence"].max(),
        **common_fields
    }

    class_metrics = {
        "id": str(uuid.uuid4()),
        "pred_class_pass_freq": vc.get("OK", 0),
        "pred_class_fail_freq": vc.get("Defect", 0),
        **common_fields
    }

    return inf_metrics, conf_metrics, class_metrics

def get_existing_agg_ranges(bq_client, start_datetime):
    """
    Query all tables for aggregation ranges starting from a specific datetime.
    Returns a dictionary of sets containing (agg_start, agg_end) tuples for each table.
    """
    tables = {
        "inference": inf_metric_table_id,
        "confidence": conf_metric_table_id,
        "pred_class": pred_class_table_id
    }
    
    existing_ranges = {}
    start_dt = start_datetime.strftime("%Y-%m-%d %H:%M:%S")

    for table_name, table_id in tables.items():
        query = f"""
            SELECT DISTINCT aggregation_start, aggregation_end
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE aggregation_start >= '{start_dt}'
            ORDER BY aggregation_start
        """
        
        result = bq_client.query(query)
        
        print(f"Fetched agg range from {start_dt} from {table_name} table")

        existing_ranges[table_name] = set((row.aggregation_start, row.aggregation_end) for row in result)
    
    return existing_ranges        # Check if the week range already exists in any of the tables


def is_agg_week_missing(week_start, week_end, existing_ranges):
    """
    Check if a week range exists in all three aggregated tables.
    Returns True if missing from any table, False if present in all.
    """

    target = (week_start, week_end)

    for table_name in existing_ranges.keys():
        if target not in existing_ranges[table_name]:
            print(f"Week {week_start} to {week_end} does not exists in {table_name} tables")
            return True  # Week not exists in one of the tables

    print(f"Week {week_start} to {week_end} exists in all tables")
    return False # Week exists from all tables

@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    print(f"Triggered by event ID: {cloud_event['id']}")
    bq_client = bigquery.Client(project=project_id)

    try:
        oldest_query = f"""
            SELECT MIN(res_insert_datetime) AS min_datetime
            FROM `{project_id}.{dataset_id}.{res_table_id}`
        """
        result = list(bq_client.query(oldest_query).result())[0]
        min_datetime = result.min_datetime
        
        print(f"Minimum datetime in inference_results table: {min_datetime}")

        if not min_datetime:
            print("No data in inference_results table.")
            return

        now_utc = datetime.now(UTC).replace(tzinfo=None)

        existing_ranges = get_existing_agg_ranges(bq_client, get_week_start(min_datetime)) # Get existing aggregation ranges from all tables

        force_update = False # Set to True to force update all weeks

        for agg_start, agg_end in get_week_ranges(min_datetime, now_utc):  # Iterate over weeks between min_datetime and now_utc

            is_missing = is_agg_week_missing(agg_start, agg_end, existing_ranges) # Check if the week range already exists in any of the tables
            is_current_week = now_utc >= agg_start and agg_end > now_utc # Check if the week is current week
            if not is_missing and not force_update and not is_current_week: 
                print(f"Skipping week {agg_start} to {agg_end} processing. Flag is_missing: {is_missing}, force_update: {force_update}, current_week: {is_current_week}")
                continue

            print(f"Processing week: {agg_start} to {agg_end} for all metrics. Flag is_missing: {is_missing}, force_update: {force_update}, current_week: {is_current_week}")

            inf_metrics, conf_metrics, class_metrics = aggregate_weekly_metrics(
                bq_client, agg_start, agg_end
            )

            if inf_metrics:
                upsert_metrics(inf_metric_table_id, inf_metrics, bq_client)
                print("✅ Inference metrics inserted or updated.")
            
            if conf_metrics:
                 upsert_metrics(conf_metric_table_id, conf_metrics, bq_client)
                 print("✅ Confidence metrics inserted or updated.")
            
            if class_metrics:
                upsert_metrics(pred_class_table_id, class_metrics, bq_client)
                print("✅ Prediction class metrics inserted or updated.")

    except Exception as e:
        print(f" Error during metrics processing: {e}")
        raise
