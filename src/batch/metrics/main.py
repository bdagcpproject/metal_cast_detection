from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
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

UTC = timezone.utc

def upsert_metrics(table_id, metric, bq_client):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Check if record exists
    check_query = f"""
        SELECT COUNT(*) as count
        FROM `{table_ref}`
        WHERE aggregation_start = @agg_start AND aggregation_end = @agg_end
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("agg_start", "DATETIME", metric["aggregation_start"]),
            bigquery.ScalarQueryParameter("agg_end", "DATETIME", metric["aggregation_end"]),
        ]
    )
    result = list(bq_client.query(check_query, job_config=job_config).result())[0]

    if result.count > 0:
        print(f"Updating existing row in {table_id} for week {metric['aggregation_start']} to {metric['aggregation_end']}")
        update_query = f"""
            UPDATE `{table_ref}`
            SET
              {', '.join(f'{k} = @{k}' for k in metric.keys() if k != 'id')}
            WHERE aggregation_start = @agg_start AND aggregation_end = @agg_end
        """
        update_params = [
            bigquery.ScalarQueryParameter(k, "STRING" if isinstance(v, str) else "FLOAT" if isinstance(v, float) else "INT64", v)
            for k, v in metric.items() if k != "id"
        ] + [
            bigquery.ScalarQueryParameter("agg_start", "DATETIME", metric["aggregation_start"]),
            bigquery.ScalarQueryParameter("agg_end", "DATETIME", metric["aggregation_end"]),
        ]
        update_config = bigquery.QueryJobConfig(query_parameters=update_params)
        bq_client.query(update_query, job_config=update_config).result()
    else:
        print(f"Inserting new row into {table_id} for week {metric['aggregation_start']} to {metric['aggregation_end']}")
        bq_client.insert_rows_json(table_ref, [metric])

def get_week_start(date: datetime) -> datetime:
    offset = (date.weekday() + 1) % 7  # Sunday = 0
    sunday = date - timedelta(days=offset)
    return sunday.replace(hour=0, minute=0, second=0, microsecond=0)

def get_week_ranges(start: datetime, end: datetime):
    current = get_week_start(start)
    while current < end:
        yield (current, current + timedelta(days=7))
        current += timedelta(days=7)

def aggregate_weekly_metrics(bq_client, agg_start, agg_end):
    agg_start_str = agg_start.strftime("%Y-%m-%d %H:%M:%S")
    agg_end_str = agg_end.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        SELECT res_id, pred_class, pred_confidence, pred_Speed, res_insert_datetime
        FROM `{project_id}.{dataset_id}.{res_table_id}`
        WHERE res_insert_datetime >= '{agg_start_str}'
          AND res_insert_datetime < '{agg_end_str}'
    """
    df = bq_client.query(query).result().to_dataframe()
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
        "inference_time_min": df["pred_Speed"].min(),
        "inference_time_med": df["pred_Speed"].median(),
        "inference_time_mean": df["pred_Speed"].mean(),
        "inference_time_max": df["pred_Speed"].max(),
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

        if not min_datetime:
            print("No data in inference_results table.")
            return

        if min_datetime.tzinfo is None:
            min_datetime = min_datetime.replace(tzinfo=UTC)
        else:
            min_datetime = min_datetime.astimezone(UTC)

        now_utc = datetime.now(UTC)

        for agg_start, agg_end in get_week_ranges(min_datetime, now_utc):
            print(f" Processing week: {agg_start} to {agg_end} (UTC)")

            inf_metrics, conf_metrics, class_metrics = aggregate_weekly_metrics(
                bq_client, agg_start, agg_end
            )

            if inf_metrics:
                upsert_metrics(inf_metric_table_id, inf_metrics, bq_client)
                upsert_metrics(conf_metric_table_id, conf_metrics, bq_client)
                upsert_metrics(pred_class_table_id, class_metrics, bq_client)
                print("✅ Metrics inserted or updated.")
            else:
                continue

    except Exception as e:
        print(f" Error during metrics processing: {e}")
        raise
