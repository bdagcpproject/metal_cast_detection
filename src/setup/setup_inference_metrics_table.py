from google.cloud import bigquery

client = bigquery.Client()

# Set your project and dataset
project_id = "cast-defect-detection"
dataset_id = "cast_defect_detection"
table_id = "inference_metrics"

# Fully qualified table ID
table_id = f"{project_id}.{dataset_id}.{table_id}"

# Check if dataset exists, if not, create it
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)  # Check if dataset exists
    print(f"Dataset {dataset_id} already exists.")
except Exception:
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = "US"  # Set your preferred location
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} created.")

# Define the schema with DATETIME type
schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("inference_time_min", "FLOAT"),
    bigquery.SchemaField("inference_time_med", "FLOAT"),
    bigquery.SchemaField("inference_time_mean", "FLOAT"),
    bigquery.SchemaField("inference_time_max", "FLOAT"),
    bigquery.SchemaField("insert_datetime", "DATETIME"),
    bigquery.SchemaField("aggregation_start", "DATETIME"),
    bigquery.SchemaField("aggregation_end", "DATETIME")
]

# Check if table exists
try:
    client.get_table(table_id)
    print(f"Table {table_id} already exists in dataset {dataset_id}.")
except Exception:
    # Create table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")