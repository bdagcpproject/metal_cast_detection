from google.cloud import bigquery

client = bigquery.Client()

# Set your project and dataset
project_id = "cast-defect-detection"
dataset_id = "cast_defect_detection"
table_id = "inference_results"

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
    bigquery.SchemaField("res_id", "STRING"),
    bigquery.SchemaField("res_image_path", "STRING"),
    bigquery.SchemaField("raw_image_path", "STRING"),
    bigquery.SchemaField("model_ver", "STRING"),
    bigquery.SchemaField("pred_class", "STRING"),
    bigquery.SchemaField("pred_confidence", "FLOAT"),
    bigquery.SchemaField("pred_speed", "FLOAT"),
    bigquery.SchemaField("res_insert_datetime", "DATETIME")
]

# Check if table exists
try:
    client.get_table(table_id)
    print(f"Table {table_id} already exists in dataset {dataset_id}.")
except Exception:
    # Create table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)

    # Add partitioning on `res_insert_datetime` (without expiration)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="res_insert_datetime"  # Partitioning column
    )

    table = client.create_table(table)

    print(
        f"Created table {table.project}.{table.dataset_id}.{table.table_id}, "
        f"partitioned on column {table.time_partitioning.field}."
    )