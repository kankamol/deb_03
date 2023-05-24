import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


PROJECT_ID = "zeta-bebop-384409"
BUCKET_NAME = "deb-bootcamp-100003"

DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"

# keyfile = os.environ.get("KEYFILE_PATH")


project_id = "zeta-bebop-384409"

# Load data from Local to GCS
def load_data_gcs(bucketname, data_to_load):
    keyfile = "zeta-bebop-384409-3e06adae2c7d_gcs.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bucket_name = bucketname
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)
    data = data_to_load
    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

# Load data from GCS to BigQuery
def load_data_gcs_to_bqry(bucketname, data_to_load, partition_dt="", clustering_fields=[]):
    keyfile = "zeta-bebop-384409-eca552063775.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
        location=LOCATION,
    )
    data = data_to_load
    table_id = f"{PROJECT_ID}.deb_bootcamp.{data}"
    dt = partition_dt
    partition = dt.replace("-", "")

    if clustering_fields:
        job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        ),
        clustering_fields=["first_name", "last_name"],
        )
    elif partition_dt:
       job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        ),
        )
    else:
        job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        )
 
    bucket_name = bucketname
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


#load_data_gcs(BUCKET_NAME,"addresses")
#load_data_gcs_to_bqry(BUCKET_NAME,"addresses")
data = {"addresses":"",
        "promos":"",
        "products":"",
        "order_items":"",
        "events":"2021-02-10",
        "orders":"2021-02-10",
        "users":["2020-10-23", ["first_name", "last_name"]]}

for x, y in data.items():
 if y is not None and type(y) is str:
  load_data_gcs(BUCKET_NAME,x)
  load_data_gcs_to_bqry(BUCKET_NAME,x, y)
 elif y is not None and type(y) is list:
  load_data_gcs(BUCKET_NAME,x)
  load_data_gcs_to_bqry(BUCKET_NAME,x, y[0], y[1])
 else:
  load_data_gcs(BUCKET_NAME,x)
  load_data_gcs_to_bqry(BUCKET_NAME,x)