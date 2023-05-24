import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "zeta-bebop-384409-eca552063775.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "zeta-bebop-384409"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

def load_data(data_file, partition_dt="", clustering_fields=[]):

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

    dt = partition_dt
    partition = dt.replace("-", "")
    data = data_file
    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        if partition=="":
            table_id = f"{project_id}.deb_bootcamp_test.{data}"
        else:
            table_id = f"{project_id}.deb_bootcamp_test.{data}${partition}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

data = {"addresses":"",
        "promos":"",
        "products":"",
        "order_items":"",
        "events":"2021-02-10",
        "orders":"2021-02-10",
        "users":["2020-10-23", ["first_name", "last_name"]]}
for x, y in data.items():
 if y is not None and type(y) is str:
  load_data(x, y)
 elif y is not None and type(y) is list:
  load_data(x, y[0], y[1])
 else:
  load_data(x)