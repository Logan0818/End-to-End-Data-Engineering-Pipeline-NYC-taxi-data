# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import os
# import requests
# from google.cloud import storage

# BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"

# DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
# LOCAL_FILE = "/tmp/yellow_tripdata_2023-01.parquet"
# GCS_OBJECT_NAME = "bronze/yellow_tripdata_2023-01.parquet"


# def download_taxi_data():
#     response = requests.get(DATA_URL, stream=True, timeout=120)
#     response.raise_for_status()

#     print(f"HTTP status: {response.status_code}")
#     print(f"Content-Type: {response.headers.get('Content-Type')}")

#     with open(LOCAL_FILE, "wb") as f:
#         for chunk in response.iter_content(chunk_size=1024 * 1024):
#             if chunk:
#                 f.write(chunk)

#     file_size = os.path.getsize(LOCAL_FILE)
#     print(f"Downloaded file size: {file_size} bytes")

#     if file_size == 0:
#         raise ValueError("Downloaded file is empty.")

#     # Basic parquet validation: parquet files should end with b'PAR1'
#     with open(LOCAL_FILE, "rb") as f:
#         f.seek(-4, os.SEEK_END)
#         tail = f.read(4)

#     print(f"Tail bytes: {tail}")

#     if tail != b"PAR1":
#         raise ValueError(
#             f"Downloaded file is not a valid parquet file. Tail bytes: {tail}"
#         )

#     print(f"Downloaded valid parquet file to {LOCAL_FILE}")


# def upload_to_gcs():
#     if not os.path.exists(LOCAL_FILE):
#         raise FileNotFoundError(f"Local file not found: {LOCAL_FILE}")

#     client = storage.Client()
#     bucket = client.bucket(BUCKET_NAME)
#     blob = bucket.blob(GCS_OBJECT_NAME)

#     blob.upload_from_filename(LOCAL_FILE)

#     print(f"Uploaded {LOCAL_FILE} to gs://{BUCKET_NAME}/{GCS_OBJECT_NAME}")


# with DAG(
#     dag_id="taxi_data_ingestion",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["taxi", "ingestion", "bronze"],
# ) as dag:

#     download_task = PythonOperator(
#         task_id="download_taxi_data",
#         python_callable=download_taxi_data,
#     )

#     upload_task = PythonOperator(
#         task_id="upload_to_gcs",
#         python_callable=upload_to_gcs,
#     )

#     download_task >> upload_task

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import requests
from google.cloud import storage

BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_year_month():
    """
    Priority:
    1. dag_run.conf["year_month"] if provided manually
    2. logical_date minus 37 months
       Example:
       2026-03 -> 2023-02
       2026-04 -> 2023-03
    """
    context = get_current_context()
    dag_run = context.get("dag_run")
    logical_date = context["logical_date"]

    if dag_run and dag_run.conf and dag_run.conf.get("year_month"):
        year_month = dag_run.conf.get("year_month")
    else:
        target_date = logical_date - relativedelta(months=37)
        year_month = target_date.strftime("%Y-%m")

    return year_month


def build_file_info(year_month: str):
    file_name = f"yellow_tripdata_{year_month}.parquet"
    data_url = f"{BASE_URL}/{file_name}"
    local_file = f"/tmp/{file_name}"
    gcs_object_name = f"bronze/{file_name}"

    return {
        "file_name": file_name,
        "data_url": data_url,
        "local_file": local_file,
        "gcs_object_name": gcs_object_name,
    }


def download_taxi_data():
    year_month = get_year_month()
    file_info = build_file_info(year_month)

    data_url = file_info["data_url"]
    local_file = file_info["local_file"]

    print(f"year_month: {year_month}")
    print(f"Downloading from: {data_url}")
    print(f"Saving to local file: {local_file}")

    response = requests.get(data_url, stream=True, timeout=120)
    response.raise_for_status()

    print(f"HTTP status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")

    with open(local_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    file_size = os.path.getsize(local_file)
    print(f"Downloaded file size: {file_size} bytes")

    if file_size == 0:
        raise ValueError("Downloaded file is empty.")

    with open(local_file, "rb") as f:
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)

    print(f"Tail bytes: {tail}")

    if tail != b"PAR1":
        raise ValueError(
            f"Downloaded file is not a valid parquet file. Tail bytes: {tail}"
        )

    print(f"Downloaded valid parquet file to {local_file}")


def upload_to_gcs():
    year_month = get_year_month()
    file_info = build_file_info(year_month)

    local_file = file_info["local_file"]
    gcs_object_name = file_info["gcs_object_name"]

    if not os.path.exists(local_file):
        raise FileNotFoundError(f"Local file not found: {local_file}")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_object_name)

    blob.upload_from_filename(local_file)

    print(f"Uploaded {local_file} to gs://{BUCKET_NAME}/{gcs_object_name}")


with DAG(
    dag_id="taxi_data_ingestion",
    start_date=datetime(2026, 3, 1),
    schedule="@monthly",
    catchup=False,
    tags=["taxi", "ingestion", "bronze"],
) as dag:

    download_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    download_task >> upload_task