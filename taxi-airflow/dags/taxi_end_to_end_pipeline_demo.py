from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import requests
from google.cloud import storage

BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PROJECT_ID = "dtc-de-course-2026"
BQ_DATASET = "project_dataset"
BQ_TABLE = "daily_trip_metrics"
MONTH_OFFSET = 36


# def get_year_month():
#     """
#     Priority:
#     1. dag_run.conf["year_month"] if provided manually
#     2. logical_date minus MONTH_OFFSET months

#     Example:
#     2026-03 -> 2023-02
#     2026-04 -> 2023-03
#     """
#     context = get_current_context()
#     dag_run = context.get("dag_run")
#     logical_date = context["logical_date"]

#     if dag_run and dag_run.conf and dag_run.conf.get("year_month"):
#         return dag_run.conf.get("year_month")

#     target_date = logical_date - relativedelta(months=MONTH_OFFSET)
#     return target_date.strftime("%Y-%m")
def get_year_month():
    context = get_current_context()
    logical_date = context["logical_date"]

    base = datetime(2023, 1, 1)
    offset = logical_date.minute // 10  # 每10分鐘一個月

    target_date = base + relativedelta(months=offset)
    return target_date.strftime("%Y-%m")

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


def resolve_year_month():
    year_month = get_year_month()
    print(f"Resolved year_month: {year_month}")
    return year_month


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
    dag_id="taxi_end_to_end_pipeline",
    start_date=datetime(2026, 3, 1),
    # schedule="@monthly",
    schedule="*/10 * * * *",
    catchup=False,
    tags=["taxi", "bronze", "silver", "gold", "bigquery"],
) as dag:

    resolve_year_month_task = PythonOperator(
        task_id="resolve_year_month",
        python_callable=resolve_year_month,
    )

    download_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        docker exec -i spark /opt/spark/bin/spark-submit \
          --jars /opt/spark_jars/gcs-connector-hadoop3-latest.jar \
          /opt/spark_jobs/bronze_to_silver.py \
          --year_month {{ ti.xcom_pull(task_ids='resolve_year_month') }}
        """,
    )

    silver_to_gold_task = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        docker exec -i spark /opt/spark/bin/spark-submit \
          --jars /opt/spark_jars/gcs-connector-hadoop3-latest.jar \
          /opt/spark_jobs/silver_to_gold.py
        """,
    )

    load_gold_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_gold_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["gold/daily_trip_metrics/*.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={
            "type": "DAY",
            "field": "pickup_date",
        },
    )

    (
        resolve_year_month_task
        >> download_task
        >> upload_task
        >> bronze_to_silver_task
        >> silver_to_gold_task
        >> load_gold_to_bigquery_task
    )