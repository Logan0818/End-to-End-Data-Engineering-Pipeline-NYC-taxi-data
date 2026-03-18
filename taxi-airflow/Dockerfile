FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    google-cloud-storage \
    apache-airflow-providers-google \
    requests