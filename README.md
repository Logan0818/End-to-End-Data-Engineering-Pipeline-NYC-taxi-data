# 🚕 NYC Taxi Data Pipeline (End-to-End)

An end-to-end data engineering pipeline that ingests, transforms, and analyzes NYC taxi trip data using Airflow, Spark, GCS, and BigQuery.

---

## 📌 Project Overview

This project demonstrates a production-style data pipeline:

- Automated ingestion using Airflow
- Data lake architecture (Bronze / Silver / Gold)
- Distributed processing with Spark
- Storage on Google Cloud Storage
- Analytics in BigQuery
- Visualization with Looker Studio

---

## 🏗️ Architecture

![Architecture](images/architecture.png)

Pipeline flow:

1. Airflow downloads monthly taxi data
2. Uploads raw data to GCS (Bronze)
3. Spark transforms data → Silver
4. Spark aggregates data → Gold
5. Airflow loads Gold data into BigQuery
6. Looker Studio dashboard consumes BigQuery

---

## ⚙️ Tech Stack

- Apache Airflow (Docker)
- Apache Spark
- Google Cloud Storage (GCS)
- BigQuery
- Python
- Docker / Docker Compose

---

## 🔄 Pipeline Flow
![Pipeline Flow](https://github.com/Logan0818/End-to-End-Data-Engineering-Pipeline-NYC-taxi-data/blob/37aae6bcae733c5988ed85b195cc347449269d89/taxi%20airflow%20graph.png)

---

## 📊 Data Model

### Silver Layer
- Cleaned taxi trip data
- Added features:
  - trip_duration
  - pickup_date
  - pickup_hour

### Gold Layer
- Daily aggregations:
  - total trips
  - total revenue
  - avg trip duration

---

## 🚀 How to Run

```bash
docker compose up airflow-init
docker compose up
```

---

## 📊 Data Model

### Silver Layer
- Cleaned taxi trip data
- Added features:
  - trip_duration
  - pickup_date
  - pickup_hour

### Gold Layer
- Daily aggregations:
  - total trips
  - total revenue
  - avg trip duration

---

## 🚀 How to Run

```bash
docker compose up airflow-init
docker compose up
```

Then open Airflow UI:

http://localhost:8080

Trigger DAG:

taxi_end_to_end_pipeline

📅 Automation Logic

Runs monthly

Automatically back-calculates data (2026 → 2023 offset)

Supports manual trigger with:

{"year_month": "2023-03"}

📈 Dashboard

![Dashboard](https://github.com/Logan0818/End-to-End-Data-Engineering-Pipeline-NYC-taxi-data/blob/37aae6bcae733c5988ed85b195cc347449269d89/taxi%20looker%20studio.png)


💡 Key Learnings

Built a full medallion architecture pipeline

Integrated Airflow with Spark via Docker

Solved real-world issues:

Docker permissions

GCS connector setup

BigQuery partition conflicts

Implemented automated scheduling and parameterization

📌 Future Improvements

Incremental loading instead of full refresh

Partitioned writes in Spark

Data quality checks (Great Expectations)

CI/CD pipeline

👤 Author

Logan Liang