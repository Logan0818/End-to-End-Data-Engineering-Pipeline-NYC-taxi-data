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

## Problem Statement

- The goal of this project is to design and implement an end-to-end data pipeline that analyzes New York City taxi trip data to uncover business insights.
- Taxi companies generate large volumes of trip data daily, but without a structured data pipeline, it is difficult to efficiently analyze trends and identify opportunities for improvement. In particular, stakeholders want to understand:
    - Which days have the lowest taxi demand
    - How revenue changes over time
- Whether there are patterns or trends that suggest opportunities for targeted promotions
- To address this, the project builds a modern data pipeline that ingests raw taxi trip data, processes and transforms it into structured datasets, and makes it available for analytics.
- The pipeline enables:
    1. Identification of low-demand days where business performance is weakest
    2. Analysis of revenue trends over time (daily/monthly)
    3. Data-driven decision-making to evaluate whether promotions or incentives could improve performance
    4. By transforming raw data into actionable insights, this project demonstrates how data engineering can directly support business optimization and strategic planning.


## 🏗️ Architecture

![Architecture](https://github.com/Logan0818/End-to-End-Data-Engineering-Pipeline-NYC-taxi-data/blob/ce31c3f22f3825bc873714eb26addccd0385b483/Images/taxi%20architecture%20diagram.png)


Pipeline flow:

1. Airflow downloads monthly taxi data
2. Uploads raw data to GCS (Bronze)
3. Spark transforms data → Silver
4. Spark aggregates data → Gold
5. Airflow loads Gold data into BigQuery
6. Looker Studio dashboard consumes BigQuery

---

## ⚙️ Tech Stack

- Terraform (IaC)
- Apache Airflow (Docker)
- Apache Spark (Docker)
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

Create Google Storage Bucket and BigQuery dataset using Terraform
 - Create a service account
 - Generate a new key (json file)
 - Update the main.tf and variable.tf files under terraform folder to use your google credential

```
terraform init
terraform plan
terraform apply
```
In terminal, go to taxi-airflow folder and run below docker commands.

```bash
docker compose up airflow-init
docker compose up
```

---


Then open Airflow UI:

 - http://localhost:8080

Trigger DAG:

 - taxi_end_to_end_pipeline
 - Once complete, you will have daily_trip_metrics table in your BigQuery dataset
 - You can now create dashboard using Looker Studio

📅 Automation Logic

 - Runs monthly

 - Automatically back-calculates data (2026 → 2023 offset)

 - For testing, I have a demo pipeline which runs every 10 minutes and each time run a different month (see taxi_end_to_end_pipeline_demo.py in dags folder)

 - Supports manual trigger with:

 - {"year_month": "2023-03"}

📈 Dashboard

![Dashboard](https://github.com/Logan0818/End-to-End-Data-Engineering-Pipeline-NYC-taxi-data/blob/37aae6bcae733c5988ed85b195cc347449269d89/taxi%20looker%20studio.png)


💡 Key Learnings

 - Built a full medallion architecture pipeline

 - Integrated Airflow with Spark via Docker

 - Docker permissions

 - GCS connector setup

 - BigQuery partition conflicts

 - Implemented automated scheduling and parameterization

📌 Future Improvements

 - Incremental loading instead of full refresh

 - Partitioned writes in Spark

 - Data quality checks

 - CI/CD pipeline

👤 Author

 - Logan Liang