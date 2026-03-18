# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     to_date,
#     year,
#     month,
#     dayofmonth,
#     hour,
#     unix_timestamp,
#     round
# )

# BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"

# BRONZE_PATH = f"gs://{BUCKET_NAME}/bronze/yellow_tripdata_2023-01.parquet"
# SILVER_PATH = f"gs://{BUCKET_NAME}/silver/yellow_tripdata_2023-01/"

# spark = (
#     SparkSession.builder
#     .appName("bronze_to_silver_gcs")
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#     .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcp-key.json")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# df = spark.read.parquet(BRONZE_PATH)

# selected_df = df.select(
#     col("VendorID"),
#     col("tpep_pickup_datetime"),
#     col("tpep_dropoff_datetime"),
#     col("passenger_count"),
#     col("trip_distance"),
#     col("fare_amount"),
#     col("tip_amount"),
#     col("total_amount"),
#     col("payment_type"),
#     col("PULocationID"),
#     col("DOLocationID")
# )

# silver_df = (
#     selected_df
#     .filter(col("tpep_pickup_datetime").isNotNull())
#     .filter(col("tpep_dropoff_datetime").isNotNull())
#     .filter(col("trip_distance") >= 0)
#     .filter(col("fare_amount") >= 0)
#     .withColumn(
#         "trip_duration_min",
#         round(
#             (unix_timestamp(col("tpep_dropoff_datetime")) -
#              unix_timestamp(col("tpep_pickup_datetime"))) / 60.0,
#             2
#         )
#     )
#     .filter(col("trip_duration_min") > 0)
#     .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
#     .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
#     .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
#     .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))
#     .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
# )

# print("=== Silver schema ===")
# silver_df.printSchema()

# print("=== Sample rows ===")
# silver_df.show(5, truncate=False)

# print(f"Row count: {silver_df.count()}")

# (
#     silver_df.write
#     .mode("overwrite")
#     .parquet(SILVER_PATH)
# )

# print(f"Bronze to Silver complete. Output written to: {SILVER_PATH}")

# spark.stop()


import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
    hour,
    unix_timestamp,
    round
)

BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation for NYC taxi data")
    parser.add_argument(
        "--year_month",
        required=True,
        help="Target year-month in YYYY-MM format, e.g. 2023-03"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    year_month = args.year_month

    bronze_path = f"gs://{BUCKET_NAME}/bronze/yellow_tripdata_{year_month}.parquet"
    silver_path = f"gs://{BUCKET_NAME}/silver/yellow_tripdata_{year_month}/"

    print(f"Processing year_month: {year_month}")
    print(f"Reading bronze data from: {bronze_path}")
    print(f"Writing silver data to: {silver_path}")

    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver_{year_month}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcp-key.json")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(bronze_path)

    print("=== Raw schema ===")
    df.printSchema()

    selected_df = df.select(
        col("VendorID"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_type"),
        col("PULocationID"),
        col("DOLocationID")
    )

    silver_df = (
        selected_df
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
        .filter(col("trip_distance") >= 0)
        .filter(col("fare_amount") >= 0)
        .withColumn(
            "trip_duration_min",
            round(
                (unix_timestamp(col("tpep_dropoff_datetime")) -
                 unix_timestamp(col("tpep_pickup_datetime"))) / 60.0,
                2
            )
        )
        .filter(col("trip_duration_min") > 0)
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    )

    print("=== Silver schema ===")
    silver_df.printSchema()

    print("=== Sample rows ===")
    silver_df.show(5, truncate=False)

    print(f"Row count: {silver_df.count()}")

    (
        silver_df.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f"Bronze to Silver complete. Output written to: {silver_path}")

    spark.stop()


if __name__ == "__main__":
    main()