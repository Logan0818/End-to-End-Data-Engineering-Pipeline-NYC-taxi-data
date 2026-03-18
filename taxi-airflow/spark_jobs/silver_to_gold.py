# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     count,
#     sum,
#     avg,
#     round
# )

# BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"

# SILVER_PATH = f"gs://{BUCKET_NAME}/silver/yellow_tripdata_2023-01/"
# GOLD_PATH = f"gs://{BUCKET_NAME}/gold/daily_trip_metrics/"

# spark = (
#     SparkSession.builder
#     .appName("silver_to_gold_gcs")
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#     .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcp-key.json")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# df = spark.read.parquet(SILVER_PATH)

# gold_df = (
#     df.groupBy("pickup_date")
#     .agg(
#         count("*").alias("total_trips"),
#         round(sum("total_amount"), 2).alias("total_revenue"),
#         round(avg("fare_amount"), 2).alias("avg_fare_amount"),
#         round(avg("trip_distance"), 2).alias("avg_trip_distance"),
#         round(avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
#     )
#     .orderBy(col("pickup_date"))
# )

# print("=== Gold schema ===")
# gold_df.printSchema()

# print("=== Gold sample rows ===")
# gold_df.show(10, truncate=False)

# (
#     gold_df.write
#     .mode("overwrite")
#     .parquet(GOLD_PATH)
# )

# print(f"Silver to Gold complete. Output written to: {GOLD_PATH}")

# spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round
)

BUCKET_NAME = "dtc-de-2026-taxi-data-lake-logan"


def main():
    silver_path = f"gs://{BUCKET_NAME}/silver/*/"
    gold_path = f"gs://{BUCKET_NAME}/gold/daily_trip_metrics/"

    print(f"Reading silver data from: {silver_path}")
    print(f"Writing gold data to: {gold_path}")

    spark = (
        SparkSession.builder
        .appName("silver_to_gold_all_months")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcp-key.json")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(silver_path)

    print("=== Silver schema ===")
    df.printSchema()

    gold_df = (
        df.groupBy("pickup_date")
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
        )
        .orderBy(col("pickup_date"))
    )

    print("=== Gold schema ===")
    gold_df.printSchema()

    print("=== Gold sample rows ===")
    gold_df.show(10, truncate=False)

    print(f"Gold row count: {gold_df.count()}")

    (
        gold_df.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print(f"Silver to Gold complete. Output written to: {gold_path}")

    spark.stop()


if __name__ == "__main__":
    main()