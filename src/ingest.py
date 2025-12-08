from pyspark.sql import SparkSession
from delta import *


def run_ingestion():
    # --- 1. SPARK SESSION CONFIGURATION ---
    print("--- Configuring Spark with Delta & MinIO Support ---")

    packages = ["com.crealytics:spark-excel_2.12:3.5.0_0.20.3"]

    builder = (
        SparkSession.builder.appName("BronzeIngestion")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    # Initialize Spark
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Reduce noise

    print("--- Spark Session Started ---")

    # --- 2. DEFINE PATHS ---
    # Input: The raw CSV sitting in MinIO
    raw_path = "s3a://raw/telco_churn.xlsx"

    # Output: The Bronze bucket (Delta Format)
    bronze_path = "s3a://bronze/telco_churn_delta"

    # --- 3. READ RAW DATA ---
    print(f"Reading from: {raw_path}")
    try:
        # We allow Spark to infer schema for Bronze, but assume header exists
        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .csv(raw_path)
        )

        row_count = df.count()
        print(f"--- Data Loaded: {row_count} rows ---")

        # --- 4. WRITE TO BRONZE (DELTA LAKE) ---
        print(f"Writing Delta Table to: {bronze_path}")

        # 'overwrite' ensures idempotency (if you run it twice, it doesn't duplicate data)
        df.write.format("delta").mode("overwrite").save(bronze_path)

        print("--- Ingestion Complete. Bronze Layer Ready. ---")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        raise e

    spark.stop()


if __name__ == "__main__":
    run_ingestion()
