from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lower, regexp_replace, count
from pyspark.sql.types import DoubleType, IntegerType
from delta import *
import re
import sys


def to_snake_case(str):
    """Converts CamelCase to snake_case (e.g., 'TotalCharges' -> 'total_charges')"""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", str).lower()


def run_cleaning():
    print("--- [SILVER] Starting Cleaning & Standardization ---")

    # 1. Setup Spark
    builder = (
        SparkSession.builder.appName("SilverCleaning")
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Load Bronze Data
    bronze_path = "s3a://bronze/telco_churn_delta"
    silver_path = "s3a://silver/telco_churn_delta"

    print(f"Reading from: {bronze_path}")
    try:
        df = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        print(f"CRITICAL: Could not read Bronze data. {e}")
        sys.exit(1)

    # 3. Standardize Column Names (CamelCase -> snake_case)
    old_cols = df.columns
    new_cols = [to_snake_case(c) for c in old_cols]
    df = df.toDF(*new_cols)
    print("--- Column Names Standardized (snake_case) ---")

    # 4. Standardize Binary Columns
    # Group A: String "Yes"/"No" -> Convert to Integer 1/0
    string_binary_cols = [
        "partner",
        "dependents",
        "phone_service",
        "paperless_billing",
        "churn",
    ]

    # Group B: Numeric binaries (e.g., SeniorCitizen) -> Force to Integer
    numeric_binary_cols = ["senior_citizen"]

    print("--- Standardizing Binary Flags (Yes/No -> 1/0) ---")
    for c in string_binary_cols:
        if c in df.columns:
            # Logic: Yes=1, No=0, Anything else=NULL (Safety)
            df = df.withColumn(
                c, when(col(c) == "Yes", 1).when(col(c) == "No", 0).otherwise(None)
            ).withColumn(c, col(c).cast(IntegerType()))

    print("--- Enforcing Integer Type for Numeric Flags ---")
    for c in numeric_binary_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    # 5. Fix Total Charges (The Critical Fix)
    if "total_charges" in df.columns:
        print("--- Cleaning Total Charges (Filling Nulls with 0.0) ---")
        # 1. Cast to Double (It should already be double from Bronze, but this enforces it)
        # 2. Fill NA with 0.0
        df = df.withColumn(
            "total_charges", col("total_charges").cast(DoubleType())
        ).fillna(0.0, subset=["total_charges"])

    # 6. FIX: Handling Unusual Casing for customer_i_d and streaming_t_v
    df = df.withColumnRenamed("customer_i_d", "customer_id").withColumnRenamed(
        "streaming_t_v", "streaming_tv"
    )

    # 7. Deduplication
    # We drop duplicates based on customer_id just in case ingestion ran twice
    original_count = df.count()
    df = df.dropDuplicates(["customer_id"])
    final_count = df.count()
    if original_count > final_count:
        print(f"--- Removed {original_count - final_count} Duplicate Rows ---")

    # 8. Write to Silver
    print(f"--- Writing {final_count} rows to Silver Layer ({silver_path}) ---")
    df.write.format("delta").mode("overwrite").save(silver_path)

    print("--- [SUCCESS] Silver Layer Processing Complete ---")
    spark.stop()


if __name__ == "__main__":
    run_cleaning()
