from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, min as spark_min, max as spark_max
import sys


def validate_gold_layer():
    print("--- [VALIDATION] Starting Gold Layer Quality Gate ---")

    # 1. Setup Spark
    builder = (
        SparkSession.builder.appName("GoldValidator")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Read the Gold Data (Parquet)
    gold_path = "s3a://gold/telco_features_parquet"
    try:
        print(f"Inspecting data at: {gold_path}")
        df = spark.read.parquet(gold_path)
    except Exception as e:
        print(f"CRITICAL: Could not read Gold data. {e}")
        sys.exit(1)

    errors = []
    total_count = df.count()
    print(f"--- Validating {total_count} rows ---")

    # ==========================================
    # CHECK 1: DATA VOLUME & COMPLETENESS
    # ==========================================
    # Sanity check: Did we lose too much data?
    if total_count < 1000:
        errors.append(
            f"Volume Error: Row count {total_count} is suspiciously low (Expected > 1000)."
        )

    # Feast Entity Check: Customer ID cannot be Null
    null_ids = df.filter(col("customer_id").isNull()).count()
    if null_ids > 0:
        errors.append(
            f"Completeness Error: Found {null_ids} rows with NULL customer_id."
        )

    # Feast Timestamp Check: event_timestamp is mandatory for Point-in-Time correctness
    if "event_timestamp" not in df.columns:
        errors.append("Schema Error: Missing mandatory column 'event_timestamp'.")
    else:
        null_ts = df.filter(col("event_timestamp").isNull()).count()
        if null_ts > 0:
            errors.append(
                f"Completeness Error: Found {null_ts} rows with NULL event_timestamp."
            )

    # ==========================================
    # CHECK 2: UNIQUENESS
    # ==========================================
    # Feature Stores require unique Entity IDs per timestamp.
    # Since we are doing a snapshot load, customer_id must be unique.
    distinct_count = df.select("customer_id").distinct().count()
    if distinct_count != total_count:
        duplicates = total_count - distinct_count
        errors.append(f"Uniqueness Error: Found {duplicates} duplicate customer IDs.")

    # ==========================================
    # CHECK 3: FEATURE ENGINEERING LOGIC
    # ==========================================
    # Validate 'Tenure Group': Should be bucket indices 0.0, 1.0, 2.0, 3.0
    if "tenure_group" in df.columns:
        invalid_buckets = df.filter(
            (col("tenure_group") < 0) | (col("tenure_group") > 4)
        ).count()
        if invalid_buckets > 0:
            errors.append(
                f"Logic Error: Found {invalid_buckets} rows with invalid 'tenure_group' indices."
            )
    else:
        errors.append("Schema Error: 'tenure_group' feature is missing.")

    # Validate 'Billing Ratio': Should be positive. Negative implies bad data math.
    if "billing_ratio" in df.columns:
        negative_ratios = df.filter(col("billing_ratio") < 0).count()
        if negative_ratios > 0:
            errors.append(
                f"Anomaly Error: Found {negative_ratios} rows with negative 'billing_ratio'."
            )
    else:
        errors.append("Schema Error: 'billing_ratio' feature is missing.")

    # ==========================================
    # CHECK 4: ENCODING VERIFICATION
    # ==========================================
    # Ensure One-Hot Encoded vectors exist (e.g., 'internet_service_vec')
    vec_cols = [c for c in df.columns if c.endswith("_vec")]
    if len(vec_cols) == 0:
        errors.append(
            "Schema Error: No One-Hot Encoded vector columns found (expected *_vec)."
        )

    # ==========================================
    # FINAL VERDICT
    # ==========================================
    if errors:
        print("\nVALIDATION FAILED. PIPELINE STOPPED.")
        print("Reasons:")
        for e in errors:
            print(f" - {e}")
        sys.exit(1)  # Fail the Airflow task
    else:
        print("\nVALIDATION PASSED. Data approved for Feature Store.")
        spark.stop()
        sys.exit(0)  # Success


if __name__ == "__main__":
    validate_gold_layer()
