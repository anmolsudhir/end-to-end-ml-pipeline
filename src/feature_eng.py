from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Bucketizer
from pyspark.ml import Pipeline
from puyspark.ml.functions import vector_to_array
from pyspark.sql.functions import current_timestamp, col, when, lit
import sys


def run_feature_engineering():
    print("--- [GOLD] Starting Feature Engineering Pipeline ---")

    # 1. Setup Spark (MinIO Configured)
    builder = (
        SparkSession.builder.appName("GoldFeatureEng")
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

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Read Silver Data (Delta Format)
    silver_path = "s3a://silver/telco_churn_delta"
    try:
        print(f"Reading Silver data from {silver_path}...")
        df = spark.read.format("delta").load(silver_path)
    except Exception as e:
        print(f"CRITICAL: Could not read Silver data. {e}")
        sys.exit(1)

    # ==========================================
    # PART A: DERIVED FEATURES (Project Requirements)
    # ==========================================
    print("--- Generating Derived Features (Math & Logic) ---")

    # 1. Tenure Group [Requirement: 0-12, 12-24, etc.]
    # We use Bucketizer to split raw tenure into categorical buckets.
    # Splits: 0-12 (New), 12-24 (Regular), 24-48 (Loyal), 48+ (Very Loyal)
    bucketizer = Bucketizer(
        splits=[0, 12, 24, 48, float("inf")],
        inputCol="tenure",
        outputCol="tenure_group",
    )
    df = bucketizer.transform(df)

    # 2. Billing Ratio [Requirement: MonthlyCharges/BillingCycleRatio]
    # Logic: Compare Current Monthly Charge to Historical Average.
    # If Ratio > 1.0, the customer's bill has increased over time (Price Hike).

    # Step A: Calculate average historical charge (Handle divide-by-zero for new customers)
    df = df.withColumn(
        "avg_historical_charge",
        when(col("tenure") == 0, col("monthly_charges")).otherwise(
            col("total_charges") / col("tenure")
        ),
    )

    # Step B: Calculate the ratio
    df = df.withColumn(
        "billing_ratio", col("monthly_charges") / col("avg_historical_charge")
    )

    # 3. High Value Flag (Bonus Feature)
    # Identifies customers who are both Loyal (>24 months) AND High Spenders (>$80)
    df = df.withColumn(
        "is_high_value",
        when((col("tenure") > 24) & (col("monthly_charges") > 80), 1).otherwise(0),
    )

    # ==========================================
    # PART B: CATEGORICAL ENCODING (ML Readiness)
    # ==========================================
    print("--- Running One-Hot Encoding Pipeline ---")

    # List of String columns to encode
    # Note: We skip 'customer_id' (Identifier) and Binary cols (already 0/1)
    categorical_cols = [
        "gender",
        "multiple_lines",
        "internet_service",
        "online_security",
        "online_backup",
        "device_protection",
        "tech_support",
        "streaming_tv",
        "streaming_movies",
        "contract",
        "payment_method",
    ]

    stages = []
    for c in categorical_cols:
        # 1. StringIndexer: Converts "Fiber Optic" -> 1.0 (Index)
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        # 2. OneHotEncoder: Converts 1.0 -> [0, 1, 0] (Vector)
        encoder = OneHotEncoder(
            inputCol=f"{c}_idx", outputCol=f"{c}_vec", dropLast=False
        )
        stages += [indexer, encoder]

    # Run the Pipeline
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df)
    df_transformed = model.transform(df)

    # ==========================================
    # PART C: CLEANUP & FEAST PREP
    # ==========================================

    # Drop intermediate columns to keep the Feature Store clean
    # We remove the raw strings and temporary indexes, keeping only the final Vectors + Raw Numerics
    # 1. Convert Spark Vectors to Standard Arrays (Crucial for Feast/Pandas)
    vec_cols = [c for c in df_transformed.columns if c.endswith("_vec")]
    for c in vec_cols:
        # distinct=True converts to a dense array (e.g. [0.0, 1.0, 0.0])
        df_transformed = df_transformed.withColumn(c, vector_to_array(col(c)))

    # 2. Drop raw categorical columns and index columns
    cols_to_drop = categorical_cols + [f"{c}_idx" for c in categorical_cols]
    df_final = df_transformed.drop(*cols_to_drop)

    print("--- Adding Feast Timestamps ---")
    # Feast requires an event timestamp to manage Point-in-Time correctness
    df_final = df_final.withColumn("event_timestamp", current_timestamp()).withColumn(
        "created_timestamp", current_timestamp()
    )

    # ==========================================
    # PART D: SAVE TO GOLD (Parquet)
    # ==========================================
    gold_path = "s3a://gold/telco_features_parquet"
    print(f"--- Writing {df_final.count()} feature rows to {gold_path} ---")

    # Write as Parquet (Feast's preferred format for Offline Store)
    df_final.write.mode("overwrite").parquet(gold_path)

    print("--- [SUCCESS] Gold Layer Feature Engineering Complete ---")
    spark.stop()


if __name__ == "__main__":
    run_feature_engineering()
