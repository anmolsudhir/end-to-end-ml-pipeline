from feast import Entity, Field, FeatureView, FileSource, ValueType
from feast.types import Float32, Int64, Array
from datetime import timedelta

# 1. Define Entity
customer = Entity(
    name="customer_id",
    value_type=ValueType.STRING,
    description="Unique ID of the telco customer",
)

# 2. Define Source
gold_source = FileSource(
    path="s3://gold/telco_features_parquet",
    s3_endpoint_override="http://minio:9000",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
    timestamp_field="event_timestamp",
)

# 3. Define Feature View (THE COMPLETE LIST)
churn_stats = FeatureView(
    name="churn_stats",
    entities=[customer],
    ttl=timedelta(days=3650),
    schema=[
        # --- A. Derived Features (New) ---
        Field(name="tenure_group", dtype=Float32),
        Field(name="billing_ratio", dtype=Float32),
        Field(name="is_high_value", dtype=Int64),
        Field(name="avg_historical_charge", dtype=Float32),
        # --- B. Raw Numerics ---
        Field(name="monthly_charges", dtype=Float32),
        Field(name="total_charges", dtype=Float32),
        Field(name="tenure", dtype=Float32),
        # --- C. Simple Binary Features (The Missing Ones!) ---
        # These are already 0/1 integers from the Silver Layer
        Field(name="senior_citizen", dtype=Int64),
        Field(name="partner", dtype=Int64),
        Field(name="dependents", dtype=Int64),
        Field(name="phone_service", dtype=Int64),
        Field(name="paperless_billing", dtype=Int64),
        # We also include the Target (Churn) so we can easily retrieve training data
        Field(name="churn", dtype=Int64),
        # --- D. Encoded Vectors (Arrays) ---
        Field(name="gender_vec", dtype=Array(Float32)),
        Field(name="internet_service_vec", dtype=Array(Float32)),
        Field(name="contract_vec", dtype=Array(Float32)),
        Field(name="payment_method_vec", dtype=Array(Float32)),
        Field(name="multiple_lines_vec", dtype=Array(Float32)),
        Field(name="online_security_vec", dtype=Array(Float32)),
        Field(name="online_backup_vec", dtype=Array(Float32)),
        Field(name="device_protection_vec", dtype=Array(Float32)),
        Field(name="tech_support_vec", dtype=Array(Float32)),
        Field(name="streaming_tv_vec", dtype=Array(Float32)),
        Field(name="streaming_movies_vec", dtype=Array(Float32)),
    ],
    online=True,
    source=gold_source,
    tags={"project": "telco_churn"},
)
