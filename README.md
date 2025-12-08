# Telco Customer Churn Pipeline: End-to-End MLOps

An enterprise-grade MLOps pipeline designed to predict customer churn. This project demonstrates a full **Medallion Architecture**, **Feature Store implementation**, **Data Validation**, and **Model Training**, orchestrated entirely via **Apache Airflow** and **Docker**.

---

## Architecture & Design

This project follows the **Medallion Architecture** pattern to progressively improve data quality, integrated with a modern Feature Store for ML readiness.

```mermaid
graph LR
    Raw(Raw Excel) -->|Ingest| Bronze[(Bronze: Delta)]
    Bronze -->|Clean & Standardize| Silver[(Silver: Delta)]
    Silver -->|Feature Eng & OHE| Gold[(Gold: Parquet)]
    Gold -->|Validate| Quality{Quality Gate}
    Quality -->|Pass| Feast[(Feast Feature Store)]
    Feast -->|Materialize| Redis[(Redis: Online Store)]
    Feast -->|Retrieval| Training(Spark ML Model)
```

### **Key Design Choices**

1.  **Spark for All Layers:** We use PySpark for Bronze, Silver, and Gold layers to ensure the pipeline is horizontally scalable, capable of handling terabytes of data, not just the sample CSV.
2.  **Delta Lake for Bronze/Silver:** We use Delta format for early layers to leverage ACID transactions, schema enforcement, and audit history (Time Travel).
3.  **Parquet for Gold:** The Gold layer is stored as Parquet to serve as the "Offline Store" for **Feast**. This ensures native compatibility without complex connectors.
4.  **Feast as the Serving Layer:** Instead of a static database, we use Feast to decouple _Feature Engineering_ (Spark) from _Feature Serving_ (Redis/Model).This prevents training-serving skew.
5.  **Strict Validation Gate:** A dedicated validation step ensures no corrupted data enters the Feature Store, protecting downstream models from silent failures.

---

## Tech Stack

- **Compute:** Apache Spark 3.5 (PySpark)
- **Orchestration:** Apache Airflow 2.7
- **Storage:** MinIO (S3 Compatible Object Storage)
- **Formats:** Delta Lake (Bronze/Silver), Parquet (Gold)
- **Feature Store:** Feast 0.36
- **Online Store:** Redis 7.0
- **Containerization:** Docker & Docker Compose

---

## Setup & Usage

### 1\. Prerequisites

- Docker Desktop installed (4GB+ RAM recommended).

### 2\. Infrastructure Startup

Start the entire stack (Spark, Airflow, MinIO, Redis):

```bash
docker compose up -d
```

### 4\. Run the Pipeline

1.  Navigate to Airflow: `http://localhost:8080` (User/Pass: `airflow`/`airflow`).
2.  Enable the `01_etl` DAG.
3.  Trigger the DAG manually.

### 5\. Verify Results

- **Airflow:** All tasks (Ingest -\> Clean -\> Engineer -\> Validate -\> Feast -\> Train) should be Dark Green.
- **Model:** Check the logs of the `train_model` task to see the Accuracy score.
- **Feature Store:** Run the exploration script to see data fetched from Redis:
  ```bash
  docker exec -it telco_spark_1 python3 /app/src/explore_feature_store.py
  ```

---

## Data Lineage & Tracking

- **Process Lineage:** Handled by Airflow DAGs.
- **Data Lineage:** Handled by Delta Lake Transaction Logs (`_delta_log`).
- **Feature Lineage:** Handled by Feast Registry, linking Models -\> Features -\> Gold Files.
