from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount
from pathlib import Path
import os

# --- CONFIGURATION ---
HOST_PROJECT_PATH = os.environ.get("HOST_PROJECT_PATH")

# Safety check
if not HOST_PROJECT_PATH:
    raise ValueError("HOST_PROJECT_PATH env var is missing! Check docker-compose.yaml")

# Network Name
DOCKER_NETWORK = Path(HOST_PROJECT_PATH).name + "_pipeline-network"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "catchup": False,
}

# Feast Environment Config
feast_env = {
    "AWS_ACCESS_KEY_ID": "minio",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "FEAST_S3_ENDPOINT_URL": "http://minio:9000",
    "FEAST_USAGE": "False",
}

# Mount Config for Task Containers
mounts_config = [
    # 1. CODE: Syncs local 'src' folder to the container
    Mount(source=f"{HOST_PROJECT_PATH}/src", target="/app/src", type="bind"),
    # 2. FEAST CONFIG: Syncs local 'features' folder
    Mount(source=f"{HOST_PROJECT_PATH}/features", target="/app/features", type="bind"),
    # 3. PERSISTENCE: Sync local 'data' folder
    Mount(source=f"{HOST_PROJECT_PATH}/data", target="/app/data", type="bind"),
]

with DAG("01_etl", default_args=default_args, schedule_interval="@once") as dag:

    # TASK 1: INGEST (Bronze)
    ingest_task = DockerOperator(
        task_id="ingest_bronze_layer",
        image="dag-spark:v1",
        api_version="auto",
        auto_remove=True,  # Clean up container after run
        # COMMAND: Run the script inside the container
        command="python3 /app/src/ingest.py",
        # NETWORK: Critical for talking to MinIO
        network_mode=DOCKER_NETWORK,
        # PERMISSIONS: Allow Docker-in-Docker
        docker_url="unix://var/run/docker.sock",
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # TASK 2: CLEAN (Silver)
    clean_task = DockerOperator(
        task_id="clean_silver",
        image="dag-spark:v1",  # Reuse the same image
        api_version="auto",
        auto_remove=True,
        command="python3 /app/src/clean.py",  # <--- Run the clean script
        network_mode=DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # TASK 3: FEATURE ENGINEERING (Gold)
    feature_eng_task = DockerOperator(
        task_id="feature_eng_bronze_layer",
        image="dag-spark:v1",
        api_version="auto",
        auto_remove=True,  # Clean up container after run
        # COMMAND: Run the script inside the container
        command="python3 /app/src/feature_eng.py",
        # NETWORK: Critical for talking to MinIO
        network_mode=DOCKER_NETWORK,
        # PERMISSIONS: Allow Docker-in-Docker
        docker_url="unix://var/run/docker.sock",
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # TASK 4: DATA VALIDATION
    data_validation_task = DockerOperator(
        task_id="validation_gold_layer",
        image="dag-spark:v1",
        api_version="auto",
        auto_remove=True,  # Clean up container after run
        # COMMAND: Run the script inside the container
        command="python3 /app/src/validate_gold_layer.py",
        # NETWORK: Critical for talking to MinIO
        network_mode=DOCKER_NETWORK,
        # PERMISSIONS: Allow Docker-in-Docker
        docker_url="unix://var/run/docker.sock",
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # Task 5: Feast Apply (Register Features)
    feast_apply = DockerOperator(
        task_id="feast_apply",
        image="dag-spark:v1",
        api_version="auto",
        auto_remove=True,
        # Assumes local 'features' folder is mounted to /app/features
        command="bash -c 'cd /app/features && feast apply'",
        network_mode=DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        environment=feast_env,
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # Task 6: Feast Materialize (Load Redis)
    # Loads data from 2020 up to NOW
    feast_materialize_task = DockerOperator(
        task_id="feast_materialize",
        image="dag-spark:v1",
        api_version="auto",
        auto_remove=True,
        command="bash -c 'cd /app/features && feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)'",
        network_mode=DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        environment=feast_env,
        force_pull=False,
        mount_tmp_dir=False,
        mounts=mounts_config,
    )

    # DEPENDENCY: Run Ingest, THEN Clean
    (
        ingest_task
        >> clean_task
        >> feature_eng_task
        >> data_validation_task
        >> feast_apply
        >> feast_materialize_task
    )
