from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# ---- Config ----
SCRIPTS_DIR = r"A:\Firmable - Data Engineering Project\scripts"
VENV_PYTHON = os.path.join(SCRIPTS_DIR, "../.venv/Scripts/python.exe")

# ---- Default arguments ----
default_args = {
    "owner": "asaf",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---- DAG definition ----
with DAG(
    dag_id="firmable_etl_dag",
    description="End-to-end ETL for ABR + CommonCrawl → Unified Company Dataset",
    start_date=datetime(2025, 11, 4),
    schedule_interval=None,  # or "0 3 * * *" for daily
    catchup=False,
    default_args=default_args,
    tags=["firmable", "etl", "staging", "pyspark"],
) as dag:

    # --- Task 1: Extract ABR ---
    extract_abr = BashOperator(
        task_id="extract_abr",
        bash_command=(
            f'"{VENV_PYTHON}" "{SCRIPTS_DIR}/extract_abr.py" '
            '--source-uri "scripts/data/20251029_Public01.xml" '
            '--local-path "scripts/data/20251029_Public01.xml" '
            '--table "stg.abr_bulk" '
            '--batch-size 100 --save-raw'
        )
    )

    # --- Task 2: Extract Common Crawl ---
    extract_commoncrawl = BashOperator(
        task_id="extract_commoncrawl",
        bash_command=(
            f'"{VENV_PYTHON}" "{SCRIPTS_DIR}/extract_commoncrawl.py" '
            '--table "stg.common_crawl_raw" '
            '--batch-size 100'
        )
    )

    # --- Task 3: Transform and Unify ---
    pyspark_transform = BashOperator(
        task_id="pyspark_transformation",
        bash_command=f'"{VENV_PYTHON}" "{SCRIPTS_DIR}/pyspark_transformation.py"'
    )

    # --- Dependencies ---
    [extract_abr, extract_commoncrawl] >> pyspark_transform
