"""
ecommerce_data_pipeline

Supabase -> parquet (send_to_s3/) -> S3 (archive/ + raw/) -> Databricks Silver -> Gold

Logika ekstrakcji i walidacji jest w pakiecie `ingestion`, a DAG tylko orkiestruje kroki.
"""
import logging
import os
import shutil
from datetime import timedelta
from pathlib import Path

import boto3
import pendulum
from botocore.exceptions import BotoCoreError, ClientError

from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag, get_current_context, task

from ingestion.config import TABLES, settings
from ingestion.supabase.extract import extract_table as extract_to_parquet
from ingestion.supabase.validation import ValidationError

log = logging.getLogger(__name__)

TZ = "Europe/Warsaw"
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data"))
SEND_DIR = DATA_DIR / "send_to_s3"
SENT_DIR = DATA_DIR / "sent"

# Klucz do stabilnej paginacji, np. {"orders": "order_id"}
ORDER_BY: dict[str, str] = {}


def _job_id(env_name: str) -> int:
    """Brak id joba to błąd konfiguracji. Wychodzi od razu przy parsowaniu DAG-a, a nie jako job 0 w runie."""
    value = os.getenv(env_name)
    if not value:
        raise ValueError(f"Brakuje {env_name} w .env")
    return int(value)


def _run_timestamp() -> pendulum.DateTime:
    """Jeden znacznik czasu na cały run - wszystkie tabele dostają tę samą wersję."""
    dag_run = get_current_context().get("dag_run")
    started = getattr(dag_run, "start_date", None) or pendulum.now("UTC")
    return pendulum.instance(started).in_timezone(TZ)


@dag(
    dag_id="ecommerce_data_pipeline",
    description="Supabase -> S3 RAW -> Databricks Silver -> Gold",
    start_date=pendulum.datetime(2026, 10, 31, tz=TZ),
    schedule="0 14 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ecommerce", "supabase", "databricks", "aws", "etl"],
)
def ecommerce_data_pipeline():

    @task
    def extract(table: str) -> dict:
        ts = _run_timestamp()
        try:
            path = extract_to_parquet(table, SEND_DIR, ts, order_by=ORDER_BY.get(table))
        except ValidationError as e:
            raise AirflowFailException(str(e)) from e
        return {"table": table, "path": str(path), "dt": ts.format("YYYY-MM-DD")}

    @task
    def upload(item: dict) -> dict:
        cfg = settings()
        path = Path(item["path"])
        if not path.exists():
            raise AirflowFailException(f"Brak pliku: {path}")

        s3 = boto3.client("s3", region_name=cfg["aws_region"])
        bucket = cfg["s3_bucket"]
        keys = [
            f"archive/{item['table']}/dt={item['dt']}/{path.name}",  # historia
            f"raw/{item['table']}.parquet",                           # najnowsza wersja
        ]
        size = path.stat().st_size

        try:
            for key in keys:
                s3.upload_file(str(path), bucket, key)
                remote = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
                if remote != size:
                    raise AirflowFailException(f"{key}: rozmiar {remote} != {size}")
                log.info("Wysłano s3://%s/%s", bucket, key)
        except (ClientError, BotoCoreError) as e:
            raise AirflowFailException(f"Upload {path.name} nie powiódł się: {e}") from e

        dest_dir = SENT_DIR / item["dt"]
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(dest_dir / path.name))
        return {**item, "s3_keys": keys}

    silver_task = DatabricksRunNowOperator(
        task_id="process_silver",
        databricks_conn_id="databricks_default",
        job_id=_job_id("DATABRICKS_SILVER_JOB_ID"),
    )
    gold_task = DatabricksRunNowOperator(
        task_id="process_gold",
        databricks_conn_id="databricks_default",
        job_id=_job_id("DATABRICKS_GOLD_JOB_ID"),
    )

    uploaded = upload.expand(item=extract.expand(table=TABLES))
    uploaded >> silver_task >> gold_task


ecommerce_data_pipeline()