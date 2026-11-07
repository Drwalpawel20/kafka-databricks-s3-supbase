"""Centralna konfiguracja. Wszystko pochodzi ze zmiennych środowiskowych (.env)."""
import os

# boto3 czyta AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY. Starsze nazwy z .env
# (aws_access / aws_secret) są przepisywane przy imporcie, żeby kontenery
# sqs-listener i Airflow nie zostały bez credentials.
for _old, _new in (("aws_access", "AWS_ACCESS_KEY_ID"), ("aws_secret", "AWS_SECRET_ACCESS_KEY")):
    if os.getenv(_old) and not os.getenv(_new):
        os.environ[_new] = os.environ[_old]

TABLES = ["customers", "orders", "order_items", "products", "payments"]

# Kolumny wymagane w każdej tabeli - walidacja schematu przed wysyłką dalej.
# Dostosuj do swojego schematu w Supabase.
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "customers": {"customer_id"},
    "orders": {"order_id", "customer_id"},
    "order_items": {"order_id", "product_id"},
    "products": {"product_id"},
    "payments": {"payment_id", "order_id"},
}


def settings() -> dict:
    """Odczyt konfiguracji przy każdym wywołaniu (nic nie jest czytane przy imporcie)."""
    return {
        "supabase_url": os.getenv("SUPABASE_URL"),
        "supabase_key": os.getenv("SUPABASE_KEY"),
        "aws_region": os.getenv("AWS_REGION", "eu-north-1"),
        "s3_bucket": os.getenv("S3_BUCKET", ""),
        "sqs_queue_name": os.getenv("SQS_QUEUE_NAME", "s3-orders-events"),
        "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "orders"),
        "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "orders-bronze-writer"),
        "bronze_dir": os.getenv("BRONZE_DIR", "/opt/data/bronze"),
        "dedup_db": os.getenv("DEDUP_DB_PATH", "/opt/data/dedup.sqlite"),
    }
