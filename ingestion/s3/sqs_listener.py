"""Listener SQS: reaguje na zdarzenia S3 (nowy plik w raw/orders*.parquet) i wysyła rekordy do Kafki."""
import io
import json
import logging
import time
from typing import Iterator
from urllib.parse import unquote_plus

import boto3
import pandas as pd

from ingestion.config import settings
from ingestion.kafka.producer import OrdersProducer

log = logging.getLogger(__name__)

PREFIX = "raw/orders"
SUFFIX = ".parquet"


def load_parquet_records(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))
    # JSON zamienia NaN/NaT na null i ujednolica typy (np. numpy -> zwykłe liczby)
    return json.loads(df.to_json(orient="records", date_format="iso"))


def iter_s3_objects(body: dict) -> Iterator[tuple[str, str, str]]:
    # Wiadomość testowa z S3 nie ma 'Records' - po prostu nic nie zwraca
    for event in body.get("Records", []):
        if event.get("eventSource") != "aws:s3":
            continue
        bucket = event["s3"]["bucket"]["name"]
        obj = event["s3"]["object"]
        yield bucket, unquote_plus(obj["key"]), obj.get("eTag", "")


def handle_message(message: dict, s3, producer: OrdersProducer, expected_bucket: str) -> None:
    body = json.loads(message["Body"])
    for bucket, key, etag in iter_s3_objects(body):
        if bucket != expected_bucket or not key.startswith(PREFIX) or not key.endswith(SUFFIX):
            continue
        records = load_parquet_records(s3, bucket, key)
        producer.send_records(records, bucket, key, etag)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = settings()
    s3 = boto3.client("s3", region_name=cfg["aws_region"])
    sqs = boto3.client("sqs", region_name=cfg["aws_region"])
    queue_url = sqs.get_queue_url(QueueName=cfg["sqs_queue_name"])["QueueUrl"]
    producer = OrdersProducer()

    log.info("Nasłuch kolejki %s", cfg["sqs_queue_name"])
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                VisibilityTimeout=300,
            )
        except Exception:
            log.exception("Błąd odbioru z SQS")
            time.sleep(5)
            continue

        for message in resp.get("Messages", []):
            try:
                handle_message(message, s3, producer, cfg["s3_bucket"])
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            except Exception:
                # Wiadomość zostaje w SQS i wróci po VisibilityTimeout.
                # Ponowne wysłanie to bezpieczne, bo event_id jest deterministyczny.
                log.exception("Przetwarzanie nie powiodło się - wiadomość zostaje w SQS")


if __name__ == "__main__":
    main()