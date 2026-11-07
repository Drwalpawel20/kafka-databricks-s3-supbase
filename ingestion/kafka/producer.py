"""Producent Kafka: wysyła rekordy z plików parquet na topic 'orders'."""
import hashlib
import json
import logging

from kafka import KafkaProducer

from ingestion.config import settings

log = logging.getLogger(__name__)


def make_event_id(bucket: str, key: str, etag: str, index: int) -> str:
    """Deterministyczny identyfikator - to samo wejście zawsze daje ten sam event_id.
    Dzięki temu ponowne wysłanie tego samego pliku nie tworzy duplikatów (patrz deduplication.py)."""
    return hashlib.sha256(f"{bucket}:{key}:{etag}:{index}".encode()).hexdigest()


def build_message(bucket: str, key: str, etag: str, index: int, record: dict) -> tuple[str, dict]:
    event_id = make_event_id(bucket, key, etag, index)
    order_id = record.get("order_id")
    # Klucz wiadomości = order_id, więc zdarzenia jednego zamówienia trafiają w tę samą partycję
    message_key = str(order_id) if order_id is not None else event_id
    value = {
        "event_id": event_id,
        "source": {"bucket": bucket, "file": key, "etag": etag},
        "data": record,
    }
    return message_key, value


class OrdersProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        cfg = settings()
        self.topic = topic or cfg["kafka_topic"]
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers or cfg["kafka_bootstrap"],
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, default=str, allow_nan=False).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=20,
        )

    def send_records(self, records: list[dict], bucket: str, key: str, etag: str) -> int:
        futures = []
        for index, record in enumerate(records):
            msg_key, value = build_message(bucket, key, etag, index, record)
            futures.append(self._producer.send(self.topic, key=msg_key, value=value))
        for future in futures:
            future.get(timeout=30)  # czekamy na potwierdzenie każdej wiadomości
        self._producer.flush()
        log.info("Wysłano %d rekordów z s3://%s/%s", len(futures), bucket, key)
        return len(futures)

    def close(self) -> None:
        self._producer.close()