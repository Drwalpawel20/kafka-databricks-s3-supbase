"""Konsument Kafka: czyta topic 'orders', odrzuca duplikaty i zapisuje partie do warstwy bronze (parquet)."""
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer

from ingestion.config import settings
from ingestion.kafka.deduplication import DedupStore

log = logging.getLogger(__name__)

BATCH_SIZE = 500
FLUSH_SECONDS = 30


def to_bronze_row(event: dict) -> dict:
    return {
        **event["data"],
        "_event_id": event["event_id"],
        "_source_file": event["source"]["file"],
        "_source_etag": event["source"]["etag"],
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_batch(rows: list[dict], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
    path = out_dir / f"orders_{stamp}.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def run() -> None:
    cfg = settings()
    consumer = KafkaConsumer(
        cfg["kafka_topic"],
        bootstrap_servers=cfg["kafka_bootstrap"],
        group_id=cfg["consumer_group"],
        enable_auto_commit=False,      # offset commitujemy dopiero po zapisie
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    store = DedupStore(cfg["dedup_db"])
    bronze_dir = Path(cfg["bronze_dir"]) / "orders"

    buffer: list[dict] = []
    seen: set[str] = set()  # duplikaty wewnątrz tej samej partii
    last_flush = time.monotonic()
    log.info("Konsument uruchomiony, topic=%s", cfg["kafka_topic"])

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            for records in polled.values():
                for msg in records:
                    event = msg.value
                    event_id = event["event_id"]
                    if event_id in seen or store.is_processed(event_id):
                        continue
                    seen.add(event_id)
                    buffer.append(to_bronze_row(event))

            time_due = time.monotonic() - last_flush >= FLUSH_SECONDS
            if buffer and (len(buffer) >= BATCH_SIZE or time_due):
                path = write_batch(buffer, bronze_dir)
                store.mark_processed(list(seen))  # najpierw zapis, potem oznaczenie
                consumer.commit()                 # i dopiero na końcu commit offsetów
                log.info("Zapisano %d rekordów -> %s", len(buffer), path)
                buffer, seen = [], set()
                last_flush = time.monotonic()
    finally:
        store.close()
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()