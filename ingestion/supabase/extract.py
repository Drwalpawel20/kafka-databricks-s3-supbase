"""Ekstrakcja tabel z Supabase: paginacja, kontrola liczby wierszy, zapis do parquet."""
import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from supabase import Client, create_client

from ingestion.config import TABLES, settings
from ingestion.supabase.validation import validate_row_count, validate_schema

log = logging.getLogger(__name__)
PAGE_SIZE = 1000  # Supabase zwraca max 1000 wierszy na zapytanie


def get_client() -> Client:
    cfg = settings()
    if not cfg["supabase_url"] or not cfg["supabase_key"]:
        raise ValueError("Brakuje SUPABASE_URL lub SUPABASE_KEY")
    return create_client(cfg["supabase_url"], cfg["supabase_key"])


def fetch_all_rows(client, table: str, page_size: int = PAGE_SIZE, order_by: str | None = None):
    """Pobiera wszystkie wiersze stronami. Zwraca (wiersze, liczba wierszy według Supabase)."""

    def fetch_page(offset: int, with_count: bool = False):
        query = client.table(table).select("*", count="exact" if with_count else None)
        if order_by:
            query = query.order(order_by)  # stabilna paginacja wymaga stałej kolejności
        return query.range(offset, offset + page_size - 1).execute()

    first = fetch_page(0, with_count=True)
    rows = list(first.data)
    expected = first.count

    last_len, offset = len(rows), page_size
    while last_len == page_size:
        batch = fetch_page(offset).data
        rows.extend(batch)
        last_len = len(batch)
        offset += page_size

    return rows, expected


def extract_table(table: str, out_dir: Path, run_ts: datetime, order_by: str | None = None) -> Path:
    """Pobiera tabelę, waliduje ją i zapisuje atomowo do <out_dir>/<tabela>_<znacznik>.parquet."""
    client = get_client()
    rows, expected = fetch_all_rows(client, table, order_by=order_by)
    validate_row_count(table, len(rows), expected)

    df = pd.DataFrame(rows)
    validate_schema(table, df)

    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = run_ts.strftime("%Y%m%d_%H%M%S")
    final = out_dir / f"{table}_{stamp}.parquet"
    tmp = final.with_name(f".{final.name}.tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(final)  

    log.info("Zapisano %s: %d wierszy -> %s", table, len(df), final)
    return final


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Lokalne pobranie tabel z Supabase")
    parser.add_argument("tables", nargs="*", default=TABLES)
    parser.add_argument("--out", default="data/send_to_s3", type=Path)
    args = parser.parse_args()
    for t in args.tables:
        extract_table(t, args.out, datetime.now())