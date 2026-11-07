"""Walidacja danych przed wysyłką. Błąd zatrzymuje pipeline zanim zepsute dane trafią do S3."""
import pandas as pd

from ingestion.config import REQUIRED_COLUMNS


class ValidationError(Exception):
    pass


def validate_row_count(table: str, actual: int, expected: int | None) -> None:
    if actual == 0:
        raise ValidationError(f"{table}: tabela jest pusta")
    if expected is not None and actual != expected:
        raise ValidationError(
            f"{table}: pobrano {actual} wierszy, a Supabase zgłasza {expected}"
        )


def validate_schema(table: str, df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.get(table, set()) - set(df.columns)
    if missing:
        raise ValidationError(f"{table}: brakuje kolumn {sorted(missing)}")