import pandas as pd
import pytest

from ingestion.supabase.validation import (
    ValidationError,
    validate_row_count,
    validate_schema,
)


def test_row_count_match_passes():
    validate_row_count("orders", 10, 10)


def test_row_count_mismatch_raises():
    with pytest.raises(ValidationError):
        validate_row_count("orders", 9, 10)


def test_empty_table_raises():
    with pytest.raises(ValidationError):
        validate_row_count("orders", 0, None)


def test_missing_required_column_raises():
    df = pd.DataFrame({"order_id": [1]})  # brak customer_id
    with pytest.raises(ValidationError, match="customer_id"):
        validate_schema("orders", df)


def test_valid_schema_passes():
    df = pd.DataFrame({"order_id": [1], "customer_id": [7]})
    validate_schema("orders", df)
