from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.ingestion.csv_reader import normalize_timestamp, read_raw_csv


def test_normalize_timestamp_epoch_seconds() -> None:
    parsed = normalize_timestamp("1735689600")
    assert parsed == datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)


def test_normalize_timestamp_iso() -> None:
    parsed = normalize_timestamp("2025-01-01T00:00:00Z")
    assert parsed == datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)


def test_optional_accuracy_column_missing_is_supported() -> None:
    fixture = Path("tests/fixtures/pings_no_accuracy.csv")
    df, row_count_in = read_raw_csv(fixture)
    assert row_count_in == 2
    assert "accuracy_m" in df.columns
    assert df.get_column("accuracy_m").null_count() == 2


def test_missing_required_columns_raises() -> None:
    fixture = Path("tests/fixtures/pings_missing_required.csv")
    with pytest.raises(ValueError, match="Missing required columns"):
        read_raw_csv(fixture)

