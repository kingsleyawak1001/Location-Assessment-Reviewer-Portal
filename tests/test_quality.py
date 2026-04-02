from __future__ import annotations

import polars as pl

from src.quality.validator import quality_counts_by_reason, validate_quality


def test_coordinate_validation_rejects_out_of_range() -> None:
    df = pl.DataFrame(
        {
            "device_id": ["dev_a"],
            "ts_utc": ["2025-01-01T00:00:00Z"],
            "latitude": [100.0],
            "longitude": [20.0],
            "accuracy_m": [1.0],
        }
    )
    accepted, rejected = validate_quality(df)
    assert accepted.height == 0
    assert rejected.height == 1
    assert "INVALID_LATITUDE_RANGE" in str(rejected["reject_reason"][0])


def test_exact_duplicate_detection() -> None:
    df = pl.DataFrame(
        {
            "device_id": ["d1", "d1"],
            "ts_utc": ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
            "latitude": [10.0, 10.0],
            "longitude": [20.0, 20.0],
            "accuracy_m": [5.0, 5.0],
        }
    )
    accepted, rejected = validate_quality(df)
    assert accepted.height == 0
    assert rejected.height == 2
    counts = quality_counts_by_reason(rejected)
    assert counts["EXACT_DUPLICATE"] == 2

