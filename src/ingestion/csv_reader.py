from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

REQUIRED_COLUMNS = ("device_id", "ts_utc", "latitude", "longitude")
OPTIONAL_COLUMNS = ("accuracy_m",)
ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


def normalize_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    text = str(value).strip()
    if text == "":
        return None

    if text.isdigit():
        raw_int = int(text)
        # Heuristic: values above 10^11 are in milliseconds.
        seconds = raw_int / 1000 if raw_int > 10**11 else raw_int
        return datetime.fromtimestamp(seconds, tz=UTC)

    iso_candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _read_raw_csv_primary(file_path: Path) -> tuple[pl.DataFrame, int]:
    df = pl.read_csv(file_path, infer_schema_length=2000, ignore_errors=False)
    row_count_in = df.height

    missing_required = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_required:
        missing = ", ".join(missing_required)
        raise ValueError(f"Missing required columns: {missing}")

    selected = []
    for column in ALL_COLUMNS:
        if column in df.columns:
            selected.append(pl.col(column))
        else:
            selected.append(pl.lit(None).alias(column))

    ts_text = pl.col("ts_utc").cast(pl.String, strict=False).str.strip_chars()
    ts_clean = pl.when(ts_text == "").then(pl.lit(None, dtype=pl.String)).otherwise(ts_text)

    is_digits = ts_clean.str.contains(r"^\d+$", literal=False)

    epoch_seconds = (
        pl.when(ts_clean.str.contains(r"^\d{1,10}$", literal=False))
        .then(pl.from_epoch(ts_clean.cast(pl.Int64, strict=False), time_unit="s"))
        .otherwise(pl.lit(None, dtype=pl.Datetime))
        .dt.replace_time_zone("UTC")
    )
    epoch_millis = (
        pl.when(ts_clean.str.contains(r"^\d{11,}$", literal=False))
        .then(pl.from_epoch(ts_clean.cast(pl.Int64, strict=False), time_unit="ms"))
        .otherwise(pl.lit(None, dtype=pl.Datetime))
        .dt.replace_time_zone("UTC")
    )
    iso_fallback = (
        pl.when(is_digits.fill_null(False))
        .then(pl.lit(None, dtype=pl.Datetime(time_zone="UTC")))
        .otherwise(
            ts_clean.map_elements(normalize_timestamp, return_dtype=pl.Datetime(time_zone="UTC"))
        )
    )

    normalized = (
        df.select(selected)
        .with_columns(
            [
                pl.col("device_id").cast(pl.String),
                pl.col("latitude").cast(pl.Float64, strict=False),
                pl.col("longitude").cast(pl.Float64, strict=False),
                pl.col("accuracy_m").cast(pl.Float64, strict=False),
                pl.coalesce([epoch_millis, epoch_seconds, iso_fallback]).alias("ts_utc"),
            ]
        )
        .with_columns(pl.col("ts_utc").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("ts_utc"))
    )

    return normalized, row_count_in


def _read_raw_csv_alternative(file_path: Path) -> tuple[pl.DataFrame, int]:
    df = pl.read_csv(file_path, infer_schema_length=2000, ignore_errors=False)
    row_count_in = df.height

    missing_required = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_required:
        missing = ", ".join(missing_required)
        raise ValueError(f"Missing required columns: {missing}")

    selected = []
    for column in ALL_COLUMNS:
        if column in df.columns:
            selected.append(pl.col(column))
        else:
            selected.append(pl.lit(None).alias(column))

    normalized = (
        df.select(selected)
        .with_columns(
            [
                pl.col("device_id").cast(pl.String),
                pl.col("latitude").cast(pl.Float64, strict=False),
                pl.col("longitude").cast(pl.Float64, strict=False),
                pl.col("accuracy_m").cast(pl.Float64, strict=False),
                pl.col("ts_utc")
                .map_elements(normalize_timestamp, return_dtype=pl.Datetime(time_zone="UTC"))
                .alias("ts_utc"),
            ]
        )
        .with_columns(pl.col("ts_utc").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("ts_utc"))
    )
    return normalized, row_count_in


def read_raw_csv(
    file_path: Path,
    algorithm: Literal["primary", "alternative"] = "primary",
) -> tuple[pl.DataFrame, int]:
    if algorithm == "primary":
        return _read_raw_csv_primary(file_path)
    return _read_raw_csv_alternative(file_path)

