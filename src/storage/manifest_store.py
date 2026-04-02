from __future__ import annotations

from pathlib import Path

import polars as pl

from src.domain.models import ManifestRecord

MANIFEST_SCHEMA = {
    "file_path": pl.String,
    "checksum": pl.String,
    "status": pl.String,
    "processed_at": pl.String,
    "row_count_in": pl.Int64,
    "row_count_out": pl.Int64,
    "error": pl.String,
    "run_id": pl.String,
}


class ManifestStore:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> pl.DataFrame:
        if not self.manifest_path.exists():
            return pl.DataFrame(schema=MANIFEST_SCHEMA)
        return pl.read_parquet(self.manifest_path)

    def has_success(self, file_path: Path, checksum: str) -> bool:
        df = self.load()
        if df.height == 0:
            return False
        matched = df.filter(
            (pl.col("file_path") == str(file_path))
            & (pl.col("checksum") == checksum)
            & (pl.col("status") == "success")
        )
        return matched.height > 0

    def append_record_atomic(self, record: ManifestRecord) -> None:
        current = self.load()
        updated = pl.concat([current, pl.DataFrame([record.to_row()])], how="vertical_relaxed")
        temp_path = self.manifest_path.with_suffix(".tmp.parquet")
        updated.write_parquet(temp_path)
        temp_path.replace(self.manifest_path)

