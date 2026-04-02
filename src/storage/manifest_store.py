from __future__ import annotations

import sqlite3
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
        if not self.manifest_path.exists():
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.manifest_path, timeout=30.0)
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA synchronous=NORMAL;")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS manifest (
                    file_path TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    status TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    row_count_in INTEGER NOT NULL,
                    row_count_out INTEGER NOT NULL,
                    error TEXT,
                    run_id TEXT NOT NULL PRIMARY KEY
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manifest_file_checksum_status
                ON manifest (file_path, checksum, status)
                """
            )

    def load(self) -> pl.DataFrame:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    file_path, checksum, status, processed_at,
                    row_count_in, row_count_out, error, run_id
                FROM manifest
                ORDER BY processed_at ASC
                """
            ).fetchall()
        if not rows:
            return pl.DataFrame(schema=MANIFEST_SCHEMA)
        return pl.DataFrame(
            rows,
            schema=[
                "file_path",
                "checksum",
                "status",
                "processed_at",
                "row_count_in",
                "row_count_out",
                "error",
                "run_id",
            ],
            orient="row",
        )

    def has_success(self, file_path: Path, checksum: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM manifest
                WHERE file_path = ? AND checksum = ? AND status = 'success'
                LIMIT 1
                """,
                (str(file_path), checksum),
            ).fetchone()
        return row is not None

    def append_record_atomic(self, record: ManifestRecord) -> None:
        row = record.to_row()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO manifest (
                    file_path, checksum, status, processed_at,
                    row_count_in, row_count_out, error, run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["file_path"],
                    row["checksum"],
                    row["status"],
                    row["processed_at"],
                    row["row_count_in"],
                    row["row_count_out"],
                    row["error"],
                    row["run_id"],
                ),
            )

