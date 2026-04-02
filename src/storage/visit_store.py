from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl


class VisitStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA synchronous=NORMAL;")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS visits (
                    run_id TEXT NOT NULL,
                    visit_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    visit_kind TEXT NOT NULL,
                    stay_type TEXT,
                    start_ts_utc TEXT NOT NULL,
                    end_ts_utc TEXT NOT NULL,
                    duration_seconds INTEGER NOT NULL,
                    ping_count INTEGER NOT NULL,
                    representative_latitude REAL NOT NULL,
                    representative_longitude REAL NOT NULL,
                    source_file TEXT NOT NULL,
                    source_checksum TEXT NOT NULL,
                    persisted_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, visit_id)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_visits_device_start
                ON visits (device_id, start_ts_utc)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS visit_lineage (
                    run_id TEXT NOT NULL PRIMARY KEY,
                    source_file TEXT NOT NULL,
                    source_checksum TEXT NOT NULL,
                    algorithm TEXT NOT NULL,
                    total_in INTEGER NOT NULL,
                    accepted_count INTEGER NOT NULL,
                    rejected_count INTEGER NOT NULL,
                    visits_count INTEGER NOT NULL,
                    accepted_path TEXT NOT NULL,
                    rejected_path TEXT NOT NULL,
                    visits_path TEXT NOT NULL,
                    report_path TEXT,
                    persisted_at TEXT NOT NULL
                )
                """
            )

    def persist_visits_with_lineage(
        self,
        *,
        run_id: str,
        source_file: str,
        source_checksum: str,
        algorithm: str,
        total_in: int,
        accepted_count: int,
        rejected_count: int,
        accepted_path: Path,
        rejected_path: Path,
        visits_path: Path,
        visits_df: pl.DataFrame,
    ) -> dict[str, Any]:
        persisted_at = datetime.now(UTC).isoformat()
        rows = visits_df.iter_rows(named=True)

        visit_rows = [
            (
                run_id,
                str(row["visit_id"]),
                str(row["device_id"]),
                str(row["visit_kind"]),
                str(row["stay_type"]) if row["stay_type"] is not None else None,
                str(row["start_ts_utc"]),
                str(row["end_ts_utc"]),
                int(row["duration_seconds"]),
                int(row["ping_count"]),
                float(row["representative_latitude"]),
                float(row["representative_longitude"]),
                source_file,
                source_checksum,
                persisted_at,
            )
            for row in rows
        ]

        with self._connect() as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO visits (
                    run_id, visit_id, device_id, visit_kind, stay_type,
                    start_ts_utc, end_ts_utc, duration_seconds, ping_count,
                    representative_latitude, representative_longitude,
                    source_file, source_checksum, persisted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                visit_rows,
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO visit_lineage (
                    run_id, source_file, source_checksum, algorithm,
                    total_in, accepted_count, rejected_count, visits_count,
                    accepted_path, rejected_path, visits_path, report_path, persisted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source_file,
                    source_checksum,
                    algorithm,
                    total_in,
                    accepted_count,
                    rejected_count,
                    visits_df.height,
                    str(accepted_path),
                    str(rejected_path),
                    str(visits_path),
                    None,
                    persisted_at,
                ),
            )

        return {
            "db_path": str(self.db_path),
            "lineage_written": True,
            "visits_written": visits_df.height,
        }

    def attach_report_path(self, run_id: str, report_path: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE visit_lineage
                SET report_path = ?
                WHERE run_id = ?
                """,
                (str(report_path), run_id),
            )

