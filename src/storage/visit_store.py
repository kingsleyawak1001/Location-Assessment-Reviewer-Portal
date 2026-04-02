"""SQLite visit storage for the assessment prototype.

This module implements the concrete database layer used by the prototype and provides:
- visit-level persistence (`visits` table),
- run-level lineage/audit metadata (`visit_lineage` table),
- materialized per-run analytics (`run_aggregates` table),
- query helpers used by CLI and API use-cases.

It is the implemented counterpart of `assessment.md` Part 2 (Design & Schema),
while remaining intentionally local/prototype-friendly (SQLite + bounded queries).
"""
from __future__ import annotations

import json
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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_aggregates (
                    run_id TEXT NOT NULL PRIMARY KEY,
                    total_visits INTEGER NOT NULL,
                    unique_devices INTEGER NOT NULL,
                    avg_duration_seconds REAL NOT NULL,
                    total_pings INTEGER NOT NULL,
                    counts_by_visit_kind_json TEXT NOT NULL,
                    counts_by_stay_type_json TEXT NOT NULL,
                    top_devices_json TEXT NOT NULL,
                    materialized_at TEXT NOT NULL
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

    def get_lineage_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    run_id,
                    source_file,
                    algorithm,
                    total_in,
                    accepted_count,
                    rejected_count,
                    visits_count,
                    persisted_at,
                    report_path
                FROM visit_lineage
                ORDER BY persisted_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "run_id": str(row[0]),
                "source_file": str(row[1]),
                "algorithm": str(row[2]),
                "total_in": int(row[3]),
                "accepted_count": int(row[4]),
                "rejected_count": int(row[5]),
                "visits_count": int(row[6]),
                "persisted_at": str(row[7]),
                "report_path": str(row[8]) if row[8] is not None else None,
            }
            for row in rows
        ]

    def get_visits_for_run(self, run_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    visit_id,
                    device_id,
                    visit_kind,
                    stay_type,
                    start_ts_utc,
                    end_ts_utc,
                    duration_seconds,
                    ping_count,
                    representative_latitude,
                    representative_longitude
                FROM visits
                WHERE run_id = ?
                ORDER BY start_ts_utc ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return [
            {
                "visit_id": str(row[0]),
                "device_id": str(row[1]),
                "visit_kind": str(row[2]),
                "stay_type": str(row[3]) if row[3] is not None else None,
                "start_ts_utc": str(row[4]),
                "end_ts_utc": str(row[5]),
                "duration_seconds": int(row[6]),
                "ping_count": int(row[7]),
                "representative_latitude": float(row[8]),
                "representative_longitude": float(row[9]),
            }
            for row in rows
        ]

    def get_run_analytics(self, run_id: str, top_devices_limit: int = 5) -> dict[str, Any]:
        with self._connect() as connection:
            totals_row = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_visits,
                    COUNT(DISTINCT device_id) AS unique_devices,
                    AVG(duration_seconds) AS avg_duration_seconds,
                    SUM(ping_count) AS total_pings
                FROM visits
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            by_kind_rows = connection.execute(
                """
                SELECT visit_kind, COUNT(*) AS count
                FROM visits
                WHERE run_id = ?
                GROUP BY visit_kind
                ORDER BY count DESC
                """,
                (run_id,),
            ).fetchall()
            by_stay_type_rows = connection.execute(
                """
                SELECT COALESCE(stay_type, 'null') AS stay_type, COUNT(*) AS count
                FROM visits
                WHERE run_id = ?
                GROUP BY COALESCE(stay_type, 'null')
                ORDER BY count DESC
                """,
                (run_id,),
            ).fetchall()
            top_devices_rows = connection.execute(
                """
                SELECT device_id, COUNT(*) AS visits_count
                FROM visits
                WHERE run_id = ?
                GROUP BY device_id
                ORDER BY visits_count DESC, device_id ASC
                LIMIT ?
                """,
                (run_id, top_devices_limit),
            ).fetchall()

        total_visits = (
            int(totals_row[0]) if totals_row is not None and totals_row[0] is not None else 0
        )
        unique_devices = (
            int(totals_row[1]) if totals_row is not None and totals_row[1] is not None else 0
        )
        avg_duration_seconds = (
            float(totals_row[2]) if totals_row is not None and totals_row[2] is not None else 0.0
        )
        total_pings = (
            int(totals_row[3]) if totals_row is not None and totals_row[3] is not None else 0
        )

        return {
            "run_id": run_id,
            "total_visits": total_visits,
            "unique_devices": unique_devices,
            "avg_duration_seconds": round(avg_duration_seconds, 3),
            "total_pings": total_pings,
            "counts_by_visit_kind": {str(row[0]): int(row[1]) for row in by_kind_rows},
            "counts_by_stay_type": {
                str(row[0]): int(row[1]) for row in by_stay_type_rows
            },
            "top_devices": [
                {"device_id": str(row[0]), "visits_count": int(row[1])} for row in top_devices_rows
            ],
        }

    def materialize_run_aggregate(self, run_id: str, top_devices_limit: int = 5) -> dict[str, Any]:
        analytics = self.get_run_analytics(run_id, top_devices_limit=top_devices_limit)
        materialized_at = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO run_aggregates (
                    run_id,
                    total_visits,
                    unique_devices,
                    avg_duration_seconds,
                    total_pings,
                    counts_by_visit_kind_json,
                    counts_by_stay_type_json,
                    top_devices_json,
                    materialized_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(analytics["total_visits"]),
                    int(analytics["unique_devices"]),
                    float(analytics["avg_duration_seconds"]),
                    int(analytics["total_pings"]),
                    json.dumps(analytics["counts_by_visit_kind"], sort_keys=True),
                    json.dumps(analytics["counts_by_stay_type"], sort_keys=True),
                    json.dumps(analytics["top_devices"]),
                    materialized_at,
                ),
            )
        return {
            "run_id": run_id,
            "materialized_at": materialized_at,
            "top_devices_limit": top_devices_limit,
            "total_visits": int(analytics["total_visits"]),
            "total_pings": int(analytics["total_pings"]),
        }

    def get_materialized_run_aggregate(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    run_id,
                    total_visits,
                    unique_devices,
                    avg_duration_seconds,
                    total_pings,
                    counts_by_visit_kind_json,
                    counts_by_stay_type_json,
                    top_devices_json,
                    materialized_at
                FROM run_aggregates
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "run_id": str(row[0]),
            "total_visits": int(row[1]),
            "unique_devices": int(row[2]),
            "avg_duration_seconds": float(row[3]),
            "total_pings": int(row[4]),
            "counts_by_visit_kind": json.loads(str(row[5])),
            "counts_by_stay_type": json.loads(str(row[6])),
            "top_devices": json.loads(str(row[7])),
            "materialized_at": str(row[8]),
        }

    def get_location_analytics(
        self,
        *,
        start_ts_utc: str,
        end_ts_utc: str,
        west: float,
        east: float,
        south: float,
        north: float,
        movement_type: str | None,
        min_visits: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return bounded geographic aggregates for heatmap-like analytics.

        Note:
        - For prototype simplicity this uses coarse pseudo-cells computed from
          representative coordinates. Production design can swap this for H3 cells
          without changing the external API contract.
        """
        where_clauses = [
            "start_ts_utc >= ?",
            "end_ts_utc <= ?",
            "representative_longitude BETWEEN ? AND ?",
            "representative_latitude BETWEEN ? AND ?",
        ]
        params: list[Any] = [start_ts_utc, end_ts_utc, west, east, south, north]
        if movement_type is not None:
            where_clauses.append("visit_kind = ?")
            params.append(movement_type)
        params.extend([min_visits, limit])
        query = f"""
            SELECT
                printf(
                    'cell_%d_%d',
                    CAST(representative_latitude * 100 AS INTEGER),
                    CAST(representative_longitude * 100 AS INTEGER)
                ) AS hex_id,
                COUNT(*) AS total_visits,
                COUNT(DISTINCT device_id) AS unique_devices,
                AVG(duration_seconds) AS avg_duration_s,
                SUM(CASE WHEN visit_kind = 'stay' THEN 1 ELSE 0 END) AS stay_count,
                SUM(CASE WHEN visit_kind = 'pass_by' THEN 1 ELSE 0 END) AS passby_count,
                MIN(start_ts_utc) AS earliest_visit,
                MAX(end_ts_utc) AS latest_visit
            FROM visits
            WHERE {' AND '.join(where_clauses)}
            GROUP BY hex_id
            HAVING COUNT(*) >= ?
            ORDER BY total_visits DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [
            {
                "hex_id": str(row[0]),
                "total_pings": int(row[1]),
                "unique_devices": int(row[2]),
                "avg_duration_s": float(row[3]),
                "stay_count": int(row[4]),
                "passby_count": int(row[5]),
                "earliest_visit": str(row[6]),
                "latest_visit": str(row[7]),
            }
            for row in rows
        ]

    def get_device_journey(
        self,
        *,
        device_id: str,
        start_ts_utc: str,
        end_ts_utc: str,
        include_pass_by: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return a time-ordered visit sequence for one device in a bounded window."""
        params: list[Any] = [device_id, start_ts_utc, end_ts_utc]
        where_clauses = [
            "device_id = ?",
            "start_ts_utc >= ?",
            "end_ts_utc <= ?",
        ]
        if not include_pass_by:
            where_clauses.append("visit_kind = 'stay'")
        params.append(limit)
        query = f"""
            SELECT
                visit_id,
                visit_kind,
                stay_type,
                start_ts_utc,
                end_ts_utc,
                duration_seconds,
                ping_count,
                representative_latitude,
                representative_longitude
            FROM visits
            WHERE {' AND '.join(where_clauses)}
            ORDER BY start_ts_utc ASC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [
            {
                "visit_id": str(row[0]),
                "visit_kind": str(row[1]),
                "stay_type": str(row[2]) if row[2] is not None else None,
                "start_ts_utc": str(row[3]),
                "end_ts_utc": str(row[4]),
                "duration_seconds": int(row[5]),
                "ping_count": int(row[6]),
                "representative_latitude": float(row[7]),
                "representative_longitude": float(row[8]),
            }
            for row in rows
        ]

