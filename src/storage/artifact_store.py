from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


class ArtifactStore:
    def __init__(
        self,
        accepted_dir: Path,
        rejected_dir: Path,
        reports_dir: Path,
        visits_dir: Path,
    ) -> None:
        self.accepted_dir = accepted_dir
        self.rejected_dir = rejected_dir
        self.reports_dir = reports_dir
        self.visits_dir = visits_dir
        self.accepted_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.visits_dir.mkdir(parents=True, exist_ok=True)

    def write_accepted(self, run_id: str, accepted: pl.DataFrame) -> Path:
        path = self.accepted_dir / f"{run_id}_accepted.parquet"
        accepted.write_parquet(path)
        return path

    def write_rejected(self, run_id: str, rejected: pl.DataFrame) -> Path:
        path = self.rejected_dir / f"{run_id}_rejected.parquet"
        rejected.write_parquet(path)
        return path

    def write_quality_report(self, run_id: str, payload: dict[str, Any]) -> Path:
        path = self.reports_dir / f"{run_id}_quality_report.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def write_visits(self, run_id: str, visits: pl.DataFrame) -> Path:
        path = self.visits_dir / f"{run_id}_visits.parquet"
        visits.write_parquet(path)
        return path

