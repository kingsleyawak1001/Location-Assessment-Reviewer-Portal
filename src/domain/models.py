from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class ManifestStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ManifestRecord(BaseModel):
    file_path: str
    checksum: str
    status: ManifestStatus
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    row_count_in: int
    row_count_out: int
    error: str | None = None
    run_id: str = Field(default_factory=lambda: uuid4().hex)

    def to_row(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "checksum": self.checksum,
            "status": self.status.value,
            "processed_at": self.processed_at.isoformat(),
            "row_count_in": self.row_count_in,
            "row_count_out": self.row_count_out,
            "error": self.error,
            "run_id": self.run_id,
        }


class QualityResult(BaseModel):
    accepted_count: int
    rejected_count: int
    total_in: int
    reject_rate: float
    counts_per_reason: dict[str, int]
    accepted_path: Path
    rejected_path: Path
    report_path: Path
    run_id: str
    algorithm: str
    total_duration_ms: float
    step_durations_ms: dict[str, float]

