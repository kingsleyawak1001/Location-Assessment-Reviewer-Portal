from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns
from typing import Any
from uuid import uuid4

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.artifact_store import ArtifactStore
from src.storage.manifest_store import ManifestStore
from src.storage.visit_store import VisitStore
from src.utils.logging import get_logger


def run_phase1_batch(
    input_dir: Path, pattern: str, max_workers: int, settings: AppSettings
) -> dict[str, Any]:
    logger = get_logger("phase1.batch", settings.log_level)
    input_dir_resolved = input_dir.resolve()
    if not input_dir_resolved.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir_resolved}")

    files = sorted(path for path in input_dir_resolved.glob(pattern) if path.is_file())
    # Initialize manifest schema once before worker fan-out to avoid DDL lock contention.
    ManifestStore(settings.manifest_path)
    # Initialize visit storage schema once before worker fan-out for the same reason.
    VisitStore(settings.phase3_db_path)
    batch_run_id = f"batch_{uuid4().hex}"
    batch_start_ns = perf_counter_ns()
    file_results: list[dict[str, Any]] = []

    if not files:
        payload = {
            "run_id": batch_run_id,
            "processed_at": datetime.now(UTC).isoformat(),
            "input_dir": str(input_dir_resolved),
            "pattern": pattern,
            "max_workers": max_workers,
            "totals": {"discovered_files": 0, "success": 0, "skipped": 0, "failed": 0},
            "files": [],
            "total_duration_ms": round((perf_counter_ns() - batch_start_ns) / 1_000_000, 3),
        }
        artifacts = ArtifactStore(
            settings.accepted_dir,
            settings.rejected_dir,
            settings.reports_dir,
            settings.visits_dir,
        )
        report_path = artifacts.write_quality_report(batch_run_id, payload)
        payload["report_path"] = str(report_path)
        return payload

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_phase1, path, settings): path for path in files}
        for future in as_completed(futures):
            path = futures[future]
            try:
                result = future.result()
                if result is None:
                    file_results.append(
                        {
                            "file_path": str(path),
                            "status": "skipped",
                            "total_duration_ms": None,
                            "accepted_count": 0,
                            "rejected_count": 0,
                            "run_id": None,
                        }
                    )
                else:
                    file_results.append(
                        {
                            "file_path": str(path),
                            "status": "success",
                            "total_duration_ms": result.total_duration_ms,
                            "accepted_count": result.accepted_count,
                            "rejected_count": result.rejected_count,
                            "run_id": result.run_id,
                        }
                    )
            except Exception as exc:
                logger.error(
                    "file failed in batch run",
                    extra={
                        "event": "phase1.batch.file_failed",
                        "extra_fields": {"file_path": str(path), "error": str(exc)},
                    },
                )
                file_results.append(
                    {
                        "file_path": str(path),
                        "status": "failed",
                        "total_duration_ms": None,
                        "accepted_count": 0,
                        "rejected_count": 0,
                        "run_id": None,
                        "error": str(exc),
                    }
                )

    success_count = sum(1 for item in file_results if item["status"] == "success")
    skipped_count = sum(1 for item in file_results if item["status"] == "skipped")
    failed_count = sum(1 for item in file_results if item["status"] == "failed")
    payload = {
        "run_id": batch_run_id,
        "processed_at": datetime.now(UTC).isoformat(),
        "input_dir": str(input_dir_resolved),
        "pattern": pattern,
        "max_workers": max_workers,
        "totals": {
            "discovered_files": len(files),
            "success": success_count,
            "skipped": skipped_count,
            "failed": failed_count,
        },
        "files": sorted(file_results, key=lambda item: item["file_path"]),
        "total_duration_ms": round((perf_counter_ns() - batch_start_ns) / 1_000_000, 3),
    }
    artifacts = ArtifactStore(
        settings.accepted_dir,
        settings.rejected_dir,
        settings.reports_dir,
        settings.visits_dir,
    )
    report_path = artifacts.write_quality_report(batch_run_id, payload)
    payload["report_path"] = str(report_path)
    return payload

