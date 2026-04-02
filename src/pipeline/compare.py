from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Literal
from uuid import uuid4

from src.config.settings import AppSettings
from src.domain.models import QualityResult
from src.pipeline.phase1 import run_phase1
from src.storage.artifact_store import ArtifactStore


def _isolated_settings(base: AppSettings, suffix: str) -> AppSettings:
    artifacts_dir = base.artifacts_dir / f"compare_{suffix}"
    return AppSettings(
        artifacts_dir=artifacts_dir,
        manifest_path=artifacts_dir / "manifest.db",
        accepted_dir=artifacts_dir / "accepted",
        rejected_dir=artifacts_dir / "rejected",
        visits_dir=artifacts_dir / "visits",
        reports_dir=artifacts_dir / "reports",
        phase3_db_path=artifacts_dir / "visits.db",
        log_level=base.log_level,
        phase2_max_gap_seconds=base.phase2_max_gap_seconds,
        phase2_max_distance_m=base.phase2_max_distance_m,
        phase2_stay_min_duration_seconds=base.phase2_stay_min_duration_seconds,
        phase2_stay_min_pings=base.phase2_stay_min_pings,
        phase4_top_devices_limit=base.phase4_top_devices_limit,
    )


def _run_variant(
    input_path: Path,
    base_settings: AppSettings,
    algorithm: Literal["primary", "alternative"],
    runs: int,
    compare_run_id: str,
) -> list[QualityResult]:
    isolated = _isolated_settings(base_settings, f"{compare_run_id}_{algorithm}")
    results: list[QualityResult] = []
    for run_index in range(runs):
        unique_input = isolated.artifacts_dir / f"run_{run_index}_{input_path.name}"
        unique_input.parent.mkdir(parents=True, exist_ok=True)
        unique_input.write_bytes(input_path.read_bytes())
        result = run_phase1(unique_input, isolated, ingestion_algorithm=algorithm)
        if result is None:
            raise RuntimeError(f"Unexpected skip for algorithm {algorithm} run {run_index}")
        results.append(result)
    return results


def compare_phase1_algorithms(
    input_path: Path,
    settings: AppSettings,
    runs: int = 1,
) -> dict[str, Any]:
    if runs < 1:
        raise ValueError("runs must be >= 1")
    input_resolved = input_path.resolve()
    if not input_resolved.exists():
        raise FileNotFoundError(f"Input file not found: {input_resolved}")

    compare_run_id = uuid4().hex
    primary_results = _run_variant(
        input_resolved,
        settings,
        "primary",
        runs,
        compare_run_id=compare_run_id,
    )
    alternative_results = _run_variant(
        input_resolved,
        settings,
        "alternative",
        runs,
        compare_run_id=compare_run_id,
    )

    primary_avg_ms = mean(result.total_duration_ms for result in primary_results)
    alternative_avg_ms = mean(result.total_duration_ms for result in alternative_results)

    primary_latest = primary_results[-1]
    alternative_latest = alternative_results[-1]
    output_consistent = (
        primary_latest.accepted_count == alternative_latest.accepted_count
        and primary_latest.rejected_count == alternative_latest.rejected_count
        and primary_latest.total_in == alternative_latest.total_in
    )

    if primary_avg_ms < alternative_avg_ms:
        winner = "primary"
    elif alternative_avg_ms < primary_avg_ms:
        winner = "alternative"
    else:
        winner = "tie"

    run_id = f"compare_{compare_run_id}"
    report: dict[str, Any] = {
        "run_id": run_id,
        "phase": "phase1",
        "processed_at": datetime.now(UTC).isoformat(),
        "input_file": str(input_resolved),
        "runs_per_algorithm": runs,
        "winner": winner,
        "quality_consistent_between_algorithms": output_consistent,
        "comparison": {
            "primary": {
                "avg_total_duration_ms": round(primary_avg_ms, 3),
                "runs": [deepcopy(result.model_dump(mode="json")) for result in primary_results],
            },
            "alternative": {
                "avg_total_duration_ms": round(alternative_avg_ms, 3),
                "runs": [
                    deepcopy(result.model_dump(mode="json"))
                    for result in alternative_results
                ],
            },
        },
        "where_better": {
            "speed": (
                "primary" if primary_avg_ms < alternative_avg_ms else
                "alternative" if alternative_avg_ms < primary_avg_ms else
                "tie"
            ),
            "quality": "tie" if output_consistent else "needs_review",
        },
    }
    artifacts = ArtifactStore(
        settings.accepted_dir,
        settings.rejected_dir,
        settings.reports_dir,
        settings.visits_dir,
    )
    report_path = artifacts.write_quality_report(run_id, report)
    report["report_path"] = str(report_path)
    return report

