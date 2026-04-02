from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns
from typing import Literal
from uuid import uuid4

from src.config.settings import AppSettings
from src.domain.models import ManifestRecord, ManifestStatus, QualityResult
from src.ingestion.csv_reader import read_raw_csv
from src.quality.validator import quality_counts_by_reason, validate_quality
from src.storage.artifact_store import ArtifactStore
from src.storage.manifest_store import ManifestStore
from src.transformation.grouping import group_pings_into_visits, summarize_visits
from src.utils.checksum import file_sha256
from src.utils.logging import get_logger


def run_phase1(
    input_path: Path,
    settings: AppSettings,
    ingestion_algorithm: Literal["primary", "alternative"] = "primary",
) -> QualityResult | None:
    run_start_ns = perf_counter_ns()
    step_durations_ms: dict[str, float] = {}

    def step_elapsed_ms(step_start_ns: int) -> float:
        return round((perf_counter_ns() - step_start_ns) / 1_000_000, 3)

    logger = get_logger("phase1.pipeline", settings.log_level)
    input_resolved = input_path.resolve()
    if not input_resolved.exists():
        raise FileNotFoundError(f"Input file not found: {input_resolved}")

    manifest = ManifestStore(settings.manifest_path)
    artifacts = ArtifactStore(
        settings.accepted_dir,
        settings.rejected_dir,
        settings.reports_dir,
        settings.visits_dir,
    )
    checksum = file_sha256(input_resolved)

    idempotency_step_start_ns = perf_counter_ns()
    already_processed = manifest.has_success(input_resolved, checksum)
    step_durations_ms["idempotency_check"] = step_elapsed_ms(idempotency_step_start_ns)

    if already_processed:
        total_duration_ms = round((perf_counter_ns() - run_start_ns) / 1_000_000, 3)
        logger.info(
            "input already processed, skipping",
            extra={
                "event": "phase1.idempotent_skip",
                "extra_fields": {
                    "input": str(input_resolved),
                    "total_duration_ms": total_duration_ms,
                    "step_durations_ms": step_durations_ms,
                },
            },
        )
        skip_record = ManifestRecord(
            file_path=str(input_resolved),
            checksum=checksum,
            status=ManifestStatus.SKIPPED,
            row_count_in=0,
            row_count_out=0,
            error=None,
        )
        manifest.append_record_atomic(skip_record)
        return None

    run_id = uuid4().hex
    row_count_in = 0
    current_step = "ingest_and_normalize"
    try:
        ingest_step_start_ns = perf_counter_ns()
        ingested_df, row_count_in = read_raw_csv(input_resolved, algorithm=ingestion_algorithm)
        step_durations_ms["ingest_and_normalize"] = step_elapsed_ms(ingest_step_start_ns)

        current_step = "quality_validation"
        quality_step_start_ns = perf_counter_ns()
        accepted, rejected = validate_quality(ingested_df)
        step_durations_ms["quality_validation"] = step_elapsed_ms(quality_step_start_ns)

        current_step = "transform_visits"
        transform_step_start_ns = perf_counter_ns()
        visits = group_pings_into_visits(
            accepted,
            max_gap_seconds=settings.phase2_max_gap_seconds,
            max_distance_m=settings.phase2_max_distance_m,
            stay_min_duration_seconds=settings.phase2_stay_min_duration_seconds,
            stay_min_pings=settings.phase2_stay_min_pings,
        )
        phase2_summary = summarize_visits(visits)
        step_durations_ms["transform_visits"] = step_elapsed_ms(transform_step_start_ns)

        current_step = "persist_outputs"
        persist_step_start_ns = perf_counter_ns()
        accepted_path = artifacts.write_accepted(run_id, accepted)
        rejected_path = artifacts.write_rejected(run_id, rejected)
        visits_path = artifacts.write_visits(run_id, visits)
        step_durations_ms["persist_outputs"] = step_elapsed_ms(persist_step_start_ns)

        current_step = "quality_aggregation"
        aggregation_step_start_ns = perf_counter_ns()
        counts_by_reason = quality_counts_by_reason(rejected)
        total_in = ingested_df.height
        reject_rate = (rejected.height / total_in) if total_in else 0.0
        step_durations_ms["quality_aggregation"] = step_elapsed_ms(aggregation_step_start_ns)

        total_duration_ms = round((perf_counter_ns() - run_start_ns) / 1_000_000, 3)
        report_payload = {
            "run_id": run_id,
            "input_file": str(input_resolved),
            "processed_at": datetime.now(UTC).isoformat(),
            "total_in": total_in,
            "algorithm": ingestion_algorithm,
            "accepted_count": accepted.height,
            "rejected_count": rejected.height,
            "reject_rate": reject_rate,
            "counts_per_reason": counts_by_reason,
            "accepted_path": str(accepted_path),
            "rejected_path": str(rejected_path),
            "visits_path": str(visits_path),
            "visits_count": visits.height,
            "phase2_summary": phase2_summary,
            "total_duration_ms": total_duration_ms,
            "step_durations_ms": step_durations_ms,
        }

        current_step = "manifest_update"
        manifest_step_start_ns = perf_counter_ns()
        success_record = ManifestRecord(
            file_path=str(input_resolved),
            checksum=checksum,
            status=ManifestStatus.SUCCESS,
            row_count_in=row_count_in,
            row_count_out=accepted.height,
            error=None,
            run_id=run_id,
        )
        manifest.append_record_atomic(success_record)
        step_durations_ms["manifest_update"] = step_elapsed_ms(manifest_step_start_ns)

        current_step = "write_report"
        report_step_start_ns = perf_counter_ns()
        report_path = artifacts.write_quality_report(run_id, report_payload)
        step_durations_ms["write_report"] = step_elapsed_ms(report_step_start_ns)

        total_duration_ms = round((perf_counter_ns() - run_start_ns) / 1_000_000, 3)
        report_payload["total_duration_ms"] = total_duration_ms
        report_payload["step_durations_ms"] = step_durations_ms
        # Persist final timings including write_report itself.
        report_path = artifacts.write_quality_report(run_id, report_payload)
        logger.info(
            "phase1 run completed",
            extra={
                "event": "phase1.completed",
                "extra_fields": {
                    "input": str(input_resolved),
                    "run_id": run_id,
                    "accepted": accepted.height,
                    "rejected": rejected.height,
                    "visits": visits.height,
                    "phase2_summary": phase2_summary,
                    "algorithm": ingestion_algorithm,
                    "total_duration_ms": total_duration_ms,
                    "step_durations_ms": step_durations_ms,
                },
            },
        )
        return QualityResult(
            accepted_count=accepted.height,
            rejected_count=rejected.height,
            total_in=total_in,
            reject_rate=reject_rate,
            counts_per_reason=counts_by_reason,
            accepted_path=accepted_path,
            rejected_path=rejected_path,
            report_path=report_path,
            run_id=run_id,
            algorithm=ingestion_algorithm,
            total_duration_ms=total_duration_ms,
            step_durations_ms=step_durations_ms,
            visits_count=visits.height,
            visits_path=visits_path,
            phase2_summary=phase2_summary,
        )
    except Exception as exc:
        total_duration_ms = round((perf_counter_ns() - run_start_ns) / 1_000_000, 3)
        failed_record = ManifestRecord(
            file_path=str(input_resolved),
            checksum=checksum,
            status=ManifestStatus.FAILED,
            row_count_in=row_count_in,
            row_count_out=0,
            error=f"{current_step}: {exc}",
            run_id=run_id,
        )
        manifest.append_record_atomic(failed_record)
        logger.error(
            "phase1 run failed",
            extra={
                "event": "phase1.failed",
                "extra_fields": {
                    "input": str(input_resolved),
                    "run_id": run_id,
                    "error": str(exc),
                    "failed_step": current_step,
                    "total_duration_ms": total_duration_ms,
                    "step_durations_ms": step_durations_ms,
                },
            },
        )
        raise

