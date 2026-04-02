from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from src.config.settings import AppSettings
from src.domain.models import ManifestRecord, ManifestStatus, QualityResult
from src.ingestion.csv_reader import read_raw_csv
from src.quality.validator import quality_counts_by_reason, validate_quality
from src.storage.artifact_store import ArtifactStore
from src.storage.manifest_store import ManifestStore
from src.utils.checksum import file_sha256
from src.utils.logging import get_logger


def run_phase1(input_path: Path, settings: AppSettings) -> QualityResult | None:
    logger = get_logger("phase1.pipeline", settings.log_level)
    input_resolved = input_path.resolve()
    if not input_resolved.exists():
        raise FileNotFoundError(f"Input file not found: {input_resolved}")

    manifest = ManifestStore(settings.manifest_path)
    artifacts = ArtifactStore(settings.accepted_dir, settings.rejected_dir, settings.reports_dir)
    checksum = file_sha256(input_resolved)

    if manifest.has_success(input_resolved, checksum):
        logger.info(
            "input already processed, skipping",
            extra={
                "event": "phase1.idempotent_skip",
                "extra_fields": {"input": str(input_resolved)},
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
    try:
        ingested_df, row_count_in = read_raw_csv(input_resolved)
        accepted, rejected = validate_quality(ingested_df)
        accepted_path = artifacts.write_accepted(run_id, accepted)
        rejected_path = artifacts.write_rejected(run_id, rejected)
        counts_by_reason = quality_counts_by_reason(rejected)
        total_in = ingested_df.height
        reject_rate = (rejected.height / total_in) if total_in else 0.0
        report_payload = {
            "run_id": run_id,
            "input_file": str(input_resolved),
            "processed_at": datetime.now(UTC).isoformat(),
            "total_in": total_in,
            "accepted_count": accepted.height,
            "rejected_count": rejected.height,
            "reject_rate": reject_rate,
            "counts_per_reason": counts_by_reason,
            "accepted_path": str(accepted_path),
            "rejected_path": str(rejected_path),
        }
        report_path = artifacts.write_quality_report(run_id, report_payload)

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
        logger.info(
            "phase1 run completed",
            extra={
                "event": "phase1.completed",
                "extra_fields": {
                    "input": str(input_resolved),
                    "run_id": run_id,
                    "accepted": accepted.height,
                    "rejected": rejected.height,
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
        )
    except Exception as exc:
        failed_record = ManifestRecord(
            file_path=str(input_resolved),
            checksum=checksum,
            status=ManifestStatus.FAILED,
            row_count_in=row_count_in,
            row_count_out=0,
            error=str(exc),
            run_id=run_id,
        )
        manifest.append_record_atomic(failed_record)
        logger.error(
            "phase1 run failed",
            extra={
                "event": "phase1.failed",
                "extra_fields": {"input": str(input_resolved), "run_id": run_id, "error": str(exc)},
            },
        )
        raise

