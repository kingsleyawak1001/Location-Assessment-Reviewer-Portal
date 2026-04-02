from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import polars as pl

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.manifest_store import ManifestStore


def make_settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        artifacts_dir=tmp_path / "artifacts",
        manifest_path=tmp_path / "artifacts/manifest.db",
        accepted_dir=tmp_path / "artifacts/accepted",
        rejected_dir=tmp_path / "artifacts/rejected",
        visits_dir=tmp_path / "artifacts/visits",
        reports_dir=tmp_path / "artifacts/reports",
        phase3_db_path=tmp_path / "artifacts/visits.db",
        log_level="INFO",
    )


def test_phase1_run_creates_artifacts_and_manifest(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    result = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert result is not None
    assert result.accepted_path.exists()
    assert result.rejected_path.exists()
    assert result.visits_path is not None
    assert result.visits_path.exists()
    assert result.report_path.exists()
    assert result.total_duration_ms > 0
    assert "ingest_and_normalize" in result.step_durations_ms
    assert "quality_validation" in result.step_durations_ms
    assert "transform_visits" in result.step_durations_ms
    assert "persist_outputs" in result.step_durations_ms
    assert result.visits_count >= 0
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["total_duration_ms"] > 0
    assert "step_durations_ms" in report
    assert "write_report" in report["step_durations_ms"]
    assert "visits_path" in report
    assert "visits_count" in report
    assert "phase2_summary" in report
    assert "phase3_summary" in report
    assert "consistency_checks" in report
    assert report["consistency_checks"]["phase1_ok"] is True
    assert report["consistency_checks"]["phase2_ok"] is True
    assert report["consistency_checks"]["phase3_ok"] is True
    by_kind = report["phase2_summary"]["counts_by_visit_kind"]
    assert sum(by_kind.values()) == report["visits_count"]
    assert report["phase3_summary"]["visits_written"] == report["visits_count"]

    with sqlite3.connect(settings.phase3_db_path) as connection:
        visits_count = connection.execute(
            "SELECT COUNT(*) FROM visits WHERE run_id = ?",
            (result.run_id,),
        ).fetchone()
        assert visits_count is not None
        assert visits_count[0] == result.visits_count
        lineage = connection.execute(
            "SELECT report_path FROM visit_lineage WHERE run_id = ?",
            (result.run_id,),
        ).fetchone()
        assert lineage is not None
        assert lineage[0] == str(result.report_path)
    manifest = ManifestStore(settings.manifest_path).load()
    assert manifest.height == 1
    assert manifest["status"][0] == "success"


def test_phase1_rerun_is_idempotent(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    first = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    second = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert first is not None
    assert second is None

    manifest = ManifestStore(settings.manifest_path).load()
    assert manifest.height == 2
    assert manifest["status"][0] == "success"
    assert manifest["status"][1] == "skipped"


def test_failed_run_sets_failed_manifest_status(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    bad_input = Path("tests/fixtures/pings_missing_required.csv")
    raised = False
    try:
        run_phase1(bad_input, settings=settings)
    except ValueError:
        raised = True
    assert raised

    manifest = ManifestStore(settings.manifest_path).load()
    assert manifest.height == 1
    assert manifest["status"][0] == "failed"
    assert "Missing required columns" in str(manifest["error"][0])


def test_multifile_batch_run(tmp_path: Path) -> None:
    from src.pipeline.batch_phase1 import run_phase1_batch

    settings = make_settings(tmp_path)
    input_dir = tmp_path / "batch_inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy("tests/fixtures/pings_no_accuracy.csv", input_dir / "a.csv")
    shutil.copy("tests/fixtures/pings_valid.csv", input_dir / "b.csv")

    batch_result = run_phase1_batch(input_dir, pattern="*.csv", max_workers=2, settings=settings)
    assert batch_result["totals"]["discovered_files"] == 2
    assert batch_result["totals"]["success"] == 2
    assert batch_result["totals"]["failed"] == 0
    assert Path(batch_result["report_path"]).exists()


def test_phase1_algorithm_comparison_report(tmp_path: Path) -> None:
    from src.pipeline.compare import compare_phase1_algorithms

    settings = make_settings(tmp_path)
    comparison = compare_phase1_algorithms(
        Path("tests/fixtures/pings_no_accuracy.csv"),
        settings=settings,
        runs=1,
    )
    assert comparison["phase"] == "phase1"
    assert comparison["winner"] in {"primary", "alternative", "tie"}
    assert comparison["quality_consistent_between_algorithms"] is True
    assert Path(comparison["report_path"]).exists()


def test_phase1_algorithm_comparison_repeatable_on_same_input(tmp_path: Path) -> None:
    from src.pipeline.compare import compare_phase1_algorithms

    settings = make_settings(tmp_path)
    first = compare_phase1_algorithms(
        Path("tests/fixtures/pings_no_accuracy.csv"),
        settings=settings,
        runs=1,
    )
    second = compare_phase1_algorithms(
        Path("tests/fixtures/pings_no_accuracy.csv"),
        settings=settings,
        runs=1,
    )
    assert first["winner"] in {"primary", "alternative", "tie"}
    assert second["winner"] in {"primary", "alternative", "tie"}
    assert first["quality_consistent_between_algorithms"] is True
    assert second["quality_consistent_between_algorithms"] is True
    assert Path(first["report_path"]).exists()
    assert Path(second["report_path"]).exists()


def test_phase2_threshold_overrides_from_settings(tmp_path: Path) -> None:
    input_path = tmp_path / "phase2_thresholds.csv"
    input_path.write_text(
        (
            "device_id,ts_utc,latitude,longitude,accuracy_m\n"
            "dev_a,2025-01-01T10:00:00Z,10.0,20.0,5.0\n"
            "dev_a,2025-01-01T10:05:00Z,10.0,20.0,5.0\n"
            "dev_a,2025-01-01T10:10:00Z,10.0,20.0,5.0\n"
        ),
        encoding="utf-8",
    )

    default_settings = make_settings(tmp_path / "default")
    default_result = run_phase1(input_path, settings=default_settings)
    assert default_result is not None
    assert default_result.visits_path is not None
    default_visits = pl.read_parquet(default_result.visits_path)
    assert default_visits["visit_kind"].to_list() == ["stay"]

    strict_settings = AppSettings(
        artifacts_dir=tmp_path / "strict" / "artifacts",
        manifest_path=tmp_path / "strict" / "artifacts/manifest.db",
        accepted_dir=tmp_path / "strict" / "artifacts/accepted",
        rejected_dir=tmp_path / "strict" / "artifacts/rejected",
        visits_dir=tmp_path / "strict" / "artifacts/visits",
        reports_dir=tmp_path / "strict" / "artifacts/reports",
        log_level="INFO",
        phase2_stay_min_pings=4,
    )
    strict_result = run_phase1(input_path, settings=strict_settings)
    assert strict_result is not None
    assert strict_result.visits_path is not None
    strict_visits = pl.read_parquet(strict_result.visits_path)
    assert strict_visits["visit_kind"].to_list() == ["pass_by"]

