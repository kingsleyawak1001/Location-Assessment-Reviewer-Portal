from __future__ import annotations

import json
import shutil
from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.manifest_store import ManifestStore


def make_settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        artifacts_dir=tmp_path / "artifacts",
        manifest_path=tmp_path / "artifacts/manifest.db",
        accepted_dir=tmp_path / "artifacts/accepted",
        rejected_dir=tmp_path / "artifacts/rejected",
        reports_dir=tmp_path / "artifacts/reports",
        log_level="INFO",
    )


def test_phase1_run_creates_artifacts_and_manifest(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    result = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert result is not None
    assert result.accepted_path.exists()
    assert result.rejected_path.exists()
    assert result.report_path.exists()
    assert result.total_duration_ms > 0
    assert "ingest_and_normalize" in result.step_durations_ms
    assert "quality_validation" in result.step_durations_ms
    assert "persist_outputs" in result.step_durations_ms
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["total_duration_ms"] > 0
    assert "step_durations_ms" in report
    assert "write_report" in report["step_durations_ms"]
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

