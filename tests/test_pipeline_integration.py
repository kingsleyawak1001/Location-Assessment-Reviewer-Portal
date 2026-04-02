from __future__ import annotations

from pathlib import Path

import polars as pl

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1


def make_settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        artifacts_dir=tmp_path / "artifacts",
        manifest_path=tmp_path / "artifacts/manifest.parquet",
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
    manifest = pl.read_parquet(settings.manifest_path)
    assert manifest.height == 1
    assert manifest["status"][0] == "success"


def test_phase1_rerun_is_idempotent(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    first = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    second = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert first is not None
    assert second is None

    manifest = pl.read_parquet(settings.manifest_path)
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

    manifest = pl.read_parquet(settings.manifest_path)
    assert manifest.height == 1
    assert manifest["status"][0] == "failed"
    assert "Missing required columns" in str(manifest["error"][0])

