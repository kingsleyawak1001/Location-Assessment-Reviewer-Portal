from __future__ import annotations

from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.visit_store import VisitStore


def test_run_analytics_matches_phase2_summary(tmp_path: Path) -> None:
    settings = AppSettings(
        artifacts_dir=tmp_path / "artifacts",
        manifest_path=tmp_path / "artifacts/manifest.db",
        accepted_dir=tmp_path / "artifacts/accepted",
        rejected_dir=tmp_path / "artifacts/rejected",
        visits_dir=tmp_path / "artifacts/visits",
        reports_dir=tmp_path / "artifacts/reports",
        phase3_db_path=tmp_path / "artifacts/visits.db",
        log_level="INFO",
    )
    result = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert result is not None
    assert result.phase2_summary is not None
    assert result.phase4_summary is not None

    store = VisitStore(settings.phase3_db_path)
    analytics = store.get_run_analytics(result.run_id, top_devices_limit=3)
    materialized = store.get_materialized_run_aggregate(result.run_id)

    assert analytics["run_id"] == result.run_id
    assert analytics["total_visits"] == result.visits_count
    assert analytics["unique_devices"] > 0
    assert analytics["avg_duration_seconds"] >= 0.0
    assert analytics["total_pings"] >= analytics["total_visits"]
    assert analytics["counts_by_visit_kind"] == result.phase2_summary["counts_by_visit_kind"]

    top_devices = analytics["top_devices"]
    assert len(top_devices) <= 3
    assert len(top_devices) > 0
    assert top_devices[0]["visits_count"] >= top_devices[-1]["visits_count"]

    assert materialized is not None
    assert materialized["run_id"] == result.run_id
    assert materialized["total_visits"] == result.visits_count

