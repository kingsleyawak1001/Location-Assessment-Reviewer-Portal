from __future__ import annotations

from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.visit_store import VisitStore


def test_visit_store_lineage_and_visits_query(tmp_path: Path) -> None:
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

    store = VisitStore(settings.phase3_db_path)
    runs = store.get_lineage_runs(limit=3)
    assert len(runs) == 1
    assert runs[0]["run_id"] == result.run_id
    assert runs[0]["visits_count"] == result.visits_count
    assert runs[0]["accepted_count"] == result.accepted_count
    assert runs[0]["rejected_count"] == result.rejected_count
    assert runs[0]["report_path"] == str(result.report_path)

    preview = store.get_visits_for_run(result.run_id, limit=2)
    assert len(preview) > 0
    assert len(preview) <= 2
    assert preview[0]["visit_id"]
    assert preview[0]["visit_kind"] in {"stay", "pass_by"}

