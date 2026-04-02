from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import create_app
from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1
from src.storage.visit_store import VisitStore


def _settings(tmp_path: Path) -> AppSettings:
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


def test_map_data_endpoint_returns_heatmap_cells(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert result is not None

    client = TestClient(create_app(settings))
    response = client.get(
        "/api/map/data",
        params={
            "start_date": "2025-01-01",
            "end_date": "2025-01-02",
            "west": -180,
            "east": 180,
            "south": -90,
            "north": 90,
            "movement_type": "pass_by",
            "min_visits": 1,
            "limit": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert payload["data"]["movement_type"] == "pass_by"
    assert len(payload["data"]["cells"]) > 0
    assert payload["data"]["cells"][0]["stay_count"] >= 0


def test_device_journey_endpoint_returns_ordered_visits(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = run_phase1(Path("tests/fixtures/pings_valid.csv"), settings=settings)
    assert result is not None

    store = VisitStore(settings.phase3_db_path)
    first_visit = store.get_visits_for_run(result.run_id, limit=1)[0]
    device_id = first_visit["device_id"]

    client = TestClient(create_app(settings))
    response = client.get(
        f"/api/devices/{device_id}/journey",
        params={
            "start_ts": "2025-01-01T00:00:00Z",
            "end_ts": "2025-01-03T00:00:00Z",
            "include_pass_by": "true",
            "limit": 50,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert payload["data"]["device_id"] == device_id
    assert len(payload["data"]["journey"]) > 0
    first = payload["data"]["journey"][0]
    assert "visit_id" in first
    assert first["visit_kind"] in {"stay", "pass_by"}
