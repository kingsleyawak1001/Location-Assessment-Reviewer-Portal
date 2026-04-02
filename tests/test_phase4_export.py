from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1


def test_export_phase4_creates_json_payload(tmp_path: Path) -> None:
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

    export_path = tmp_path / "exports" / "phase4.json"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.pipeline.cli",
            "export",
            "phase4",
            "--db-path",
            str(settings.phase3_db_path),
            "--run-id",
            result.run_id,
            "--output",
            str(export_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Exported analytics to" in completed.stdout
    assert export_path.exists()

    payload = json.loads(export_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == result.run_id
    assert payload["materialized"] is True
    assert payload["analytics"]["total_visits"] == result.visits_count
