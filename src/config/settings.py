from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="PHASE1_", extra="ignore")

    artifacts_dir: Path = Field(default=Path("artifacts"))
    manifest_path: Path = Field(default=Path("artifacts/manifest.db"))
    accepted_dir: Path = Field(default=Path("artifacts/accepted"))
    rejected_dir: Path = Field(default=Path("artifacts/rejected"))
    visits_dir: Path = Field(default=Path("artifacts/visits"))
    reports_dir: Path = Field(default=Path("artifacts/reports"))
    log_level: str = Field(default="INFO")
    phase2_max_gap_seconds: int = Field(default=900)
    phase2_max_distance_m: float = Field(default=250.0)
    phase2_stay_min_duration_seconds: int = Field(default=600)
    phase2_stay_min_pings: int = Field(default=3)

