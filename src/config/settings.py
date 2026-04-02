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
    reports_dir: Path = Field(default=Path("artifacts/reports"))
    log_level: str = Field(default="INFO")

