"""Minimal API implementation for the assessment's required query use-cases.

Implemented endpoints map to Part 3 requirements:
- Location analytics / heatmap data: `GET /api/map/data`
- Device journey / visit details: `GET /api/devices/{device_id}/journey`
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.config.settings import AppSettings
from src.storage.visit_store import VisitStore


def _window_to_utc_bounds(start_date: str, end_date: str) -> tuple[str, str]:
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
    end_dt = datetime.fromisoformat(end_date).replace(tzinfo=UTC)
    start_bound = start_dt.strftime("%Y-%m-%dT00:00:00Z")
    end_bound = end_dt.strftime("%Y-%m-%dT23:59:59Z")
    return start_bound, end_bound


def create_app(settings: AppSettings | None = None) -> FastAPI:
    """Create a FastAPI app bound to the configured visit store."""
    app_settings = settings if settings is not None else AppSettings()
    app = FastAPI(title="Location Analytics API", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    store = VisitStore(app_settings.phase3_db_path)

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/map/data")
    def get_map_data(
        start_date: str,
        end_date: str,
        west: float,
        east: float,
        south: float,
        north: float,
        movement_type: str | None = Query(default=None, pattern="^(stay|pass_by)$"),
        min_visits: int = Query(default=1, ge=1),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        start_ts_utc, end_ts_utc = _window_to_utc_bounds(start_date, end_date)
        cells = store.get_location_analytics(
            start_ts_utc=start_ts_utc,
            end_ts_utc=end_ts_utc,
            west=west,
            east=east,
            south=south,
            north=north,
            movement_type=movement_type,
            min_visits=min_visits,
            limit=limit,
        )
        return {
            "status": "success",
            "data": {
                "window": {"start_date": start_date, "end_date": end_date},
                "bbox": {
                    "west": west,
                    "east": east,
                    "south": south,
                    "north": north,
                },
                "movement_type": movement_type,
                "cells": cells,
            },
        }

    @app.get("/api/devices/{device_id}/journey")
    def get_device_journey(
        device_id: str,
        start_ts: str,
        end_ts: str,
        include_pass_by: bool = True,
        limit: int = Query(default=200, ge=1, le=5000),
    ) -> dict[str, Any]:
        journey = store.get_device_journey(
            device_id=device_id,
            start_ts_utc=start_ts,
            end_ts_utc=end_ts,
            include_pass_by=include_pass_by,
            limit=limit,
        )
        return {
            "status": "success",
            "data": {
                "device_id": device_id,
                "window": {"start_ts": start_ts, "end_ts": end_ts},
                "journey": journey,
            },
        }

    return app


app = create_app()
