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
        run_id: str | None = None,
        movement_type: str | None = Query(default=None, pattern="^(stay|pass_by)$"),
        min_visits: int = Query(default=1, ge=1),
        limit: int = Query(default=100, ge=1, le=1000),
        response_format: str = Query(default="extended", pattern="^(extended|assessment)$"),
    ) -> dict[str, Any]:
        start_ts_utc, end_ts_utc = _window_to_utc_bounds(start_date, end_date)
        cells = store.get_location_analytics(
            start_ts_utc=start_ts_utc,
            end_ts_utc=end_ts_utc,
            run_id=run_id,
            west=west,
            east=east,
            south=south,
            north=north,
            movement_type=movement_type,
            min_visits=min_visits,
            limit=limit,
        )
        if response_format == "assessment":
            # Assessment format expects one aggregate object in "data".
            primary = cells[0] if cells else None
            return {
                "status": "success",
                "data": {
                    "hex_id": str(primary["hex_id"]) if primary is not None else "",
                    "total_pings": int(primary["total_pings"]) if primary is not None else 0,
                    "unique_devices": int(primary["unique_devices"]) if primary is not None else 0,
                    "avg_duration_s": (
                        float(primary["avg_duration_s"]) if primary is not None else 0.0
                    ),
                    "stay_count": int(primary["stay_count"]) if primary is not None else 0,
                    "passby_count": int(primary["passby_count"]) if primary is not None else 0,
                    "earliest_visit": str(primary["earliest_visit"]) if primary is not None else "",
                    "latest_visit": str(primary["latest_visit"]) if primary is not None else "",
                },
            }
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
        run_id: str | None = None,
        include_pass_by: bool = True,
        limit: int = Query(default=200, ge=1, le=5000),
    ) -> dict[str, Any]:
        journey = store.get_device_journey(
            device_id=device_id,
            run_id=run_id,
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

    @app.get("/api/devices/suggestions")
    def get_device_suggestions(
        start_ts: str,
        end_ts: str,
        run_id: str | None = None,
        include_pass_by: bool = True,
        limit: int = Query(default=5, ge=1, le=200),
    ) -> dict[str, Any]:
        devices = store.get_active_devices(
            run_id=run_id,
            start_ts_utc=start_ts,
            end_ts_utc=end_ts,
            include_pass_by=include_pass_by,
            limit=limit,
        )
        return {
            "status": "success",
            "data": {
                "window": {"start_ts": start_ts, "end_ts": end_ts},
                "devices": devices,
            },
        }

    @app.get("/api/runs/latest")
    def get_latest_run(source_contains: str | None = None) -> dict[str, Any]:
        run = store.get_latest_run(source_contains=source_contains)
        return {
            "status": "success",
            "data": run,
        }

    @app.get("/api/runs/{run_id}/bounds")
    def get_run_bounds(run_id: str) -> dict[str, Any]:
        bounds = store.get_run_time_bounds(run_id)
        return {
            "status": "success",
            "data": bounds,
        }

    return app


app = create_app()
