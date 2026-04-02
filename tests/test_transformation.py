from __future__ import annotations

import polars as pl

from src.transformation.grouping import group_pings_into_visits


def test_grouping_produces_stay_and_pass_by() -> None:
    pings = pl.DataFrame(
        {
            "device_id": [
                "dev_1",
                "dev_1",
                "dev_1",
                "dev_1",
                "dev_1",
            ],
            "ts_utc": [
                "2025-01-01T08:00:00Z",
                "2025-01-01T08:06:00Z",
                "2025-01-01T08:13:00Z",
                "2025-01-01T08:40:00Z",
                "2025-01-01T08:41:00Z",
            ],
            "latitude": [10.0, 10.0001, 9.9999, 10.02, 10.0201],
            "longitude": [20.0, 20.0001, 20.0002, 20.02, 20.0202],
            "accuracy_m": [5.0, 5.0, 5.0, 5.0, 5.0],
        }
    )

    visits = group_pings_into_visits(
        pings,
        max_gap_seconds=900,
        max_distance_m=300.0,
        stay_min_duration_seconds=600,
        stay_min_pings=3,
    )
    assert visits.height == 2
    assert visits["visit_kind"].to_list() == ["stay", "pass_by"]
    assert visits["duration_seconds"].to_list() == [780, 60]
    assert visits["ping_count"].to_list() == [3, 2]


def test_stay_type_defaults_to_home_work_other_rules() -> None:
    pings = pl.DataFrame(
        {
            "device_id": [
                "dev_2",
                "dev_2",
                "dev_2",
                "dev_3",
                "dev_3",
                "dev_3",
                "dev_4",
                "dev_4",
                "dev_4",
            ],
            "ts_utc": [
                "2025-01-01T21:00:00Z",
                "2025-01-01T22:00:00Z",
                "2025-01-01T23:30:00Z",
                "2025-01-02T09:00:00Z",
                "2025-01-02T11:00:00Z",
                "2025-01-02T13:00:00Z",
                "2025-01-03T12:00:00Z",
                "2025-01-03T12:30:00Z",
                "2025-01-03T13:00:00Z",
            ],
            "latitude": [10.0, 10.0, 10.0, 11.0, 11.0, 11.0, 12.0, 12.0, 12.0],
            "longitude": [20.0, 20.0, 20.0, 21.0, 21.0, 21.0, 22.0, 22.0, 22.0],
            "accuracy_m": [5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0],
        }
    )

    visits = group_pings_into_visits(
        pings,
        max_gap_seconds=8_000,
        max_distance_m=200.0,
        stay_min_duration_seconds=600,
        stay_min_pings=3,
    ).sort("device_id")

    assert visits.height == 3
    assert visits["visit_kind"].to_list() == ["stay", "stay", "stay"]
    assert visits["stay_type"].to_list() == ["home", "work", "other"]
