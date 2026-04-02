from __future__ import annotations

import polars as pl

from src.transformation.grouping import group_pings_into_visits, summarize_visits


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


def test_visit_summary_counts_are_consistent() -> None:
    visits = pl.DataFrame(
        {
            "visit_id": ["a", "b", "c", "d"],
            "device_id": ["d1", "d1", "d2", "d2"],
            "visit_kind": ["stay", "pass_by", "stay", "stay"],
            "stay_type": ["home", None, "work", "other"],
            "start_ts_utc": [
                "2025-01-01T00:00:00Z",
                "2025-01-01T01:00:00Z",
                "2025-01-01T02:00:00Z",
                "2025-01-01T03:00:00Z",
            ],
            "end_ts_utc": [
                "2025-01-01T00:10:00Z",
                "2025-01-01T01:05:00Z",
                "2025-01-01T03:00:00Z",
                "2025-01-01T04:00:00Z",
            ],
            "duration_seconds": [600, 300, 3600, 3600],
            "ping_count": [3, 2, 6, 5],
            "representative_latitude": [10.0, 10.1, 11.0, 11.1],
            "representative_longitude": [20.0, 20.1, 21.0, 21.1],
        }
    )
    summary = summarize_visits(visits)
    assert summary["counts_by_visit_kind"] == {"stay": 3, "pass_by": 1}
    assert summary["counts_by_stay_type"] == {"home": 1, "work": 1, "other": 1}


def test_single_ping_is_pass_by() -> None:
    pings = pl.DataFrame(
        {
            "device_id": ["dev_single"],
            "ts_utc": ["2025-01-01T08:00:00Z"],
            "latitude": [10.0],
            "longitude": [20.0],
            "accuracy_m": [0.0],
        }
    )
    visits = group_pings_into_visits(pings)
    assert visits.height == 1
    assert visits["visit_kind"].to_list() == ["pass_by"]
    assert visits["ping_count"].to_list() == [1]


def test_accuracy_zero_is_strict_while_noisy_points_get_tolerance() -> None:
    base = {
        "device_id": ["dev_acc", "dev_acc"],
        "ts_utc": ["2025-01-01T10:00:00Z", "2025-01-01T10:05:00Z"],
        "latitude": [10.0, 10.0024],
        "longitude": [20.0, 20.0],
    }
    perfect_accuracy = pl.DataFrame({**base, "accuracy_m": [0.0, 0.0]})
    noisy_accuracy = pl.DataFrame({**base, "accuracy_m": [120.0, 120.0]})

    strict_visits = group_pings_into_visits(
        perfect_accuracy,
        max_gap_seconds=900,
        max_distance_m=250.0,
    )
    tolerant_visits = group_pings_into_visits(
        noisy_accuracy,
        max_gap_seconds=900,
        max_distance_m=250.0,
    )

    assert strict_visits.height == 2
    assert tolerant_visits.height == 1


def test_night_gap_bridge_keeps_single_visit_but_day_gap_splits() -> None:
    night_pings = pl.DataFrame(
        {
            "device_id": ["dev_night", "dev_night"],
            "ts_utc": ["2025-01-01T23:00:00Z", "2025-01-02T01:00:00Z"],
            "latitude": [10.0, 10.00001],
            "longitude": [20.0, 20.00001],
            "accuracy_m": [0.0, 0.0],
        }
    )
    day_pings = pl.DataFrame(
        {
            "device_id": ["dev_day", "dev_day"],
            "ts_utc": ["2025-01-01T12:00:00Z", "2025-01-01T14:00:00Z"],
            "latitude": [10.0, 10.00001],
            "longitude": [20.0, 20.00001],
            "accuracy_m": [0.0, 0.0],
        }
    )

    night_visits = group_pings_into_visits(
        night_pings,
        max_gap_seconds=900,
        night_gap_seconds=10_800,
        night_start_hour=22,
        night_end_hour=6,
        night_max_distance_m=120.0,
    )
    day_visits = group_pings_into_visits(
        day_pings,
        max_gap_seconds=900,
        night_gap_seconds=10_800,
        night_start_hour=22,
        night_end_hour=6,
        night_max_distance_m=120.0,
    )

    assert night_visits.height == 1
    assert day_visits.height == 2
