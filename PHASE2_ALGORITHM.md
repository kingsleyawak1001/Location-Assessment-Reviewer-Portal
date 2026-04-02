# Phase 2 Algorithm Walkthrough

This document describes how accepted pings become visit records in the current implementation.

## Input and ordering

- Phase 2 receives only the accepted records from quality validation.
- Pings are processed per `device_id` and ordered by parsed UTC timestamp.

## Visit segmentation

For each device, a new visit is started when at least one condition is true:

1. Current row is the first ping for the device.
2. Time gap from previous ping exceeds `phase2_max_gap_seconds` (default `900`).
3. Spatial jump from previous ping exceeds `phase2_max_distance_m` (default `250m`, haversine distance).

This produces deterministic visit episodes (`device_id`, `visit_seq`).

## Visit aggregation

Each visit episode is reduced to one record with:

- `start_ts_utc`: min timestamp in episode.
- `end_ts_utc`: max timestamp in episode.
- `duration_seconds`: `end - start`.
- `ping_count`: number of pings in episode.
- `representative_latitude` / `representative_longitude`: median coordinate pair.

## stay vs pass_by

- `stay` if:
  - `duration_seconds >= phase2_stay_min_duration_seconds` (default `600`), and
  - `ping_count >= phase2_stay_min_pings` (default `3`).
- Otherwise `pass_by`.

## Stay type classification

Classification is applied only when `visit_kind == stay`:

- `home`: overnight pattern and long duration.
- `work`: daytime pattern and long duration.
- `other`: remaining stays.

`pass_by` visits keep `stay_type = null`.

## Structured observability outputs

Phase 2 contributes:

- `transform_visits` timing in `step_durations_ms`.
- `visits_count` and `visits_path` in the quality report.
- `phase2_summary` with:
  - `counts_by_visit_kind`
  - `counts_by_stay_type`

## Current limits (intentional)

- No cross-file carry-over for a visit that spans files.
- No POI/geofence enrichment.
- No personalized historical behavior model.
