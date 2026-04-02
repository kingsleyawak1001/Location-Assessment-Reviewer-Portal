## Database Design

This document describes the processed-storage schema and production-oriented design decisions.

## 1) Prototype Storage (implemented)

SQLite database: `artifacts/visits.db`

### Tables

#### `visits`

- One row per derived visit.
- Key fields:
  - `run_id`, `visit_id` (PK pair)
  - `device_id`, `visit_kind`, `stay_type`
  - `start_ts_utc`, `end_ts_utc`, `duration_seconds`, `ping_count`
  - `representative_latitude`, `representative_longitude`
  - lineage fields: `source_file`, `source_checksum`, `persisted_at`

#### `visit_lineage`

- Run-level lineage and processing metadata.
- Key fields:
  - `run_id` (PK)
  - input source/checksum
  - `algorithm`
  - `total_in`, `accepted_count`, `rejected_count`, `visits_count`
  - artifact paths (`accepted_path`, `rejected_path`, `visits_path`, `report_path`)
  - `persisted_at`

#### `run_aggregates`

- Materialized aggregate snapshot per run.
- Key fields:
  - `run_id` (PK)
  - `total_visits`, `unique_devices`, `avg_duration_seconds`, `total_pings`
  - JSON fields for `counts_by_visit_kind`, `counts_by_stay_type`, `top_devices`
  - `materialized_at`

### Indexing (implemented)

- `idx_visits_device_start` on (`device_id`, `start_ts_utc`)
  - supports journey-oriented scans per device.

### Incremental processing (implemented)

- File-level manifest idempotency (`manifest.db`) prevents duplicate committed runs for same `(file, checksum)`.
- New inputs are processed and appended as new `run_id`.
- Reruns with unchanged checksum are skipped.

---

## 2) Production Database Architecture (design)

### Technology choice

- Analytics plane: ClickHouse (or BigQuery/Snowflake equivalent).
- Metadata/control plane: PostgreSQL.

### Partitioning strategy

- Partition fact visits by `visit_date`.
- Cluster/order keys by (`h3_cell`, `visit_kind`, `device_id`, `start_ts_utc`).
- Rationale: prune by time first, keep geo and journey locality.

### Indexing strategy

- Primary index/order by time + geography.
- Secondary skip indexes for `visit_kind`, `stay_type`.
- Materialized views for hot aggregates:
  - geo/time heatmaps,
  - daily device-level summaries.

### Query performance strategy

- Bounded query contracts (time + area + limits).
- Cache hot windows.
- Pre-aggregations for map endpoints.
- Backpressure and timeout guardrails for expensive queries.

---

## 3) Supporting metadata and lineage model (design)

- `processed_files` / manifest table:
  - file URI, checksum, schema version, state transitions, timestamps.
- `pipeline_runs`:
  - run status, step timings, data quality metrics, version tags.
- `data_quality_events`:
  - reject reasons, counts, anomaly flags.

These tables support audit, replay, and incident analysis.

---

## 4) Setup instructions (prototype)

1. Run pipeline:

```bash
uv run phase1 run phase1 --input ../raw_pings.csv
```

2. Query lineage/visits:

```bash
uv run phase1 query phase3 --db-path artifacts/visits.db --limit 5
```

3. Query materialized run analytics:

```bash
uv run phase1 query phase4 --db-path artifacts/visits.db --run-id <run_id> --top-devices-limit 5
```
