# Location Data Assessment Solution

This repository contains an end-to-end prototype for the location-data assessment:

- **Part 1: ETL Pipeline Implementation** - ingestion, data quality checks, idempotency, artifacts, reporting, ping grouping, and stay/pass-by classification.
- **Part 2: Database Architecture** - query-ready visit storage with lineage metadata and run tracking.
- **Part 3: API/Query Layer** - map analytics and device journey access patterns (HTTP + CLI query surface).
- **Part 4: Production System Architecture** - documented scaling strategy, infrastructure direction, and production migration path.

## Tech stack

- Python 3.12+
- Polars
- Pydantic v2 + pydantic-settings
- Pytest
- Ruff
- mypy
- uv/pyproject

## Setup

```bash
uv sync
```

## Run pipeline

From `Assessment Solution/`:

```bash
uv run phase1 run phase1 --input ../raw_pings.csv
```

Process multiple files concurrently:

```bash
uv run phase1 run phase1 --input-dir ../assessment --glob "*.csv" --max-workers 4
```

Compare primary vs alternative algorithm (Phase 1):

```bash
uv run phase1 compare phase1 --input ../raw_pings.csv --runs 2
```

## Config (env overrides)

All settings use `PHASE1_` prefix:

- `PHASE1_PHASE2_MAX_GAP_SECONDS`
- `PHASE1_PHASE2_MAX_DISTANCE_M`
- `PHASE1_PHASE2_STAY_MIN_DURATION_SECONDS`
- `PHASE1_PHASE2_STAY_MIN_PINGS`
- `PHASE1_PHASE2_UNKNOWN_ACCURACY_M`
- `PHASE1_PHASE2_NIGHT_GAP_SECONDS`
- `PHASE1_PHASE2_NIGHT_START_HOUR`
- `PHASE1_PHASE2_NIGHT_END_HOUR`
- `PHASE1_PHASE2_NIGHT_MAX_DISTANCE_M`
- `PHASE1_PHASE3_DB_PATH`
- `PHASE1_PHASE4_TOP_DEVICES_LIMIT`

## Query and export layer

Inspect lineage and visit previews:

```bash
uv run phase1 query phase3 --db-path artifacts/visits.db --limit 5
uv run phase1 query phase3 --db-path artifacts/visits.db --run-id <run_id> --limit 10
```

Query analytics (materialized when available):

```bash
uv run phase1 query phase4 --db-path artifacts/visits.db --run-id <run_id> --top-devices-limit 5
```

Export analytics payload:

```bash
uv run phase1 export phase4 --db-path artifacts/visits.db --run-id <run_id> --output artifacts/reports/<run_id>_phase4_export.json
```

## API layer (implemented for required use-cases)

Run local API server:

```bash
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

Endpoints:

- `GET /api/health`
- `GET /api/map/data`
- `GET /api/devices/{device_id}/journey`
- `GET /api/devices/suggestions`
- `GET /api/runs/latest`
- `GET /api/runs/{run_id}/bounds`

## Reviewer Portal (Web UI)

Interactive API Playground is located in:

- `reviewer_site/index.html`

Local run:

```bash
python -m http.server 8080
```

Hosted run (GitHub Pages):

- [Reviewer Portal (GitHub Pages)](https://kingsleyawak1001.github.io/Location-Assessment-Reviewer-Portal/)

## Design docs (assessment deliverables)

- Canonical structured documentation:
  - [`docs/solution/README.md`](docs/solution/README.md)
  - [`docs/solution/02_final_delivery_summary.md`](docs/solution/02_final_delivery_summary.md)
  - [`docs/solution/01_full_solution_documentation.md`](docs/solution/01_full_solution_documentation.md)
  - [`docs/solution/domains/part1_etl_pipeline.md`](docs/solution/domains/part1_etl_pipeline.md)
  - [`docs/solution/domains/part2_database_architecture.md`](docs/solution/domains/part2_database_architecture.md)
  - [`docs/solution/domains/part3_api_query_layer.md`](docs/solution/domains/part3_api_query_layer.md)
  - [`docs/solution/domains/part4_production_architecture.md`](docs/solution/domains/part4_production_architecture.md)
  - [`docs/solution/appendix/api_playground_guide.md`](docs/solution/appendix/api_playground_guide.md)
  - [`docs/solution/appendix/technology_decisions.md`](docs/solution/appendix/technology_decisions.md)
  - [`docs/solution/appendix/system_diagrams.md`](docs/solution/appendix/system_diagrams.md)

## Quality / tests

```bash
uv run pytest
uv run ruff check .
uv run mypy .
```

Project validation policy (always use primary + additional generated datasets):

```bash
python -m src.utils.validation_runner \
  --raw-input ../raw_pings.csv \
  --additional-dir ../generated_pings
```

This runner always executes:

- mandatory static checks (`ruff`, `mypy`)
- mandatory tests for Phase 1 and Phase 2
- e2e on primary dataset (`raw_pings.csv`)
- e2e on all generated additional datasets in `--additional-dir`

## Outputs

All generated files are in `artifacts/`:

- `accepted/<run_id>_accepted.parquet`
- `rejected/<run_id>_rejected.parquet`
- `visits/<run_id>_visits.parquet`
- `reports/<run_id>_quality_report.json`
- `reports/<run_id>_phase4_export.json` (optional export)
- `manifest.db`
- `visits.db`

Quality report also includes runtime metrics:

- `total_duration_ms`
- `step_durations_ms`
- `visits_count` and `visits_path`
- `phase2_summary`
- `phase3_summary`
- `phase4_summary`
- `consistency_checks` (`phase1_ok`..`phase4_ok`)

## What is implemented vs design-only

Implemented:

- ETL code prototype with grouping and stay classification.
- Database persistence with lineage.
- API/query layer for required use-cases (`/api/map/data`, `/api/devices/{device_id}/journey`).
- Query/export interface via CLI.
- Materialized analytics by run.

Design-only (documented, not fully deployed at production scale):

- Production-grade API hardening (auth, advanced rate limiting, multi-tenant controls).
- Cloud orchestration and managed infra rollout.
- Distributed columnar deployment at billion-scale.

## Known limitations and trade-offs

- Grouping/classification defaults are heuristic and intentionally simple.
- Duplicate detection is exact-row based after normalization.
- Timestamp parser handles common epoch/ISO formats; malformed mixed formats are rejected.
- Prototype uses SQLite for local simplicity; production should use columnar analytics storage.
