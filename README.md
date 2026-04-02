# Phase 1 + Phase 2 Core ETL

This repository contains the first two implementation layers of the location-data ETL assessment:

- **Phase 1**: ingestion, data quality, idempotent processing, and local orchestration.
- **Phase 2**: core transformation from accepted pings into visit records (`stay` / `pass_by`) with baseline stay classification.

## Scope (implemented)

- CSV ingestion from an input file.
- Schema-resilient parsing (`accuracy_m` optional).
- Timestamp normalization (Unix epoch + ISO-8601 fallback).
- Data quality checks and split into accepted/rejected records.
- Visit grouping by device into visit sessions.
- Visit kind detection (`stay` vs `pass_by`).
- Basic stay type classification (`home`, `work`, `other`) with rule-based defaults.
- File manifest tracking with idempotency.
- Quality report generation.

Out of scope for the current implementation: production database/API implementation.

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

## Run Phase 1

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

This generates a comparison report with:

- winner by average runtime
- quality consistency between variants
- per-run metrics for both algorithms
- explicit "where better" section (speed/quality)

## Quality / tests

```bash
uv run pytest
uv run ruff check .
uv run mypy .
```

## Conventional Commits

Repository is configured to enforce Conventional Commits for commit messages.

Install and activate hooks:

```bash
uv sync --group dev
uv run pre-commit install --hook-type commit-msg
```

Validate the latest commit message manually:

```bash
uv run cz check --rev-range HEAD~1..HEAD
```

Message format:

```text
<type>[optional scope]: <description>
```

Examples:

- `feat(ingestion): add schema-resilient csv parser`
- `fix(quality): reject invalid latitude and longitude ranges`

## Outputs

All generated files are in `artifacts/`:

- `accepted/<run_id>_accepted.parquet`
- `rejected/<run_id>_rejected.parquet`
- `visits/<run_id>_visits.parquet`
- `reports/<run_id>_quality_report.json`
- `manifest.db`

Quality report also includes runtime metrics:

- `total_duration_ms`
- `step_durations_ms` (per pipeline step timing)
- `visits_count`
- `visits_path`

## Current limitations

- Grouping/classification defaults are heuristic and intentionally simple.
- Duplicate detection is exact-row based after normalization.
- Timestamp parser handles common epoch/ISO formats; malformed mixed formats are rejected.
- No database write or API layer for visit records yet.
