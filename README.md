# Phase 1 ETL Foundations

This repository contains **Phase 1** of the location-data ETL assessment: ingestion, data quality, idempotent processing, and local orchestration.

## Scope (Phase 1 only)

- CSV ingestion from an input file.
- Schema-resilient parsing (`accuracy_m` optional).
- Timestamp normalization (Unix epoch + ISO-8601 fallback).
- Data quality checks and split into accepted/rejected records.
- File manifest tracking with idempotency.
- Quality report generation.

Out of scope for this phase: visit grouping (`stay`/`pass_by`), classification, database/API design implementation.

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
- `reports/<run_id>_quality_report.json`
- `manifest.parquet`

## Current limitations

- Single-file local runner for Phase 1 baseline.
- Duplicate detection is exact-row based after normalization.
- Timestamp parser handles common epoch/ISO formats; malformed mixed formats are rejected.
