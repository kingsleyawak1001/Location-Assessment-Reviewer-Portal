# Location Data Assessment Solution

End-to-end prototype for `Take-Home Assessment - Location Data`.

Implemented scope:
- **Part 1: ETL Pipeline Implementation** - ingestion, quality checks, idempotency, ping grouping, stay/pass-by classification.
- **Part 2: Database Architecture** - query-ready visit persistence with lineage and run metadata.
- **Part 3: API/Query Layer** - map analytics + device journey access patterns.
- **Part 4: Production System Architecture** - documented scale path and production design decisions.

## Quickstart (100% reproducible with python3)

From this folder (`Assessment Solution/`):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .
.venv/bin/python -m pip install pytest ruff mypy httpx
```

Run mandatory checks:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
.venv/bin/python -m pytest -vv tests/test_ingestion.py tests/test_quality.py tests/test_pipeline_integration.py
.venv/bin/python -m pytest -vv tests/test_transformation.py
```

Run full validation (primary + generated datasets):

```bash
.venv/bin/python -m src.utils.validation_runner \
  --raw-input ../raw_pings.csv \
  --additional-dir ../generated_pings
```

## Pipeline and API commands

Run pipeline:

```bash
.venv/bin/python -m src.pipeline.cli run phase1 --input ../raw_pings.csv
```

Run API:

```bash
.venv/bin/python -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

API endpoints:
- `GET /api/health`
- `GET /api/map/data`
- `GET /api/devices/{device_id}/journey`
- `GET /api/devices/suggestions`
- `GET /api/runs/latest`
- `GET /api/runs/{run_id}/bounds`

## Reviewer Portal

- Hosted: [Reviewer Portal (GitHub Pages)](https://kingsleyawak1001.github.io/Location-Assessment-Reviewer-Portal/)
- Local static run:

```bash
python3 -m http.server 8080
```

Then open `http://127.0.0.1:8080/reviewer_site/index.html`.

## Documentation for review

- Main index: [`docs/solution/README.md`](docs/solution/README.md)
- Final delivery summary: [`docs/solution/02_final_delivery_summary.md`](docs/solution/02_final_delivery_summary.md)
- Full solution overview: [`docs/solution/01_full_solution_documentation.md`](docs/solution/01_full_solution_documentation.md)
- Part 1 (ETL): [`docs/solution/domains/part1_etl_pipeline.md`](docs/solution/domains/part1_etl_pipeline.md)
- Part 2 (DB): [`docs/solution/domains/part2_database_architecture.md`](docs/solution/domains/part2_database_architecture.md)
- Part 3 (API): [`docs/solution/domains/part3_api_query_layer.md`](docs/solution/domains/part3_api_query_layer.md)
- Part 4 (Production): [`docs/solution/domains/part4_production_architecture.md`](docs/solution/domains/part4_production_architecture.md)
- API Playground guide: [`docs/solution/appendix/api_playground_guide.md`](docs/solution/appendix/api_playground_guide.md)
- Technology decisions: [`docs/solution/appendix/technology_decisions.md`](docs/solution/appendix/technology_decisions.md)
- System diagrams: [`docs/solution/appendix/system_diagrams.md`](docs/solution/appendix/system_diagrams.md)

## Outputs

Generated artifacts are written to `artifacts/`:
- `accepted/`, `rejected/`, `visits/`, `reports/`
- `manifest.db`, `visits.db`
- validation outputs in `artifacts/validation_runs/`
