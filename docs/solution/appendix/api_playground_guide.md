# API Playground Guide

## Purpose

`reviewer_site/index.html` is the interactive reviewer surface for:
- validating API behavior on real pipeline outputs;
- observing live processing state transitions;
- verifying response payloads with visual charts.

It is designed for technical review sessions where the reviewer needs both
request-level control and immediate runtime visibility.

## Start Checklist

From `Assessment Solution/`, run:

```bash
uvicorn src.api.app:app --host 127.0.0.1 --port 8000
python -m http.server 8080
```

Then open:
- Playground: [http://127.0.0.1:8080/reviewer_site/index.html](http://127.0.0.1:8080/reviewer_site/index.html)
- API health: [http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)

## Primary Workflow

1. Select one of the 3 presets (auto-applies map + journey defaults).
2. Run `Map Query` and `Journey Query` manually, or run `Run Full Demo`.
3. Inspect charts, raw response JSON, execution log, and timeline details.
4. Copy outputs with `Copy Raw Response` and `Copy Execution Log`.

## UI Areas and Behavior

### Controls

- `API Base URL`: base URL for all API requests.
  - On GitHub Pages, if this is set to localhost, the playground auto-falls back to
    built-in autonomous demo data.
- `Preset Profile`: applies predefined map/journey parameters.
- `Run Full Demo`: executes:
  1) resolve latest `raw_pings.csv` run,
  2) find sample device,
  3) map query,
  4) journey query.

### Query Forms

- **Map Analytics** (`/api/map/data`)
  - Sends assessment-compatible payload request (`response_format=assessment`).
  - Automatically includes active `run_id` from lineage resolution.
- **Device Journey** (`/api/devices/{device_id}/journey`)
  - Uses same active `run_id` for source consistency with map data.
  - Supports `include_pass_by` and bounded `limit`.

### Live Monitor

- `Live updates` toggle:
  - **ON:** background health checks + automatic device suggestions.
  - **OFF:** pauses background health checks and auto suggestions.
- Collapsible monitor (`Hide` / `Show`) to optimize screen space.

### Activity Timeline

- Displays latest events first (capped feed).
- Each event is expandable and includes:
  - `Description`: what happened.
  - `Request`: endpoint URL.
  - `Source`: why/how it was triggered (`Page bootstrap`, `Preset change`,
    `Run Full Demo`, `Background health check`, manual action).

### Live Process Monitor

Stage states tracked in real time:
- API Connectivity
- Run Resolution
- Preset Application
- Map Query
- Device Suggestions
- Journey Query
- Chart Rendering

Each stage updates with state badge (`idle`, `running`, `success`, `warn`, `error`)
and summary detail.

### KPIs

- Last API latency
- Last map cells
- Last journey visits
- Last suggestions

### Outputs

- **Raw API Response:** latest JSON response body.
- **Execution Log:** timestamped event stream and raw payload traces.

## API Endpoints Used by Playground

### Health and Run Context

- `GET /api/health`
- `GET /api/runs/latest?source_contains=raw_pings.csv`
- `GET /api/runs/{run_id}/bounds`

### Query Execution

- `GET /api/map/data`
- `GET /api/devices/suggestions`
- `GET /api/devices/{device_id}/journey`

## Data Consistency Contract

The playground is run-aware:
- it resolves the latest `raw_pings.csv` lineage run;
- applies that run window to form defaults;
- injects the same `run_id` into map and journey queries.

This ensures both visualizations refer to the same ingestion run.

## Autonomous Hosted Mode

When hosted on GitHub Pages (or any static host without backend reachability),
the playground can still run end-to-end via built-in demo responses:
- health checks;
- run/bounds resolution;
- suggestions;
- map analytics;
- journey analytics.

This mode is activated automatically when hosted remotely and `API Base URL`
points to localhost.

## Troubleshooting

- **No sample device found**
  - Verify selected time window is within active run bounds.
  - Re-run pipeline for `raw_pings.csv` if lineage is empty.
- **Map/Journey returns empty**
  - Check `run_id` was resolved.
  - Increase `limit` and widen date/time window.
  - Confirm API server and DB artifacts are from same recent run.
- **Live Monitor not updating**
  - Ensure `Live updates` is enabled.
  - Confirm API base URL points to running server.
- **Charts not rendering**
  - Check response payload shape in `Raw API Response`.
  - Verify browser console for Chart.js runtime errors.

## Evidence in Code

- `reviewer_site/index.html`
- `reviewer_site/app.js`
- `reviewer_site/styles.css`
- `src/api/app.py`
- `src/storage/visit_store.py`
