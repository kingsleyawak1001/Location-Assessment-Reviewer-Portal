## API / Query Design

This document provides the required query design for the 2 core use cases from `assessment.md`.

### Goals

- Expose query-ready visit analytics (not raw pings).
- Support bounded, low-latency queries for map and journey workloads.
- Keep response contracts explicit and stable.

### Data Source

- Primary source: processed `visits` and `run_aggregates` in `artifacts/visits.db`.
- Query paths in prototype:
  - `uv run phase1 query phase4 ...` (analytics)
  - `uv run phase1 query phase3 ...` (visit preview / lineage)
- Export path:
  - `uv run phase1 export phase4 ...` (JSON artifact for downstream consumers)

---

## Use Case 1: Location Analytics / Heatmap

### Intent

Return aggregated movement metrics for a geographic area and time window.

### Prototype Query Contract (CLI)

The current prototype stores representative location per visit and can aggregate a run:

```bash
uv run phase1 query phase4 \
  --db-path artifacts/visits.db \
  --run-id <run_id> \
  --top-devices-limit 5
```

### Production REST Contract (design)

```bash
GET /api/map/data?start_date=2025-01-01&end_date=2025-01-31&west=-87.7&east=-87.5&south=41.8&north=42.0&movement_type=stay&min_visits=5
```

### Example Response

```json
{
  "status": "success",
  "data": {
    "window": {
      "start_date": "2025-01-01",
      "end_date": "2025-01-31"
    },
    "bbox": {
      "west": -87.7,
      "east": -87.5,
      "south": 41.8,
      "north": 42.0
    },
    "movement_type": "stay",
    "cells": [
      {
        "hex_id": "882a100d6dfffff",
        "total_visits": 1240,
        "unique_devices": 212,
        "avg_duration_s": 1842.6,
        "stay_count": 1179,
        "passby_count": 61,
        "earliest_visit": "2025-01-01T08:00:00Z",
        "latest_visit": "2025-01-31T18:30:00Z"
      }
    ]
  }
}
```

### Query Guards

- Required time window.
- Bounded bbox area and max window length.
- Pagination for large cell sets.
- Request timeout and rate limiting.

---

## Use Case 2: Device Journey / Visit Details

### Intent

Return a device journey (ordered visit sequence) and details for a selected location context.

### Prototype Query Contract (CLI)

Preview visits for a run:

```bash
uv run phase1 query phase3 \
  --db-path artifacts/visits.db \
  --run-id <run_id> \
  --limit 10
```

### Production REST Contract (design)

```bash
GET /api/devices/{device_id}/journey?start_ts=2025-01-01T00:00:00Z&end_ts=2025-01-02T00:00:00Z&limit=200
```

### Example Response

```json
{
  "status": "success",
  "data": {
    "device_id": "69ce7ee7-7e59-4773-8353-c1e132f5f31b",
    "window": {
      "start_ts": "2025-01-01T00:00:00Z",
      "end_ts": "2025-01-02T00:00:00Z"
    },
    "journey": [
      {
        "visit_id": "69ce7..._34_1735718400",
        "visit_kind": "stay",
        "stay_type": "work",
        "start_ts_utc": "2025-01-01T08:00:00Z",
        "end_ts_utc": "2025-01-01T12:30:00Z",
        "duration_seconds": 16200,
        "ping_count": 26,
        "representative_latitude": 41.8786,
        "representative_longitude": -87.6291
      }
    ]
  }
}
```

### Query Guards

- Device filter + bounded time window.
- `limit` and cursor pagination.
- Optional include/exclude `pass_by`.
- Server-side sorting by `start_ts_utc`.

---

## Caching and Performance Strategy (design)

- Cache key dimensions: query type + normalized filters + window.
- Hot-path TTL cache for heatmap responses.
- Pre-aggregated tables for frequent map windows.
- Fallback to base `visits` scans for cold queries.

## Rate Limits and Reliability (design)

- Token bucket per API key and per IP.
- Retry-safe read endpoints.
- Circuit breaker and stale-cache fallback for backend pressure.
