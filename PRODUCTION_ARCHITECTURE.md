## Production Architecture

This document covers the production-scale design for billions of pings/day and thousands of files/day.

## 1) End-to-End Data Flow

```mermaid
flowchart LR
    A[Object Storage: raw CSV files] --> B[Ingestion Workers]
    B --> C[Quality Validation]
    C --> D[Visit Transformation]
    D --> E[Processed Visits Store]
    D --> F[Lineage and Run Metadata]
    E --> G[Materialized Aggregates]
    G --> H[Query/API Layer]
    H --> I[Consumers: Maps, Analytics, Exports]
```

## 2) Runtime Architecture

```mermaid
flowchart TB
    subgraph Orchestration
      O1[Scheduler / DAG Orchestrator]
      O2[Task Queue]
    end

    subgraph Compute
      W1[File Workers]
      W2[Transform Workers]
      W3[Aggregation Workers]
    end

    subgraph Storage
      S1[(Object Storage)]
      S2[(Analytics DB)]
      S3[(Metadata DB)]
      S4[(Cache)]
    end

    subgraph Serving
      A1[API Gateway]
      A2[Query Service]
    end

    O1 --> O2
    O2 --> W1
    O2 --> W2
    O2 --> W3
    W1 --> S1
    W2 --> S2
    W2 --> S3
    W3 --> S2
    A1 --> A2
    A2 --> S2
    A2 --> S4
    A2 --> S3
```

## 3) ETL Pipeline Architecture

- File-level fan-out with worker pools.
- Checkpointed and idempotent processing via manifest/state machine.
- Retry with failure isolation at file/chunk scope.
- Backfill by partition window, not full reprocess by default.

## 4) Database and Storage Architecture

- Analytics DB for visits and aggregates (columnar in production).
- Metadata DB for runs, lineage, and processing state.
- Partition by date; cluster by geography/device dimensions.
- Hot aggregate tables for map analytics.

## 5) API and Query Layer

- Read-only query service for visit analytics.
- Bounded contracts (time-range, bbox, limits, pagination).
- Cache for frequent heatmap and summary requests.
- Rate limiting and query guards to protect backend.

## 6) Scaling Strategy

- 1M/day: single region, modest worker pool.
- 1B/day: autoscaled workers, partitioned compute queues, aggressive pre-aggregation.
- 10B+/day: multi-cluster storage, tiered retention, workload isolation by tenant/use-case.

## 7) Reliability, Monitoring, and Cost

- SLOs on ingest latency, successful run ratio, API p95.
- Metrics/logs/traces for pipeline and query service.
- DLQ and replay pipeline for failed units.
- Cost control via incremental processing, storage tiering, and autoscaling bounds.
