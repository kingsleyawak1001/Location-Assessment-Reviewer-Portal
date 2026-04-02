# System Diagrams Appendix

This appendix contains standardized diagrams for architecture communication.
Each diagram is paired with usage notes so it can be reused in reports and presentations.

## A) ETL Control and Processing Flow

```mermaid
flowchart TB
    A[Input datasets] --> B[CLI operation router]
    B --> C[Idempotency guard]
    C --> D[Ingestion and normalization]
    D --> E[Quality gate]
    E --> F[Visit transformation]
    F --> G[Artifact persistence]
    G --> H[Visit storage and lineage]
    H --> I[Materialized run analytics]
```

**Use this diagram when:** explaining how a single run progresses from raw input to persisted outputs.

## B) Storage and Lineage Topology

```mermaid
flowchart LR
    A[Derived visits] --> B[(visits fact table)]
    C[Run metadata] --> D[(visit_lineage table)]
    B --> E[(run_aggregates table)]
    D --> E
```

**Use this diagram when:** explaining schema roles and auditability boundaries.

## C) Serving Paths (HTTP and CLI)

```mermaid
flowchart LR
    A[Consumer] --> B{Access mode}
    B -->|HTTP| C[FastAPI endpoints]
    B -->|CLI| D[query/export commands]
    C --> E[VisitStore read layer]
    D --> E
    E --> F[Structured payload / export]
```

**Use this diagram when:** clarifying that API and CLI share one read model.

## D) Production Scale Evolution

```mermaid
flowchart LR
    A[Stage A\nLocal prototype] --> B[Stage B\nSingle-region production]
    B --> C[Stage C\nPartitioned multi-worker scale]
    C --> D[Stage D\nMulti-cluster analytics platform]
```

**Use this diagram when:** discussing capacity planning and migration sequencing.

## Diagram Usage Rules

- Keep node names semantic (role-oriented, not implementation trivia).
- Keep arrows directional by data or control ownership.
- Avoid crossing lines unless the relationship is essential.
- Use one diagram per question (execution, storage, serving, or scaling).
