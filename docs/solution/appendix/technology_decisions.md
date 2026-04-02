# Technology Decisions Appendix

This appendix provides a structured comparison of technology decisions with explicit
rationale, risks, and migration considerations.

## Decision Principles

- Prefer deterministic and explainable behavior in prototype phase.
- Keep external data/API contracts stable to reduce migration risk.
- Shift complexity from code rewrites to platform evolution.

## Two-Lens Comparison Matrix

| Concern | Prototype Choice | Why Chosen (Prototype) | Production-Grade Direction | Benefits | Risks / Limits | Migration Notes |
|---|---|---|---|---|---|---|
| Runtime / compute | Python 3.12 + Polars | Fast delivery and readable pipeline logic | Orchestrated distributed execution with partition-aware workers | High developer velocity and clear transformations | Limited horizontal scaling for very high throughput | Preserve transform contracts and scale execution backend first |
| Persistence | SQLite | Zero infrastructure and deterministic local reproducibility | Columnar analytics store + metadata/control DB | Very low setup overhead | Write concurrency and scale ceilings | Migrate semantically: `visits`, lineage, and aggregates |
| API serving | FastAPI + Uvicorn | Typed contracts and rapid endpoint implementation | API gateway + autoscaled query services + policy controls | Fast implementation and testability | Limited hardening without gateway/policy layer | Keep response contracts stable, add gateway incrementally |
| Configuration | Pydantic settings | Type-safe env-driven configuration | Centralized config + secret manager + governance | Low misconfiguration risk in prototype | Operational sprawl at larger team/environment counts | Move source of truth to config platform, keep model validation |
| Quality gates | ruff + mypy + pytest + e2e runner | Tight feedback loop and strong local confidence | CI/CD gates with perf and data-SLO enforcement | Fast regression detection | No load/perf guardrails by default | Add load and contract tests before scale jumps |
| Observability | Structured logs + step timings | Enough for debugging and bottleneck inspection | Metrics/logs/traces stack with SLO dashboards | Quick diagnosis during development | Limited long-term operational visibility | Add telemetry before aggressive throughput scaling |

## Why This Is Structurally Correct

- Prototype choices minimize setup friction and maximize correctness visibility.
- Production choices maximize elasticity, reliability, and cost efficiency.
- Migration path is additive: infrastructure and storage evolve, contracts stay stable.

