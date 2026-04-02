from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.config.settings import AppSettings
from src.pipeline.batch_phase1 import run_phase1_batch
from src.pipeline.compare import compare_phase1_algorithms
from src.pipeline.phase1 import run_phase1
from src.storage.visit_store import VisitStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Location ETL pipeline runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run pipeline actions")
    run_subparsers = run_parser.add_subparsers(dest="phase", required=True)
    compare_parser = subparsers.add_parser("compare", help="Compare algorithm variants")
    compare_subparsers = compare_parser.add_subparsers(dest="phase", required=True)
    query_parser = subparsers.add_parser("query", help="Inspect persisted outputs")
    query_subparsers = query_parser.add_subparsers(dest="phase", required=True)
    export_parser = subparsers.add_parser("export", help="Export persisted analytics")
    export_subparsers = export_parser.add_subparsers(dest="phase", required=True)

    phase1_parser = run_subparsers.add_parser(
        "phase1",
        help="Run Phase 1 foundations pipeline",
    )
    source_group = phase1_parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--input", type=Path, help="Path to source CSV file")
    source_group.add_argument(
        "--input-dir",
        type=Path,
        help="Path to source directory with CSV files",
    )
    phase1_parser.add_argument("--glob", default="*.csv", help="Glob pattern for --input-dir mode")
    phase1_parser.add_argument(
        "--max-workers", type=int, default=4, help="Parallel workers for --input-dir mode"
    )

    compare_phase1_parser = compare_subparsers.add_parser(
        "phase1",
        help="Compare primary vs alternative Phase 1 algorithms",
    )
    compare_phase1_parser.add_argument("--input", required=True, type=Path)
    compare_phase1_parser.add_argument("--runs", type=int, default=1)

    query_phase3_parser = query_subparsers.add_parser(
        "phase3",
        help="Inspect Phase 3 visit storage and lineage",
    )
    query_phase3_parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional SQLite path for Phase 3 visit store",
    )
    query_phase3_parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional run_id to preview visits for a specific run",
    )
    query_phase3_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max records to display",
    )
    query_phase4_parser = query_subparsers.add_parser(
        "phase4",
        help="Return analytics summary from Phase 3 visit storage",
    )
    query_phase4_parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional SQLite path for Phase 3 visit store",
    )
    query_phase4_parser.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="Run identifier to aggregate",
    )
    query_phase4_parser.add_argument(
        "--top-devices-limit",
        type=int,
        default=5,
        help="How many devices to include in top_devices",
    )

    export_phase4_parser = export_subparsers.add_parser(
        "phase4",
        help="Export Phase 4 analytics to JSON",
    )
    export_phase4_parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional SQLite path for Phase 3 visit store",
    )
    export_phase4_parser.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="Run identifier to export",
    )
    export_phase4_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination JSON file path",
    )
    export_phase4_parser.add_argument(
        "--top-devices-limit",
        type=int,
        default=5,
        help="How many devices to include when materialized aggregate does not exist",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = AppSettings()

    if args.command == "run" and args.phase == "phase1":
        if args.input is not None:
            result = run_phase1(args.input, settings=settings)
            if result is None:
                print("Skipped: already successfully processed input file checksum.")
            else:
                sorted_steps = ", ".join(
                    f"{step}={duration_ms:.3f}ms"
                    for step, duration_ms in sorted(result.step_durations_ms.items())
                )
                print(
                    f"Run {result.run_id}: total={result.total_in}, "
                    f"accepted={result.accepted_count}, rejected={result.rejected_count}, "
                    f"duration={result.total_duration_ms:.3f}ms"
                )
                print(f"Step timings: {sorted_steps}")
        else:
            if args.max_workers < 1:
                parser.error("--max-workers must be >= 1")
            batch_result = run_phase1_batch(
                args.input_dir,  # argparse enforces non-null in this branch.
                pattern=args.glob,
                max_workers=args.max_workers,
                settings=settings,
            )
            totals = batch_result["totals"]
            print(
                "Batch run "
                f"{batch_result['run_id']}: discovered={totals['discovered_files']}, "
                f"success={totals['success']}, "
                f"skipped={totals['skipped']}, "
                f"failed={totals['failed']}, "
                f"duration={batch_result['total_duration_ms']:.3f}ms"
            )
            print(f"Batch report: {batch_result['report_path']}")
        return
    if args.command == "compare" and args.phase == "phase1":
        comparison = compare_phase1_algorithms(args.input, settings=settings, runs=args.runs)
        print(
            f"Comparison {comparison['run_id']}: winner={comparison['winner']}, "
            f"quality_consistent={comparison['quality_consistent_between_algorithms']}, "
            f"report={comparison['report_path']}"
        )
        print(
            "Average duration ms -> "
            f"primary={comparison['comparison']['primary']['avg_total_duration_ms']}, "
            f"alternative={comparison['comparison']['alternative']['avg_total_duration_ms']}"
        )
        return
    if args.command == "query" and args.phase == "phase3":
        if args.limit < 1:
            parser.error("--limit must be >= 1")
        db_path = args.db_path if args.db_path is not None else settings.phase3_db_path
        store = VisitStore(db_path)
        if args.run_id is None:
            payload_phase3: dict[str, Any] = {
                "db_path": str(db_path.resolve()),
                "lineage_runs": store.get_lineage_runs(limit=args.limit),
            }
        else:
            payload_phase3 = {
                "db_path": str(db_path.resolve()),
                "run_id": args.run_id,
                "visits_preview": store.get_visits_for_run(args.run_id, limit=args.limit),
            }
        print(json.dumps(payload_phase3, indent=2))
        return
    if args.command == "query" and args.phase == "phase4":
        if args.top_devices_limit < 1:
            parser.error("--top-devices-limit must be >= 1")
        db_path = args.db_path if args.db_path is not None else settings.phase3_db_path
        store = VisitStore(db_path)
        materialized = store.get_materialized_run_aggregate(args.run_id)
        analytics_payload = (
            materialized
            if materialized is not None
            else store.get_run_analytics(
                run_id=args.run_id,
                top_devices_limit=args.top_devices_limit,
            )
        )
        payload_phase4: dict[str, Any] = {
            "db_path": str(db_path.resolve()),
            "analytics": analytics_payload,
            "materialized": materialized is not None,
        }
        print(json.dumps(payload_phase4, indent=2))
        return
    if args.command == "export" and args.phase == "phase4":
        if args.top_devices_limit < 1:
            parser.error("--top-devices-limit must be >= 1")
        db_path = args.db_path if args.db_path is not None else settings.phase3_db_path
        store = VisitStore(db_path)
        materialized = store.get_materialized_run_aggregate(args.run_id)
        analytics = (
            materialized
            if materialized is not None
            else store.get_run_analytics(
                run_id=args.run_id,
                top_devices_limit=args.top_devices_limit,
            )
        )
        payload_export: dict[str, Any] = {
            "db_path": str(db_path.resolve()),
            "run_id": args.run_id,
            "materialized": materialized is not None,
            "analytics": analytics,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload_export, indent=2), encoding="utf-8")
        print(f"Exported analytics to {args.output.resolve()}")
        return
    parser.error("Unsupported command.")


if __name__ == "__main__":
    main()

