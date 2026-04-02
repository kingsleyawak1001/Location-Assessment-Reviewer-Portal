from __future__ import annotations

import argparse
from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.batch_phase1 import run_phase1_batch
from src.pipeline.compare import compare_phase1_algorithms
from src.pipeline.phase1 import run_phase1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Location ETL pipeline runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run pipeline actions")
    run_subparsers = run_parser.add_subparsers(dest="phase", required=True)
    compare_parser = subparsers.add_parser("compare", help="Compare algorithm variants")
    compare_subparsers = compare_parser.add_subparsers(dest="phase", required=True)

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
    parser.error("Unsupported command.")


if __name__ == "__main__":
    main()

