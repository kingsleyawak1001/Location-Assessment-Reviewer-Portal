from __future__ import annotations

import argparse
from pathlib import Path

from src.config.settings import AppSettings
from src.pipeline.phase1 import run_phase1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Location ETL pipeline runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run pipeline actions")
    run_subparsers = run_parser.add_subparsers(dest="phase", required=True)

    phase1_parser = run_subparsers.add_parser("phase1", help="Run Phase 1 foundations pipeline")
    phase1_parser.add_argument("--input", required=True, type=Path, help="Path to source CSV file")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = AppSettings()

    if args.command == "run" and args.phase == "phase1":
        result = run_phase1(args.input, settings=settings)
        if result is None:
            print("Skipped: already successfully processed input file checksum.")
        else:
            print(
                f"Run {result.run_id}: total={result.total_in}, "
                f"accepted={result.accepted_count}, rejected={result.rejected_count}"
            )
        return
    parser.error("Unsupported command.")


if __name__ == "__main__":
    main()

