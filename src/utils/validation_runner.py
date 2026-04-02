from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    exit_code: int
    stdout: str
    stderr: str


def _run_command(command: list[str], cwd: Path) -> CommandResult:
    completed = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
    return CommandResult(
        command=command,
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _extract_report_metrics(run_dir: Path) -> dict[str, Any]:
    reports_dir = run_dir / "reports"
    report_files = sorted(reports_dir.glob("*_quality_report.json"))
    if not report_files:
        raise FileNotFoundError(f"No report file found in {reports_dir}")
    report = json.loads(report_files[-1].read_text(encoding="utf-8"))
    return {
        "report_path": str(report_files[-1]),
        "total_in": int(report["total_in"]),
        "accepted": int(report["accepted_count"]),
        "rejected": int(report["rejected_count"]),
        "visits": int(report["visits_count"]),
        "step_durations_ms": report["step_durations_ms"],
        "consistency_checks": report["consistency_checks"],
    }


def _run_e2e_for_input(project_root: Path, input_csv: Path, run_dir: Path) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PHASE1_ARTIFACTS_DIR"] = str(run_dir)
    env["PHASE1_MANIFEST_PATH"] = str(run_dir / "manifest.db")
    env["PHASE1_ACCEPTED_DIR"] = str(run_dir / "accepted")
    env["PHASE1_REJECTED_DIR"] = str(run_dir / "rejected")
    env["PHASE1_VISITS_DIR"] = str(run_dir / "visits")
    env["PHASE1_REPORTS_DIR"] = str(run_dir / "reports")
    env["PHASE1_PHASE3_DB_PATH"] = str(run_dir / "visits.db")
    command = ["python", "-m", "src.pipeline.cli", "run", "phase1", "--input", str(input_csv)]
    completed = subprocess.run(
        command,
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"E2E failed for {input_csv}.\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    metrics = _extract_report_metrics(run_dir)
    metrics["input_csv"] = str(input_csv)
    return metrics


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run mandatory validation on raw + generated datasets."
    )
    parser.add_argument(
        "--raw-input",
        type=Path,
        default=Path("../raw_pings.csv"),
        help="Primary raw dataset path",
    )
    parser.add_argument(
        "--additional-dir",
        type=Path,
        default=Path("../generated_pings"),
        help="Directory with additional generated CSV datasets",
    )
    parser.add_argument(
        "--additional-glob",
        default="*.csv",
        help="Glob for additional datasets directory",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/validation_runs"),
        help="Directory for validation reports",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[2]
    raw_input = (project_root / args.raw_input).resolve()
    additional_dir = (project_root / args.additional_dir).resolve()
    output_root = (project_root / args.output_dir).resolve()

    if not raw_input.exists():
        raise FileNotFoundError(f"Primary dataset not found: {raw_input}")
    if not additional_dir.exists():
        raise FileNotFoundError(f"Additional datasets directory not found: {additional_dir}")
    additional_inputs = sorted(
        path for path in additional_dir.glob(args.additional_glob) if path.is_file()
    )
    if not additional_inputs:
        raise ValueError(
            f"No additional datasets matched {args.additional_glob} in {additional_dir}"
        )

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_root = output_root / f"validation_{run_id}"
    run_root.mkdir(parents=True, exist_ok=True)

    command_results: list[dict[str, Any]] = []
    mandatory_commands = [
        ["ruff", "check", "."],
        ["mypy", "."],
        [
            "pytest",
            "-vv",
            "tests/test_ingestion.py",
            "tests/test_quality.py",
            "tests/test_pipeline_integration.py",
        ],
        ["pytest", "-vv", "tests/test_transformation.py"],
    ]
    for command in mandatory_commands:
        result = _run_command(command, cwd=project_root)
        command_results.append(
            {
                "command": " ".join(command),
                "exit_code": result.exit_code,
            }
        )
        if result.exit_code != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(command)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

    e2e_runs: list[dict[str, Any]] = []
    e2e_runs.append(_run_e2e_for_input(project_root, raw_input, run_root / "raw"))
    for dataset in additional_inputs:
        target = run_root / "additional" / dataset.stem
        e2e_runs.append(_run_e2e_for_input(project_root, dataset, target))

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "project_root": str(project_root),
        "primary_dataset": str(raw_input),
        "additional_dataset_count": len(additional_inputs),
        "additional_datasets": [str(path) for path in additional_inputs],
        "commands": command_results,
        "e2e_runs": e2e_runs,
    }
    report_json = run_root / "validation_report.json"
    report_md = run_root / "validation_report.md"
    report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Validation Report\n",
        f"- generated_at: {report['generated_at']}\n",
        f"- primary_dataset: `{report['primary_dataset']}`\n",
        f"- additional_dataset_count: {report['additional_dataset_count']}\n\n",
        "## Commands\n",
    ]
    for item in command_results:
        lines.append(f"- `{item['command']}` -> exit_code={item['exit_code']}\n")
    lines.append("\n## E2E Metrics\n")
    lines.append(
        "| input | total_in | accepted | rejected | visits | phase1_ok | "
        "phase2_ok | phase3_ok | phase4_ok |\n"
    )
    lines.append("|---|---:|---:|---:|---:|:---:|:---:|:---:|:---:|\n")
    for run in e2e_runs:
        checks = run["consistency_checks"]
        lines.append(
            f"| `{Path(run['input_csv']).name}` | {run['total_in']} | {run['accepted']} | "
            f"{run['rejected']} | {run['visits']} | "
            f"{checks['phase1_ok']} | {checks['phase2_ok']} | "
            f"{checks['phase3_ok']} | {checks['phase4_ok']} |\n"
        )
    report_md.write_text("".join(lines), encoding="utf-8")

    print(f"VALIDATION_JSON={report_json}")
    print(f"VALIDATION_MD={report_md}")


if __name__ == "__main__":
    main()
