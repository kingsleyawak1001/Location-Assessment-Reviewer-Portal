from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GenerationConfig:
    sizes_mb: list[int]
    max_ts_shift_seconds: int = 3600
    max_lat_lon_shift: float = 0.003
    max_accuracy_shift: int = 6
    seed: int = 42
    device_id_remap_probability: float = 0.0
    filename_prefix: str = "raw_pings_similar"


FIELDNAMES = ["device_id", "ts_utc", "latitude", "longitude", "accuracy_m"]


def _bounded(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_sizes_mb(raw: str) -> list[int]:
    sizes = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not sizes:
        raise ValueError("sizes list cannot be empty")
    if any(size < 1 for size in sizes):
        raise ValueError("all sizes must be >= 1 MB")
    return sizes


def _load_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Config JSON must be an object.")
    return payload


def _build_config(args: argparse.Namespace) -> GenerationConfig:
    file_config: dict[str, Any] = {}
    if args.config is not None:
        file_config = _load_config_file(args.config)

    sizes_raw = str(file_config.get("sizes_mb", args.sizes_mb))
    sizes_mb = _parse_sizes_mb(sizes_raw)

    return GenerationConfig(
        sizes_mb=sizes_mb,
        max_ts_shift_seconds=int(
            file_config.get("max_ts_shift_seconds", args.max_ts_shift_seconds)
        ),
        max_lat_lon_shift=float(file_config.get("max_lat_lon_shift", args.max_lat_lon_shift)),
        max_accuracy_shift=int(file_config.get("max_accuracy_shift", args.max_accuracy_shift)),
        seed=int(file_config.get("seed", args.seed)),
        device_id_remap_probability=float(
            file_config.get("device_id_remap_probability", args.device_id_remap_probability)
        ),
        filename_prefix=str(file_config.get("filename_prefix", args.filename_prefix)),
    )


def _mutate_row(
    row: dict[str, str],
    cfg: GenerationConfig,
    random_device_id: str,
) -> dict[str, str]:
    ts = int(float(row["ts_utc"]))
    lat = float(row["latitude"])
    lon = float(row["longitude"])
    acc = int(float(row["accuracy_m"]))

    ts_shift = random.randint(-cfg.max_ts_shift_seconds, cfg.max_ts_shift_seconds)
    lat_shift = random.uniform(-cfg.max_lat_lon_shift, cfg.max_lat_lon_shift)
    lon_shift = random.uniform(-cfg.max_lat_lon_shift, cfg.max_lat_lon_shift)
    acc_shift = random.randint(-cfg.max_accuracy_shift, cfg.max_accuracy_shift)

    mutated_ts = max(1, ts + ts_shift)
    mutated_lat = _bounded(lat + lat_shift, -90.0, 90.0)
    mutated_lon = _bounded(lon + lon_shift, -180.0, 180.0)
    mutated_acc = max(0, acc + acc_shift)

    if random.random() < cfg.device_id_remap_probability:
        device_id = random_device_id
    else:
        device_id = row["device_id"]

    return {
        "device_id": device_id,
        "ts_utc": str(mutated_ts),
        "latitude": f"{mutated_lat:.6f}",
        "longitude": f"{mutated_lon:.6f}",
        "accuracy_m": str(mutated_acc),
    }


def generate_similar_files(input_csv: Path, output_dir: Path, cfg: GenerationConfig) -> list[Path]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    output_dir.mkdir(parents=True, exist_ok=True)

    with input_csv.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        rows = list(reader)

    if not rows:
        raise ValueError("Input CSV is empty.")

    random.seed(cfg.seed)

    outputs: list[Path] = []
    for index, size_mb in enumerate(cfg.sizes_mb, start=1):
        target_bytes = size_mb * 1024 * 1024
        output_path = output_dir / f"{cfg.filename_prefix}_{size_mb}mb_{index}.csv"
        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=FIELDNAMES)
            writer.writeheader()
            while output_file.tell() < target_bytes:
                row = random.choice(rows)
                writer.writerow(
                    _mutate_row(
                        row,
                        cfg,
                        random_device_id=f"synthetic-{random.getrandbits(64):016x}",
                    )
                )

        outputs.append(output_path)

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate similar ping datasets by target size.")
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Path to source raw_pings.csv file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where generated CSV files will be created",
    )
    parser.add_argument(
        "--sizes-mb",
        default="1,5,10,50,100,250,500,1024",
        help="Comma-separated list of output file sizes in MB",
    )
    parser.add_argument("--max-ts-shift-seconds", type=int, default=3600)
    parser.add_argument("--max-lat-lon-shift", type=float, default=0.003)
    parser.add_argument("--max-accuracy-shift", type=int, default=6)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--device-id-remap-probability", type=float, default=0.0)
    parser.add_argument("--filename-prefix", default="raw_pings_similar")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional JSON config file. Overrides CLI generation params.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[3]
    source = args.input if args.input is not None else root / "raw_pings.csv"
    target_dir = args.output_dir if args.output_dir is not None else root / "generated_pings"
    config = _build_config(args)
    generated = generate_similar_files(source, target_dir, config)
    for path in generated:
        print(path)


if __name__ == "__main__":
    main()
