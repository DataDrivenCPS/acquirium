"""Generate fake SCADA-style CSV exports for the dummy plant.

Each file mimics the DPR trailer raw exports:
- row 1: metadata banner (``Date:,M/D/YYYY,Time:,H:MM AM,Operator:,``)
- row 2: column header
- rows 3+: data at fixed cadence (default 10s) — numeric or status

Usage:
    python generate_data.py                     # 5 files × 120 rows into ./raw
    python generate_data.py --num-files 3 --rows-per-file 60
"""

from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
COLUMN_LOOKUP_PATH = _THIS_DIR / "property_to_column.json"
DEFAULT_OUTPUT_DIR = _THIS_DIR / "raw"
FILE_STEM = "Dummy Plant SCADA v1.0"


def _load_columns() -> list[str]:
    with open(COLUMN_LOOKUP_PATH) as f:
        mapping: dict[str, str] = json.load(f)
    return list(mapping.values())


def _classify(col: str) -> str:
    """Return a category tag used to pick a value distribution."""
    low = col.lower()
    if low.endswith("state") or " state" in low:
        return "state"
    if "(gpm)" in low:
        return "gpm"
    if "(psi)" in low:
        return "psi"
    if "(gal)" in low:
        return "gal"
    if "(c)" in low or "temperature" in low:
        return "tempC"
    if low.endswith("ph") or " ph" in low:
        return "ph"
    if "(ntu)" in low:
        return "ntu"
    if "(ppm)" in low:
        return "ppm"
    if "(mg/l)" in low:
        return "mgL"
    if "(ml/min)" in low:
        return "mlMin"
    if "speed (%)" in low or "(%)" in low:
        return "pct"
    return "num"


def _sample(category: str, rng: random.Random, t_idx: int) -> object:
    """Return a column value. Some categories use a slow sinusoidal drift."""
    if category == "state":
        return rng.choice(("ON", "OFF"))
    if category == "gpm":
        base = 4.0 + 1.5 * math.sin(t_idx / 30.0)
        return round(max(0.0, base + rng.gauss(0.0, 0.4)), 3)
    if category == "psi":
        base = 10.0 + 1.0 * math.cos(t_idx / 40.0)
        return round(max(0.0, base + rng.gauss(0.0, 0.3)), 3)
    if category == "gal":
        base = 2500.0 + 200.0 * math.sin(t_idx / 60.0)
        return round(max(0.0, base + rng.gauss(0.0, 10.0)), 3)
    if category == "tempC":
        base = 20.0 + 1.5 * math.sin(t_idx / 90.0)
        return round(base + rng.gauss(0.0, 0.15), 3)
    if category == "ph":
        return round(7.0 + 0.3 * math.sin(t_idx / 50.0) + rng.gauss(0.0, 0.05), 3)
    if category == "ntu":
        return round(max(0.0, 0.5 + 0.3 * math.sin(t_idx / 25.0) + rng.gauss(0.0, 0.1)), 3)
    if category == "ppm":
        return round(max(0.0, 3.0 + 1.0 * math.sin(t_idx / 35.0) + rng.gauss(0.0, 0.2)), 3)
    if category == "mgL":
        return round(max(0.0, 1.0 + 0.5 * math.sin(t_idx / 45.0) + rng.gauss(0.0, 0.05)), 3)
    if category == "mlMin":
        return round(max(0.0, 200.0 + 50.0 * math.sin(t_idx / 20.0) + rng.gauss(0.0, 8.0)), 3)
    if category == "pct":
        return round(max(0.0, min(100.0, 60.0 + 15.0 * math.sin(t_idx / 30.0) + rng.gauss(0.0, 2.0))), 3)
    return round(rng.gauss(0.0, 1.0), 3)


def _format_value(value: object) -> str:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return str(value)


def _write_file(
    path: Path,
    columns: list[str],
    categories: list[str],
    start_ts: datetime,
    rows: int,
    cadence_seconds: int,
    rng: random.Random,
    nan_rate: float,
) -> None:
    banner_date = f"{start_ts.month}/{start_ts.day}/{start_ts.year}"
    banner_time = start_ts.strftime("%I:%M %p").lstrip("0")
    with open(path, "w", newline="") as f:
        f.write(f"Date:,{banner_date},Time:,{banner_time},Operator:,\n")
        f.write("Date,Time," + ",".join(columns) + "\n")
        for i in range(rows):
            ts = start_ts + timedelta(seconds=i * cadence_seconds)
            row_date = f"{ts.month}/{ts.day}/{ts.year}"
            row_time = ts.strftime("%I:%M:%S %p").lstrip("0")
            values: list[str] = []
            for col, cat in zip(columns, categories):
                if cat != "state" and rng.random() < nan_rate:
                    values.append("NaN")
                    continue
                values.append(_format_value(_sample(cat, rng, i)))
            f.write(row_date + "," + row_time + "," + ",".join(values) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-files", type=int, default=5)
    parser.add_argument("--rows-per-file", type=int, default=120)
    parser.add_argument("--cadence-seconds", type=int, default=10)
    parser.add_argument(
        "--start",
        type=str,
        default="2026-06-01 09:00",
        help="First timestamp (YYYY-MM-DD HH:MM, 24h).",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--nan-rate", type=float, default=0.01)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    columns = _load_columns()
    categories = [_classify(c) for c in columns]

    args.output_dir.mkdir(parents=True, exist_ok=True)

    start = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
    stem_ts = start.strftime("%Y.%m.%d-%H%M")
    base_name = f"{FILE_STEM} - {stem_ts}"

    file_span = timedelta(seconds=args.rows_per_file * args.cadence_seconds)
    cursor = start
    for n in range(args.num_files):
        suffix = "" if n == 0 else f"-{n}"
        path = args.output_dir / f"{base_name}{suffix}.csv"
        _write_file(
            path=path,
            columns=columns,
            categories=categories,
            start_ts=cursor,
            rows=args.rows_per_file,
            cadence_seconds=args.cadence_seconds,
            rng=rng,
            nan_rate=args.nan_rate,
        )
        print(f"wrote {path.name}  ({args.rows_per_file} rows, start={cursor:%Y-%m-%d %H:%M:%S})")
        cursor += file_span


if __name__ == "__main__":
    main()
