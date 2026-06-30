"""Generic time-series data generator for WaterTAP deployment models.

Drives any model under ``deployments/WATERTAP/models/<name>/`` that exposes the
standard interface:

    build-and-solve.py    build() -> m ,  change_inputs(m, d) ,  solve(m)
    generate-values.py    generate_new_values(ts, rng) -> dict
    watertap-mapping.json  { "namespace": ..., "properties": {urn: "fs.<path>"} }

It walks ``--N`` timestamps spaced ``-T`` apart, re-solves the model at each, and
writes **wide** parquet snapshots (one column per mapped property, one row per
timestamp) into ``models/<name>/data/`` — a new file every ``-X`` rows.

Usage:
    python data-generator.py seawater-ro --N 168 -T 1h
    python data-generator.py simple-pipe --N 100000 -X 50000 --seed 7
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType

import numpy as np
import pandas as pd
from pyomo.environ import value

HERE = Path(__file__).resolve().parent          # .../WATERTAP/scripts
MODELS_DIR = HERE.parent / "models"             # .../WATERTAP/models
DEFAULT_START = "2025-01-01T00:00:00"


# --------------------------------------------------------------------------- #
# Model interface loading
# --------------------------------------------------------------------------- #
def load_module(py_path: Path, mod_name: str) -> ModuleType:
    """Import a model file by path (filenames use hyphens, so not importable)."""
    spec = importlib.util.spec_from_file_location(mod_name, py_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {py_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def resolve_model_dir(model: str) -> Path:
    """Accept a model folder name (under models/) or an explicit path."""
    for cand in (MODELS_DIR / model, Path(model)):
        if cand.is_dir():
            return cand.resolve()
    raise SystemExit(f"model folder not found: {model!r} (looked in {MODELS_DIR})")


# --------------------------------------------------------------------------- #
# Property extraction
# --------------------------------------------------------------------------- #
def extract_properties(m, properties: dict, namespace: str) -> dict:
    """Evaluate every mapped Pyomo path to a float, keyed by short property name."""
    ns = {"fs": m.fs}
    row = {}
    for urn, path in properties.items():
        col = urn[len(namespace):] if urn.startswith(namespace) else urn
        row[col] = value(eval(path, {"__builtins__": {}}, ns))
    return row


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _parse_interval(text: str) -> timedelta:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd])\s*", text.lower())
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid interval {text!r}; use e.g. 30m, 1h, 2d, 90s"
        )
    qty, unit = float(m.group(1)), m.group(2)
    return timedelta(seconds=qty * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit])


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("model", help="model folder name under models/ (e.g. seawater-ro)")
    p.add_argument("--N", type=int, default=24, help="number of data points to generate")
    p.add_argument(
        "-T", "--interval", type=_parse_interval, default="1h",
        help="spacing between points (e.g. 30m, 1h, 1d). Default 1h.",
    )
    p.add_argument(
        "-X", "--points-per-file", type=int, default=50_000,
        help="max rows per parquet file; a new file starts every X points",
    )
    p.add_argument("--start", default=DEFAULT_START, help="ISO start timestamp")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    return p.parse_args(argv)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv=None) -> None:
    args = parse_args(argv)
    model_dir = resolve_model_dir(args.model)
    prefix = model_dir.name

    bs = load_module(model_dir / "build-and-solve.py", f"{prefix}_build_solve")
    gv = load_module(model_dir / "generate-values.py", f"{prefix}_generate_values")
    mapping = json.loads((model_dir / "watertap-mapping.json").read_text())
    namespace, properties = mapping["namespace"], mapping["properties"]

    out_dir = model_dir / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    start = datetime.fromisoformat(args.start)
    rng = np.random.RandomState(args.seed)

    print(f"Building model '{prefix}' ...")
    m = bs.build()
    if m is None:
        raise SystemExit(
            f"{prefix}/build-and-solve.py: build() returned None (it must return the model)"
        )

    batch: list[dict] = []
    batch_start = 0          # index of the first row in the current batch
    files_written = 0

    def flush(last_idx: int) -> None:
        nonlocal batch, files_written
        if not batch:
            return
        df = pd.DataFrame(batch)
        ts0 = datetime.fromisoformat(batch[0]["timestamp"])
        fname = (
            out_dir
            / f"{prefix}_{batch_start:06d}-{last_idx:06d}_{ts0.strftime('%Y%m%dT%H%M%S')}.parquet"
        )
        df.to_parquet(fname, index=False)
        files_written += 1
        print(f"  wrote {len(batch)} rows x {len(df.columns)} cols -> {fname.name}")
        batch = []

    for i in range(args.N):
        ts = start + i * args.interval
        drivers = gv.generate_new_values(ts, rng)
        bs.change_inputs(m, drivers)

        try:
            bs.solve(m)
        except Exception as exc:  # noqa: BLE001 - keep the series going
            print(f"[{i:06d} {ts.isoformat()}] solve failed ({exc}); rebuilding")
            try:
                m = bs.build()
                bs.change_inputs(m, drivers)
                bs.solve(m)
            except Exception as exc2:  # noqa: BLE001
                print(f"[{i:06d} {ts.isoformat()}] retry failed ({exc2}); skipping")
                continue

        row = {"timestamp": ts.isoformat()}
        row.update(extract_properties(m, properties, namespace))

        if not batch:
            batch_start = i
        batch.append(row)

        if len(batch) >= args.points_per_file:
            flush(i)

    flush(args.N - 1)
    print(f"Done. {args.N} points -> {files_written} file(s) in {out_dir}")


if __name__ == "__main__":
    main()
