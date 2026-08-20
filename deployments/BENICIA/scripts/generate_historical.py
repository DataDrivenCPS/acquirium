"""Generate historical Benicia data as wide parquet.

Introspects a Benicia model TTL for its properties and writes one **wide**
parquet snapshot — a ``timestamp`` column plus one column per property (named by
the property's local name) — into the output directory. This is the file
``parquet_driver.py:BeniciaParquetDriver`` replays.

The per-property series come from ``benicia_generator`` (the same logic the live
simulator driver uses), so historical and live data are statistically
consistent and land on the same ontology points.

Usage (from the repo root):
    python deployments/BENICIA/scripts/generate_historical.py --N 43200 -T 60s

Defaults produce ~1 month at 1-minute spacing into ``deployments/BENICIA/data/historical``.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import rdflib


from benicia_generator import (
    build_state_for_property,
    get_properties,
    is_enumeration,
    local_name,
)

HERE = Path(__file__).resolve().parent          # .../BENICIA/scripts
DEPLOYMENT = HERE.parent                          # .../BENICIA


def _parse_interval(text: str) -> timedelta:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd])\s*", text.lower())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid interval {text!r}; use e.g. 30m, 1h, 2d, 90s")
    qty, unit = float(m.group(1)), m.group(2)
    return timedelta(seconds=qty * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit])


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--model", default=str(DEPLOYMENT / "benicia-model-100.ttl"),
                   help="Path to the Benicia model TTL.")
    p.add_argument("--output-dir", default=str(DEPLOYMENT / "data" / "historical"),
                   help="Directory to write the parquet file into.")
    p.add_argument("--N", type=int, default=43_200, help="Number of rows to generate (default ~1 month @ 1-min).")
    p.add_argument("-T", "--interval", type=_parse_interval, default="60s",
                   help="Spacing between samples (e.g. 30s, 1m, 1h). Default 60s.")
    p.add_argument("--start", default="2025-01-01T00:00:00", help="ISO-8601 start timestamp.")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducible output.")
    p.add_argument("--excursion-rate", type=float, default=0.02,
                   help="Probability per sample of an excursion beyond typical limits.")
    p.add_argument("--step-frac", type=float, default=0.02,
                   help="Random-walk step size as a fraction of the range width.")
    return p.parse_args(argv)


def main(argv=None) -> None:
    import random

    args = parse_args(argv)
    if args.N <= 0:
        raise SystemExit("--N must be a positive integer.")
    if not (0.0 <= args.excursion_rate <= 1.0):
        raise SystemExit("--excursion-rate must be between 0 and 1.")
    if args.step_frac <= 0.0:
        raise SystemExit("--step-frac must be positive.")

    model_path = Path(args.model)
    graph = rdflib.Graph().parse(model_path)
    properties = get_properties(graph)

    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    # One SeriesState per property (None for enumerations, which emit 0/1).
    states = {}
    enums = []
    for prop in properties:
        name = local_name(prop)
        if is_enumeration(graph, prop):
            enums.append(name)
        else:
            states[name] = build_state_for_property(rng, graph, prop, step_frac=args.step_frac)

    timestamps = [start + i * args.interval for i in range(args.N)]
    columns: dict[str, list] = {"timestamp": timestamps}
    for name, state in states.items():
        columns[name] = [state.next_value(rng, args.excursion_rate) for _ in range(args.N)]
    for name in enums:
        columns[name] = [float(rng.choice([0, 1])) for _ in range(args.N)]

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = out_dir / f"benicia_{args.N:06d}_{start.strftime('%Y%m%dT%H%M%S')}.parquet"
    pl.DataFrame(columns).write_parquet(fname)

    print(f"Wrote {args.N} rows x {len(columns)} cols -> {fname}")


if __name__ == "__main__":
    main()
