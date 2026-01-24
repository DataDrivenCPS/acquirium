import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import math
import random
from typing import Optional

import polars as pl
import rdflib
from rdflib.namespace import RDF

from acquirium.internals.internals_namespaces import QUDT, QUDT_UNIT, S223


PROPERTY_TYPES = {
    S223.QuantifiableObservableProperty,
    S223.QuantifiableActuatableProperty,
}

# Existing fallbacks
UNIT_RANGES = {
    str(QUDT_UNIT.PH): (6.0, 9.0),
    str(QUDT_UNIT["MilliGM-PER-L"]): (0.0, 100.0),
    str(QUDT_UNIT["MicroGM-PER-L"]): (0.0, 100.0),
    str(QUDT_UNIT["CFU-PER-100ML"]): (0.0, 2000.0),
    str(QUDT_UNIT.NTU): (0.0, 100.0),
    str(QUDT_UNIT.GAL_US): (0.0, 1_000_000.0),
    str(QUDT_UNIT.A): (0.0, 200.0),
}

QUANTITY_KIND_RANGES = {
    "VolumeFlowRate": (0.0, 10.0),
    "Volume": (0.0, 1_000_000.0),
    "Density": (0.0, 5000.0),
    "Concentration": (0.0, 200.0),
    "Turbidity": (0.0, 100.0),
    "ElectricCurrent": (0.0, 200.0),
    "OpeningRatio": (0.0, 1.0),
    "Acidity": (6.0, 9.0),
}

# Permit informed targets and limits for Benicia specific property URIs.
# Values are used as guidance for realistic generation, not strict enforcement.
# Units are informational only here.
BENICIA_LIMITS = {
    # Influent
    "urn:ex/Influent_Pump-in-flow-rate": {"min": 0.0, "max": 4.5, "unit": "MGD", "center": 2.5},
    "urn:ex/Influent_Pump-in-cyanide": {"min": 0.0, "max": 6.6, "unit": "ug/L", "center": 2.0},
    # Influent BOD and TSS have no direct limit; use typical plant like ranges
    "urn:ex/Influent_Pump-in-biochemical-oxygen-demand": {"min": 80.0, "max": 450.0, "unit": "mg/L", "center": 220.0},
    "urn:ex/Influent_Pump-in-tss-concentration": {"min": 80.0, "max": 500.0, "unit": "mg/L", "center": 250.0},

    # Effluent
    "urn:ex/Effluent_Pump-out-biochemical-oxygen-demand": {"min": 0.0, "max": 30.0, "unit": "mg/L", "center": 18.0},
    "urn:ex/Effluent_Pump-out-tss-concentration": {"min": 0.0, "max": 30.0, "unit": "mg/L", "center": 16.0},
    "urn:ex/Effluent_Pump-out-ph": {"min": 6.0, "max": 9.0, "unit": "pH", "center": 7.2},
    "urn:ex/Effluent_Pump-out-cl2-mgL": {"min": 0.0, "max": 0.38, "unit": "mg/L", "center": 0.08},
    "urn:ex/Effluent_Pump-out-nh4-mgL": {"min": 0.0, "max": 64.0, "unit": "mg/L", "center": 20.0},
    "urn:ex/Effluent_Pump-out-copper": {"min": 0.0, "max": 64.0, "unit": "ug/L", "center": 25.0},
    "urn:ex/Effluent_Pump-out-cyanide": {"min": 0.0, "max": 17.0, "unit": "ug/L", "center": 5.0},
    "urn:ex/Effluent_Pump-out-teq-dioxin": {"min": 0.0, "max": 1.4e-8, "unit": "ug/L", "center": 0.6e-8},
    # Enterococcus has two rules; use 210 as typical cap and allow spikes above 1000 sometimes
    "urn:ex/Effluent_Pump-out-bacteria-enterococcus": {"min": 0.0, "max": 210.0, "unit": "CFU/100mL", "center": 80.0},
}

# Optional stronger max values used when we intentionally generate excursions
BENICIA_EXCURSION_CAPS = {
    "urn:ex/Influent_Pump-in-flow-rate": 6.0,
    "urn:ex/Influent_Pump-in-cyanide": 15.0,
    "urn:ex/Effluent_Pump-out-biochemical-oxygen-demand": 60.0,
    "urn:ex/Effluent_Pump-out-tss-concentration": 80.0,
    "urn:ex/Effluent_Pump-out-ph": 10.0,
    "urn:ex/Effluent_Pump-out-cl2-mgL": 0.8,
    "urn:ex/Effluent_Pump-out-nh4-mgL": 120.0,
    "urn:ex/Effluent_Pump-out-copper": 150.0,
    "urn:ex/Effluent_Pump-out-cyanide": 60.0,
    "urn:ex/Effluent_Pump-out-teq-dioxin": 4.0e-8,
    "urn:ex/Effluent_Pump-out-bacteria-enterococcus": 2000.0,
}


def local_name(uri: rdflib.term.Identifier) -> str:
    return str(uri).split("/")[-1]


def get_properties(graph: rdflib.Graph) -> list[rdflib.term.Identifier]:
    props = []
    for subj, _, obj in graph.triples((None, RDF.type, None)):
        if obj in PROPERTY_TYPES:
            props.append(subj)
    return sorted(props, key=local_name)


def get_unit_and_qk(graph: rdflib.Graph, prop: rdflib.term.Identifier) -> tuple[Optional[str], Optional[str]]:
    unit_uri = None
    quantity_kind = None

    for _, _, unit in graph.triples((prop, QUDT.hasUnit, None)):
        unit_uri = str(unit)
        break

    for _, _, qk in graph.triples((prop, QUDT.hasQuantityKind, None)):
        quantity_kind = str(qk).split("/")[-1]
        break

    return unit_uri, quantity_kind


def get_unit_range(graph: rdflib.Graph, prop: rdflib.term.Identifier) -> tuple[float, float]:
    unit_uri, quantity_kind = get_unit_and_qk(graph, prop)

    if unit_uri and unit_uri in UNIT_RANGES:
        return UNIT_RANGES[unit_uri]

    if quantity_kind and quantity_kind in QUANTITY_KIND_RANGES:
        return QUANTITY_KIND_RANGES[quantity_kind]

    return (0.0, 100.0)


def is_enumeration(graph: rdflib.Graph, prop: rdflib.term.Identifier) -> bool:
    return any(graph.triples((prop, QUDT.hasEnumerationKind, None)))


def build_series(start: datetime, length: int, interval_seconds: int) -> list[str]:
    return [(start + timedelta(seconds=interval_seconds * idx)).isoformat() for idx in range(length)]


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _random_walk_series(
    rng: random.Random,
    length: int,
    center: float,
    lo: float,
    hi: float,
    step_frac: float,
    excursion_rate: float,
    excursion_cap: Optional[float],
    allow_below_lo: bool = False,
) -> list[float]:
    """
    Smooth series that stays near center and mostly within [lo, hi].
    With small probability, inject excursions above hi (and optionally below lo).
    """
    width = max(hi - lo, 1e-12)
    step_sigma = max(width * step_frac, 1e-12)

    x = center
    out: list[float] = []

    for _ in range(length):
        # Mean reversion toward center
        x = x + rng.gauss(0.0, step_sigma) + 0.05 * (center - x)

        # Excursion
        if rng.random() < excursion_rate:
            if allow_below_lo and rng.random() < 0.2:
                # rare low excursion
                x = lo - abs(rng.gauss(0.0, step_sigma * 5.0))
            else:
                cap = excursion_cap if excursion_cap is not None else (hi + width * 0.5)
                # draw above hi but not crazy
                x = hi + abs(rng.gauss(0.0, step_sigma * 6.0))
                x = min(x, cap)

        # Clamp typical values
        if allow_below_lo:
            x = min(x, excursion_cap if excursion_cap is not None else hi)
        else:
            x = _clamp(x, lo, excursion_cap if excursion_cap is not None else hi)

        out.append(float(x))

    return out


def _logish_series(
    rng: random.Random,
    length: int,
    center: float,
    lo: float,
    hi: float,
    step_frac: float,
    excursion_rate: float,
    excursion_cap: Optional[float],
) -> list[float]:
    """
    For microbial counts: multiplicative noise feels more realistic than additive.
    """
    # work in log space
    safe_center = max(center, 1e-6)
    logx = math.log(safe_center)

    log_lo = math.log(max(lo, 1e-6))
    log_hi = math.log(max(hi, 1e-6))
    width = max(log_hi - log_lo, 1e-6)

    step_sigma = max(width * step_frac, 1e-6)

    out: list[float] = []
    for _ in range(length):
        logx = logx + rng.gauss(0.0, step_sigma) + 0.05 * (math.log(safe_center) - logx)

        if rng.random() < excursion_rate:
            cap = excursion_cap if excursion_cap is not None else 2000.0
            # spike upward
            spike = math.exp(log_hi + abs(rng.gauss(0.0, width * 0.8)))
            out.append(float(min(spike, cap)))
            continue

        x = math.exp(logx)
        x = _clamp(x, lo, hi)
        out.append(float(x))
    return out


def get_benicia_profile(prop_uri: str) -> Optional[dict]:
    return BENICIA_LIMITS.get(prop_uri)


def build_values(
    rng: random.Random,
    graph: rdflib.Graph,
    prop: rdflib.term.Identifier,
    length: int,
    excursion_rate: float,
    step_frac: float,
) -> list[float | int]:
    if is_enumeration(graph, prop):
        return [rng.choice([0, 1]) for _ in range(length)]

    prop_uri = str(prop)
    name = local_name(prop).lower()

    profile = get_benicia_profile(prop_uri)
    excursion_cap = BENICIA_EXCURSION_CAPS.get(prop_uri)

    if profile is not None:
        lo = float(profile["min"])
        hi = float(profile["max"])
        center = float(profile.get("center", (lo + hi) / 2.0))

        # Special handling
        if "ph" in name:
            # allow rare below 6 and above 9
            return _random_walk_series(
                rng,
                length,
                center=center,
                lo=lo,
                hi=hi,
                step_frac=step_frac,
                excursion_rate=excursion_rate,
                excursion_cap=excursion_cap,
                allow_below_lo=True,
            )

        if "enterococcus" in name or "bacteria" in name:
            # microbial counts spike
            return _logish_series(
                rng,
                length,
                center=center,
                lo=lo,
                hi=hi,
                step_frac=step_frac,
                excursion_rate=excursion_rate,
                excursion_cap=excursion_cap,
            )

        # Default for permit based numeric series
        return _random_walk_series(
            rng,
            length,
            center=center,
            lo=lo,
            hi=hi,
            step_frac=step_frac,
            excursion_rate=excursion_rate,
            excursion_cap=excursion_cap,
            allow_below_lo=False,
        )

    # Heuristics based on unit or quantity kind if we do not have a known profile
    unit_uri, qk = get_unit_and_qk(graph, prop)
    lo, hi = get_unit_range(graph, prop)

    # If it looks like a flow rate but unit mapping is generic, tighten it
    if qk == "VolumeFlowRate" and "flow" in name and hi >= 10.0:
        lo, hi = (0.0, 6.0)

    # If it is concentration mg/L, avoid huge uniform ranges
    if unit_uri == str(QUDT_UNIT["MilliGM-PER-L"]):
        lo, hi = (0.0, 120.0)

    center = (lo + hi) / 2.0
    return _random_walk_series(
        rng,
        length,
        center=center,
        lo=float(lo),
        hi=float(hi),
        step_frac=step_frac,
        excursion_rate=excursion_rate,
        excursion_cap=None,
        allow_below_lo=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate parquet files for each Benicia property.")
    parser.add_argument("--model", default="deployments/BENICIA/benicia-model.ttl", help="Path to the base Benicia model.")
    parser.add_argument("--output-dir", default="data/BENICIA/parquet", help="Directory to write parquet files.")
    parser.add_argument("--length", type=int, required=True, help="Number of rows to generate per property.")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Seconds between samples.")
    parser.add_argument("--start", default=None, help="Start time in ISO-8601 (defaults to now UTC).")
    parser.add_argument("--seed", type=int, default=42, help="Optional random seed for reproducible output.")

    # New knobs
    parser.add_argument(
        "--excursion-rate",
        type=float,
        default=0.02,
        help="Probability per sample of an excursion beyond typical limits, for stress testing.",
    )
    parser.add_argument(
        "--step-frac",
        type=float,
        default=0.02,
        help="Random walk step size as a fraction of the normal range width.",
    )

    args = parser.parse_args()

    if args.length <= 0:
        raise SystemExit("--length must be a positive integer.")
    if not (0.0 <= args.excursion_rate <= 1.0):
        raise SystemExit("--excursion-rate must be between 0 and 1.")
    if args.step_frac <= 0.0:
        raise SystemExit("--step-frac must be positive.")

    start = datetime.fromisoformat(args.start) if args.start else datetime.now(timezone.utc)

    graph = rdflib.Graph().parse(args.model, format="turtle")
    properties = get_properties(graph)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)

    for prop in properties:
        name = local_name(prop)
        timestamps = build_series(start, args.length, args.interval_seconds)
        values = build_values(
            rng=rng,
            graph=graph,
            prop=prop,
            length=args.length,
            excursion_rate=args.excursion_rate,
            step_frac=args.step_frac,
        )
        df = pl.DataFrame({"Timestamp": timestamps, "Value": values})
        output_path = output_dir / f"{name}.parquet"
        df.write_parquet(output_path)

    print(f"Generated {len(properties)} parquet files in {output_dir}")


if __name__ == "__main__":
    main()
