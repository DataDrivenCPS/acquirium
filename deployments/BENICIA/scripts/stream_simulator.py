import argparse
import json
import time
from datetime import datetime, timezone
import math
import random
from typing import Optional

import paho.mqtt.client as mqtt
import rdflib
from rdflib.namespace import RDF
from rdflib import Namespace
import logging
QUDT = Namespace("http://qudt.org/schema/qudt/")
QUDT_UNIT = Namespace("http://qudt.org/vocab/unit/")
S223 = Namespace("http://data.ashrae.org/standard223#")


PROPERTY_TYPES = {
    S223.QuantifiableObservableProperty,
    S223.QuantifiableActuatableProperty,
}

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

# Permit informed profiles for specific Benicia property URIs.
# These are used to produce realistic values during simulation.
BENICIA_LIMITS = {
    # Influent
    "urn:ex/Influent_Pump-in-flow-rate": {"min": 0.0, "max": 4.5, "unit": "MGD", "center": 2.5},
    "urn:ex/Influent_Pump-in-cyanide": {"min": 0.0, "max": 6.6, "unit": "ug/L", "center": 2.0},
    # No direct limit in permit; use typical ranges so percent removal logic can be added later
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
    "urn:ex/Effluent_Pump-out-bacteria-enterococcus": {"min": 0.0, "max": 210.0, "unit": "CFU/100mL", "center": 80.0},
}

# Upper caps used only when injecting excursions
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


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _random_walk_next(
    rng: random.Random,
    x: float,
    center: float,
    lo: float,
    hi: float,
    step_sigma: float,
    excursion_rate: float,
    excursion_cap: Optional[float],
    allow_below_lo: bool = False,
) -> float:
    # mean reversion + noise
    x = x + rng.gauss(0.0, step_sigma) + 0.05 * (center - x)

    # occasional excursion
    if rng.random() < excursion_rate:
        if allow_below_lo and rng.random() < 0.2:
            x = lo - abs(rng.gauss(0.0, step_sigma * 5.0))
        else:
            cap = excursion_cap if excursion_cap is not None else (hi + (hi - lo) * 0.5)
            x = hi + abs(rng.gauss(0.0, step_sigma * 6.0))
            x = min(x, cap)

    if allow_below_lo:
        cap2 = excursion_cap if excursion_cap is not None else hi
        return float(min(x, cap2))
    cap2 = excursion_cap if excursion_cap is not None else hi
    return float(_clamp(x, lo, cap2))


def _logish_next(
    rng: random.Random,
    logx: float,
    log_center: float,
    lo: float,
    hi: float,
    step_sigma: float,
    excursion_rate: float,
    excursion_cap: Optional[float],
) -> tuple[float, float]:
    # evolve log state
    logx = logx + rng.gauss(0.0, step_sigma) + 0.05 * (log_center - logx)

    if rng.random() < excursion_rate:
        cap = excursion_cap if excursion_cap is not None else 2000.0
        spike = math.exp(math.log(max(hi, 1e-6)) + abs(rng.gauss(0.0, step_sigma * 8.0)))
        return logx, float(min(spike, cap))

    x = math.exp(logx)
    return logx, float(_clamp(x, lo, hi))


class SeriesState:
    """
    Holds per property state so each property produces a smooth time series.
    """

    def __init__(
        self,
        prop_uri: str,
        name: str,
        lo: float,
        hi: float,
        center: float,
        step_sigma: float,
        is_logish: bool,
        excursion_cap: Optional[float],
        allow_below_lo: bool,
        rng: random.Random,
    ):
        self.prop_uri = prop_uri
        self.name = name
        self.lo = lo
        self.hi = hi
        self.center = center
        self.step_sigma = step_sigma
        self.is_logish = is_logish
        self.excursion_cap = excursion_cap
        self.allow_below_lo = allow_below_lo

        if is_logish:
            safe_center = max(center, 1e-6)
            self.log_center = math.log(safe_center)
            self.logx = self.log_center
            self.x = safe_center
        else:
            self.x = center

        # randomize initial value slightly
        if is_logish:
            self.logx = self.logx + rng.gauss(0.0, self.step_sigma)
            self.x = float(_clamp(math.exp(self.logx), self.lo, self.hi))
        else:
            self.x = float(_clamp(self.x + rng.gauss(0.0, self.step_sigma), self.lo, self.hi))

    def next_value(self, rng: random.Random, excursion_rate: float) -> float:
        if self.is_logish:
            self.logx, self.x = _logish_next(
                rng,
                logx=self.logx,
                log_center=self.log_center,
                lo=self.lo,
                hi=self.hi,
                step_sigma=self.step_sigma,
                excursion_rate=excursion_rate,
                excursion_cap=self.excursion_cap,
            )
            return float(self.x)

        self.x = _random_walk_next(
            rng,
            x=self.x,
            center=self.center,
            lo=self.lo,
            hi=self.hi,
            step_sigma=self.step_sigma,
            excursion_rate=excursion_rate,
            excursion_cap=self.excursion_cap,
            allow_below_lo=self.allow_below_lo,
        )
        return float(self.x)


def _build_state_for_property(
    rng: random.Random,
    graph: rdflib.Graph,
    prop: rdflib.term.Identifier,
    step_frac: float,
) -> SeriesState | None:
    if is_enumeration(graph, prop):
        return None

    prop_uri = str(prop)
    name = local_name(prop).lower()

    profile = BENICIA_LIMITS.get(prop_uri)
    excursion_cap = BENICIA_EXCURSION_CAPS.get(prop_uri)

    if profile is not None:
        lo = float(profile["min"])
        hi = float(profile["max"])
        center = float(profile.get("center", (lo + hi) / 2.0))
    else:
        unit_uri, qk = get_unit_and_qk(graph, prop)
        lo, hi = get_unit_range(graph, prop)

        # tighten some common cases
        if qk == "VolumeFlowRate" and "flow" in name and hi >= 10.0:
            lo, hi = (0.0, 6.0)
        if unit_uri == str(QUDT_UNIT["MilliGM-PER-L"]):
            lo, hi = (0.0, 120.0)

        lo = float(lo)
        hi = float(hi)
        center = (lo + hi) / 2.0

    width = max(hi - lo, 1e-12)
    step_sigma = max(width * step_frac, 1e-12)

    allow_below_lo = "ph" in name
    is_logish = ("enterococcus" in name) or ("bacteria" in name)

    return SeriesState(
        prop_uri=prop_uri,
        name=name,
        lo=lo,
        hi=hi,
        center=center,
        step_sigma=step_sigma,
        is_logish=is_logish,
        excursion_cap=excursion_cap,
        allow_below_lo=allow_below_lo,
        rng=rng,
    )


def build_value(
    rng: random.Random,
    graph: rdflib.Graph,
    prop: rdflib.term.Identifier,
    states: dict[str, SeriesState],
    excursion_rate: float,
    step_frac: float,
) -> float | int:
    # Enumeration stays as before
    if is_enumeration(graph, prop):
        return rng.choice([0, 1])

    prop_uri = str(prop)

    # lazily init series state
    st = states.get(prop_uri)
    if st is None:
        st2 = _build_state_for_property(rng, graph, prop, step_frac=step_frac)
        if st2 is None:
            # should not happen because enumeration handled above
            low, high = get_unit_range(graph, prop)
            return float(rng.uniform(low, high))
        states[prop_uri] = st2
        st = st2

    return st.next_value(rng, excursion_rate=excursion_rate)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish simulated Benicia property data over MQTT.")
    parser.add_argument("--model", default="deployments/BENICIA/benicia-model.ttl", help="Path to the base Benicia model.")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port.")
    parser.add_argument("--topic-prefix", default="benicia", help="Prefix for MQTT topics.")
    parser.add_argument("--frequency-hz", type=float, default=1.0, help="Messages per second (per property).")
    parser.add_argument("--count", type=int, default=None, help="Optional number of samples to publish per property.")
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

    if args.frequency_hz <= 0:
        raise SystemExit("--frequency-hz must be greater than 0.")
    if not (0.0 <= args.excursion_rate <= 1.0):
        raise SystemExit("--excursion-rate must be between 0 and 1.")
    if args.step_frac <= 0.0:
        raise SystemExit("--step-frac must be positive.")

    graph = rdflib.Graph().parse(args.model, format="turtle")
    properties = get_properties(graph)

    rng = random.Random(args.seed)
    client = mqtt.Client()
    client.connect(args.broker, args.port)
    client.loop_start()

    interval = 1.0 / args.frequency_hz
    published = 0

    # Per property state for smooth series
    states: dict[str, SeriesState] = {}
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
        force=True,  # ensure this takes effect even if something configured logging earlier
    )
    logging.info(f"Starting simulation of {len(properties)} properties...")
    try:
        while True:
            timestamp = datetime.now(timezone.utc).isoformat()
            for prop in properties:
                name = local_name(prop)
                topic = f"{args.topic_prefix}/{name}"
                value = build_value(
                    rng=rng,
                    graph=graph,
                    prop=prop,
                    states=states,
                    excursion_rate=args.excursion_rate,
                    step_frac=args.step_frac,
                )
                payload = {"Timestamp": timestamp, "Value": value}
                client.publish(topic, json.dumps(payload))

            published += 1
            if args.count is not None and published >= args.count:
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
