"""Shared value generation for the Benicia deployment.

Both the live driver (``simulator_driver.py``) and the historical data script
(``generate_historical.py``) introspect a Benicia model TTL for its
``s223:Quantifiable*Property`` nodes and synthesise a realistic time series per
property. This module holds that logic in one place:

  * :func:`get_properties` — enumerate the model's measurement points.
  * :class:`SeriesState`   — per-property stateful random walk. Call
    :meth:`SeriesState.next_value` once per timestep; the live driver calls it
    each tick, the historical script calls it in a loop.

Ranges come from permit-informed profiles (:data:`BENICIA_LIMITS`) when the
property URI is known, otherwise from the property's QUDT unit / quantity kind.
Nothing here is Benicia-model-specific beyond those profile tables, so it works
against ``benicia-model.ttl`` and ``benicia-model-100.ttl`` alike.
"""

from __future__ import annotations

import math
import random
from typing import Optional

import rdflib
from rdflib.namespace import RDF

from acquirium.internals.internals_namespaces import QUDT, QUDT_UNIT, S223

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

# Permit-informed profiles for specific Benicia property URIs, used to produce
# realistic values. Properties not listed here fall back to unit/quantity-kind
# ranges above.
BENICIA_LIMITS = {
    # Influent
    "urn:ex/Influent_Pump-in-flow-rate": {"min": 0.0, "max": 4.5, "center": 2.5},
    "urn:ex/Influent_Pump-in-cyanide": {"min": 0.0, "max": 6.6, "center": 2.0},
    "urn:ex/Influent_Pump-in-biochemical-oxygen-demand": {"min": 80.0, "max": 450.0, "center": 220.0},
    "urn:ex/Influent_Pump-in-tss-concentration": {"min": 80.0, "max": 500.0, "center": 250.0},
    # Effluent
    "urn:ex/Effluent_Pump-out-biochemical-oxygen-demand": {"min": 0.0, "max": 30.0, "center": 18.0},
    "urn:ex/Effluent_Pump-out-tss-concentration": {"min": 0.0, "max": 30.0, "center": 16.0},
    "urn:ex/Effluent_Pump-out-ph": {"min": 6.0, "max": 9.0, "center": 7.2},
    "urn:ex/Effluent_Pump-out-cl2-mgL": {"min": 0.0, "max": 0.38, "center": 0.08},
    "urn:ex/Effluent_Pump-out-nh4-mgL": {"min": 0.0, "max": 64.0, "center": 20.0},
    "urn:ex/Effluent_Pump-out-copper": {"min": 0.0, "max": 64.0, "center": 25.0},
    "urn:ex/Effluent_Pump-out-cyanide": {"min": 0.0, "max": 17.0, "center": 5.0},
    "urn:ex/Effluent_Pump-out-teq-dioxin": {"min": 0.0, "max": 1.4e-8, "center": 0.6e-8},
    "urn:ex/Effluent_Pump-out-bacteria-enterococcus": {"min": 0.0, "max": 210.0, "center": 80.0},
}

# Upper caps used only when injecting excursions beyond the typical range.
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
    """Return the model's measurement-point property URIs, sorted by local name."""
    props = [
        subj
        for subj, _, obj in graph.triples((None, RDF.type, None))
        if obj in PROPERTY_TYPES
    ]
    return sorted(props, key=local_name)


def get_unit_and_qk(
    graph: rdflib.Graph, prop: rdflib.term.Identifier
) -> tuple[Optional[str], Optional[str]]:
    unit_uri = next((str(u) for _, _, u in graph.triples((prop, QUDT.hasUnit, None))), None)
    qk = next(
        (str(q).split("/")[-1] for _, _, q in graph.triples((prop, QUDT.hasQuantityKind, None))),
        None,
    )
    return unit_uri, qk


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
    return lo if x < lo else hi if x > hi else x


class SeriesState:
    """Per-property stateful generator producing a smooth, realistic time series.

    Call :meth:`next_value` once per timestep. Numeric series follow a
    mean-reverting random walk with occasional excursions; microbial counts
    ("enterococcus"/"bacteria") evolve multiplicatively in log space; pH is
    allowed to dip below its lower limit. Enumeration properties are handled by
    the caller (they emit 0/1 directly, not a SeriesState).
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
            self.log_center = math.log(max(center, 1e-6))
            self.logx = self.log_center + rng.gauss(0.0, self.step_sigma)
            self.x = float(_clamp(math.exp(self.logx), self.lo, self.hi))
        else:
            self.x = float(_clamp(center + rng.gauss(0.0, self.step_sigma), self.lo, self.hi))

    def next_value(self, rng: random.Random, excursion_rate: float) -> float:
        if self.is_logish:
            self.logx, self.x = _logish_next(
                rng, self.logx, self.log_center, self.lo, self.hi,
                self.step_sigma, excursion_rate, self.excursion_cap,
            )
            return float(self.x)

        self.x = _random_walk_next(
            rng, self.x, self.center, self.lo, self.hi,
            self.step_sigma, excursion_rate, self.excursion_cap, self.allow_below_lo,
        )
        return float(self.x)


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
    # Mean reversion toward center plus Gaussian noise.
    x = x + rng.gauss(0.0, step_sigma) + 0.05 * (center - x)

    if rng.random() < excursion_rate:
        if allow_below_lo and rng.random() < 0.2:
            x = lo - abs(rng.gauss(0.0, step_sigma * 5.0))
        else:
            cap = excursion_cap if excursion_cap is not None else (hi + (hi - lo) * 0.5)
            x = min(hi + abs(rng.gauss(0.0, step_sigma * 6.0)), cap)

    cap2 = excursion_cap if excursion_cap is not None else hi
    return float(min(x, cap2)) if allow_below_lo else float(_clamp(x, lo, cap2))


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
    logx = logx + rng.gauss(0.0, step_sigma) + 0.05 * (log_center - logx)

    if rng.random() < excursion_rate:
        cap = excursion_cap if excursion_cap is not None else 2000.0
        # Build the spike in log space and clamp the exponent for numeric safety.
        exp_arg = min(math.log(max(hi, 1e-6)) + abs(rng.gauss(0.0, step_sigma * 8.0)), 700)
        return logx, float(min(math.exp(exp_arg), cap))

    return logx, float(_clamp(math.exp(logx), lo, hi))


def build_state_for_property(
    rng: random.Random,
    graph: rdflib.Graph,
    prop: rdflib.term.Identifier,
    step_frac: float,
) -> SeriesState | None:
    """Build the :class:`SeriesState` for ``prop``, or ``None`` if it is an enum."""
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
        # Tighten a couple of common cases the generic ranges get too wide.
        if qk == "VolumeFlowRate" and "flow" in name and hi >= 10.0:
            lo, hi = (0.0, 6.0)
        if unit_uri == str(QUDT_UNIT["MilliGM-PER-L"]):
            lo, hi = (0.0, 120.0)
        lo, hi = float(lo), float(hi)
        center = (lo + hi) / 2.0

    step_sigma = max(max(hi - lo, 1e-12) * step_frac, 1e-12)
    return SeriesState(
        prop_uri=prop_uri,
        name=name,
        lo=lo,
        hi=hi,
        center=center,
        step_sigma=step_sigma,
        is_logish=("enterococcus" in name) or ("bacteria" in name),
        excursion_cap=excursion_cap,
        allow_below_lo=("ph" in name),
        rng=rng,
    )
