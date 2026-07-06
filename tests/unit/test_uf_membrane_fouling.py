from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl
import pytest

from acquirium.Apps.uf_membrane_fouling import (
    ModuleSignals,
    PlantSignals,
    _one,
    _viscosity_pas,
    assign_cycles,
    derived_uri,
    discover,
    flag_fouling,
    fouling_rates,
    normalize,
)


PREFIXES = {
    "s223": "http://data.ashrae.org/standard223#",
    "qudt": "http://qudt.org/schema/qudt/",
    "qk": "http://qudt.org/vocab/quantitykind/",
    "nawi": "urn:nawi-water-ontology#",
    "unit": "http://qudt.org/vocab/unit/",
}

FEED_PRESSURE = "urn:port-hueneme#UF-feed-pressure"
FEED_TEMP = "urn:port-hueneme#UF-feed-temperature"
BACKWASH = "urn:port-hueneme#UF-backwash-flow"
MODULE = "urn:port-hueneme#UF1"
PERM_FLOW = "urn:port-hueneme#UF1-filtrate-flow"
PERM_PRESSURE = "urn:port-hueneme#UF1-filtrate-pressure"

UNITS = {
    FEED_PRESSURE: "http://qudt.org/vocab/unit/PSI",
    PERM_PRESSURE: "http://qudt.org/vocab/unit/PSI",
    PERM_FLOW: "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
}


class FakeGraphClient:
    """A minimal fake answering the exact SPARQL shapes `discover()` compiles.

    Dispatch is by distinguishing substring (same technique as
    ``tests/unit/test_dpr_toc_alarm.py``'s ``TocClient``) rather than a real
    triple store, since the App's own correctness was validated end-to-end
    against the live Port Hueneme server (see project memory).
    """

    def expand_uri(self, text: str) -> str:
        s = str(text)
        if s.startswith(("http://", "https://", "urn:")):
            return s
        prefix, local = s.split(":", 1)
        return PREFIXES[prefix] + local

    def compact_uri(self, item) -> str:
        s = str(item)
        for pfx, ns in PREFIXES.items():
            if s.startswith(ns):
                return f"{pfx}:{s[len(ns):]}"
        return s

    def namespace_manager(self):
        class _NM:
            def namespaces(self):
                return list(PREFIXES.items())

        return _NM()

    def sparql_query(self, sparql: str, use_union: bool = True) -> dict:
        if "hasUnit" in sparql and "VALUES ?n0" in sparql:
            point = sparql.split("VALUES ?n0 { <", 1)[1].split(">", 1)[0]
            return {"columns": ["focus"], "rows": [[UNITS[point]]]}
        if "hasProcess" in sparql:
            return {"columns": ["focus"], "rows": [[BACKWASH]]}
        if "?module" in sparql and "?area" in sparql:
            return {"columns": ["module", "area"], "rows": [[MODULE, 50.0]]}
        if "hasConnectionPoint" in sparql:
            if "VolumeFlowRate" in sparql:
                return {"columns": ["focus"], "rows": [[PERM_FLOW]]}
            return {"columns": ["focus"], "rows": [[PERM_PRESSURE]]}
        if "quantitykind/Temperature" in sparql:
            return {"columns": ["focus"], "rows": [[FEED_TEMP]]}
        if "quantitykind/Pressure" in sparql:
            return {"columns": ["focus"], "rows": [[FEED_PRESSURE]]}
        raise AssertionError(f"unexpected query in FakeGraphClient:\n{sparql}")


class FakeUnitClient:
    """Fake conversion-factor client for `normalize()`'s unit math."""

    _factors = {
        ("http://qudt.org/vocab/unit/PSI", "PA"): (6894.757293168362, 0.0, 1.0, 0.0),
        (
            "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            "M3-PER-SEC",
        ): (6.30901964e-05, 0.0, 1.0, 0.0),
    }

    def get_conversion_factors(self, from_unit: str, to_unit: str) -> dict:
        from_mult, from_off, to_mult, to_off = self._factors[(from_unit, to_unit)]
        return {
            "from_multiplier": from_mult,
            "from_offset": from_off,
            "to_multiplier": to_mult,
            "to_offset": to_off,
            "compatible": True,
        }


def test_discover_finds_module_and_signals_via_facet_api():
    plant, modules = discover(FakeGraphClient())

    assert plant == PlantSignals(FEED_PRESSURE, UNITS[FEED_PRESSURE], FEED_TEMP, BACKWASH)
    assert modules == [
        ModuleSignals(
            MODULE, 50.0, PERM_FLOW, UNITS[PERM_FLOW], PERM_PRESSURE, UNITS[PERM_PRESSURE]
        )
    ]


def test_viscosity_matches_known_water_viscosity_at_20c():
    df = pl.DataFrame({"temp_c": [20.0]}).with_columns(_viscosity_pas(pl.col("temp_c")).alias("mu"))
    assert df["mu"][0] == pytest.approx(1.0016e-3, rel=1e-2)


def test_normalize_computes_tmp_flux_and_resistance():
    plant = PlantSignals(FEED_PRESSURE, UNITS[FEED_PRESSURE], FEED_TEMP, BACKWASH)
    mod = ModuleSignals(MODULE, 50.0, PERM_FLOW, UNITS[PERM_FLOW], PERM_PRESSURE, UNITS[PERM_PRESSURE])
    t0 = datetime(2013, 6, 5, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "time": [t0, t0 + timedelta(seconds=1)],
            "feed_p": [35.32, 35.03],
            "temp_c": [17.84, 17.84],
            "bw_gpm": [0.01, 0.01],
            "perm_flow": [9.55, 9.56],
            "perm_p": [32.06, 32.31],
        }
    )

    op = normalize(FakeUnitClient(), plant, mod, df)

    assert op.height == 2
    assert op["tmp_bar"][0] == pytest.approx(0.2246, rel=1e-2)
    assert op["flux_lmh"][0] == pytest.approx(43.36, rel=1e-2)
    assert op["resistance_per_m"][0] == pytest.approx(1.7646e12, rel=1e-2)
    assert op["filtering"].all()


def test_normalize_masks_out_non_filtering_samples():
    plant = PlantSignals(FEED_PRESSURE, UNITS[FEED_PRESSURE], FEED_TEMP, BACKWASH)
    mod = ModuleSignals(MODULE, 50.0, PERM_FLOW, UNITS[PERM_FLOW], PERM_PRESSURE, UNITS[PERM_PRESSURE])
    t0 = datetime(2013, 6, 5, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "time": [t0],
            "feed_p": [0.0],
            "temp_c": [17.84],
            "bw_gpm": [0.0],
            "perm_flow": [0.0],  # pump off -> no flux -> not filtering
            "perm_p": [0.0],
        }
    )

    op = normalize(FakeUnitClient(), plant, mod, df)

    assert op.is_empty()


def _cycle_frame(cycle: int, start: datetime, n: int, resistance: np.ndarray) -> pl.DataFrame:
    times = [start + timedelta(seconds=i) for i in range(n)]
    return pl.DataFrame({"time": times, "cycle": [cycle] * n, "resistance_per_m": resistance})


def test_assign_cycles_numbers_by_backwash_onset():
    t0 = datetime(2013, 6, 5, tzinfo=timezone.utc)
    times = [t0 + timedelta(seconds=i) for i in range(6)]
    full_df = pl.DataFrame({"time": times, "bw_gpm": [0.0, 0.0, 5.0, 5.0, 0.0, 0.0]})
    op = pl.DataFrame({"time": times, "value": list(range(6))})

    tagged = assign_cycles(full_df, op, bw_on_gpm=1.0)

    assert tagged.sort("time")["cycle"].to_list() == [0, 0, 1, 1, 1, 1]


def test_fouling_rates_drops_short_cycles_and_fits_positive_slope():
    t0 = datetime(2013, 6, 5, tzinfo=timezone.utc)
    long_cycle = _cycle_frame(
        cycle=1, start=t0, n=1800, resistance=1e12 + np.arange(1800) * 1e8  # 30 min, rising
    )
    short_cycle = _cycle_frame(
        cycle=2, start=t0 + timedelta(hours=1), n=120, resistance=np.full(120, 1e12)  # 2 min
    )
    op = pl.concat([long_cycle, short_cycle])

    cycles = fouling_rates(op, min_cycle_min=10.0, trim_s=60.0)

    assert cycles["cycle"].to_list() == [1]
    assert cycles["fouling_rate"][0] == pytest.approx(1e8 * 3600, rel=1e-2)


def test_flag_fouling_flags_anomalous_rate_against_prior_history():
    cycles = pl.DataFrame(
        {
            "cycle": list(range(6)),
            "end": [datetime(2013, 6, 5, tzinfo=timezone.utc) + timedelta(hours=i) for i in range(6)],
            "fouling_rate": [1e11, 1.1e11, 0.9e11, 1.05e11, 0.95e11, 5e12],
            "resistance": [1e12] * 6,
        }
    )

    flagged = flag_fouling(cycles, z_alarm=3.0, cip_resistance_per_m=9.63e12)

    assert flagged["fouling_detected"].to_list() == [0.0, 0.0, 0.0, 0.0, 0.0, 1.0]


def test_flag_fouling_flags_resistance_above_cip_threshold_even_without_anomaly():
    cycles = pl.DataFrame(
        {
            "cycle": list(range(4)),
            "end": [datetime(2013, 6, 5, tzinfo=timezone.utc) + timedelta(hours=i) for i in range(4)],
            "fouling_rate": [1e11, 1e11, 1e11, 1e11],
            "resistance": [1e12, 1e12, 1e12, 1e13],
        }
    )

    flagged = flag_fouling(cycles, z_alarm=3.0, cip_resistance_per_m=9.63e12)

    assert flagged["fouling_detected"].to_list() == [0.0, 0.0, 0.0, 1.0]


def test_derived_uri_is_stable_and_sanitized():
    uri = derived_uri("urn:port-hueneme#UF1", "resistance_per_m")

    assert uri == "urn:acquirium:point#urn_port_hueneme_UF1_resistance_per_m"
    assert derived_uri("urn:port-hueneme#UF1", "resistance_per_m") == uri


def test_one_raises_named_lookuperror_when_signal_missing():
    with pytest.raises(LookupError, match="could not find a UF feed pressure point"):
        _one([], "feed pressure point")


def test_one_raises_when_multiple_matches_found():
    with pytest.raises(LookupError, match="found 2"):
        _one(["urn:a", "urn:b"], "feed pressure point")
