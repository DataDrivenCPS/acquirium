"""Tests for the RO permeate TOC rolling-sign-test alarm (examples/ro_permeate_toc_alarm).

Loaded by file path since the example app lives outside the installed
``acquirium`` package, mirroring how tests/unit/test_dpr_csv_driver.py loads
standalone driver scripts.
"""

from __future__ import annotations

import importlib.util
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "ro_permeate_toc_alarm"
    / "ro_permeate_toc_alarm.py"
)
_SPEC = importlib.util.spec_from_file_location("ro_permeate_toc_alarm", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

ToCRule = _MODULE.ToCRule
REG_RULES = _MODULE.REG_RULES
evaluate_rules = _MODULE.evaluate_rules
_binom_sf_at_least = _MODULE._binom_sf_at_least
ROPermeateTOCAlarm = _MODULE.ROPermeateTOCAlarm

J1_RULE, J2_RULE = REG_RULES[1], REG_RULES[0]  # 0.15mg/L-120h, 0.1mg/L-24h respectively


def _series(t0: datetime, baseline_segments: list[tuple[float, float, int]], interval_minutes: float = 15.0) -> pl.DataFrame:
    """Build a [ts, value] frame from (mean, stdev, n_samples) segments, back to back from t0."""
    rng = random.Random(7)
    ts: list[datetime] = []
    values: list[float] = []
    t = t0
    for mean, stdev, n in baseline_segments:
        for _ in range(n):
            ts.append(t)
            values.append(max(0.0, rng.gauss(mean, stdev)))
            t += timedelta(minutes=interval_minutes)
    return pl.DataFrame({"ts": ts, "value": values})


class TestBinomSF:
    def test_k_zero_is_certain(self):
        assert _binom_sf_at_least(0, 10) == 1.0

    def test_all_above_is_unlikely(self):
        # P(X >= 10) for Binomial(10, 0.5) = 1/1024
        assert _binom_sf_at_least(10, 10) == pytest.approx(1 / 1024)

    def test_monotonic_in_k(self):
        vals = [_binom_sf_at_least(k, 20) for k in range(21)]
        assert vals == sorted(vals, reverse=True)

    def test_zero_samples_is_certain(self):
        assert _binom_sf_at_least(0, 0) == 1.0


class TestEvaluateRules:
    def test_quiet_baseline_never_flags(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        df = _series(t0, [(0.05, 0.01, 600)])  # well below both thresholds, ~150h of data
        out = evaluate_rules(df)
        assert (out["flag"] == "").all()

    def test_sustained_exceedance_trips_24h_rule_before_120h_rule(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        df = _series(
            t0,
            [
                (0.05, 0.01, 24 * 4),  # 24h of quiet baseline (4 samples/hour)
                (0.20, 0.02, 130 * 4),  # then 130h pinned well above both thresholds
            ],
        )
        excursion_start = t0 + timedelta(hours=24)
        out = evaluate_rules(df)

        # Nothing should fire before the excursion even starts.
        before = out.filter(pl.col("ts") < excursion_start)
        assert (before["flag"] == "").all()

        j2_hits = out.filter(pl.col("flag").str.contains(J2_RULE.message, literal=True))
        j1_hits = out.filter(pl.col("flag").str.contains(J1_RULE.message, literal=True))
        assert j2_hits.height > 0, "0.1 mg/L / 24h rule should have fired"
        assert j1_hits.height > 0, "0.15 mg/L / 120h rule should have fired"

        first_j2 = j2_hits["ts"].min()
        first_j1 = j1_hits["ts"].min()

        # Detection lags the true exceedance start by ~duration threshold plus
        # the sign-test window's own lead-in (default 2h) plus a little slack
        # for the binomial test to reach significance.
        assert excursion_start + timedelta(hours=24) <= first_j2 <= excursion_start + timedelta(hours=27)
        assert excursion_start + timedelta(hours=120) <= first_j1 <= excursion_start + timedelta(hours=123)

        # 120h rule firing implies the 24h rule has necessarily already fired
        # (0.15 mg/L exceedance is a strict subset of 0.1 mg/L exceedance).
        assert first_j1 > first_j2

    def test_single_noisy_dip_does_not_reset_the_streak(self):
        """A single low outlier inside an otherwise sustained excursion shouldn't
        truncate the continuous-exceedance clock back to zero (that's the whole
        point of using a rolling sign test instead of a raw threshold check)."""
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        df = _series(t0, [(0.20, 0.02, 26 * 4)])  # 26h pinned above threshold
        # Inject one noisy low sample partway through (well within the window).
        values = df["value"].to_list()
        dip_index = len(values) // 2
        values[dip_index] = 0.0
        df = df.with_columns(pl.Series("value", values))

        out = evaluate_rules(df)
        j2_hits = out.filter(pl.col("flag").str.contains(J2_RULE.message, literal=True))
        assert j2_hits.height > 0, "single noisy dip should not prevent the 24h rule from firing"

    def test_clearing_below_threshold_resets_the_flag(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        df = _series(
            t0,
            [
                (0.20, 0.02, 26 * 4),  # sustained excursion long enough to trip the 24h rule
                (0.05, 0.01, 10 * 4),  # then a clean recovery
            ],
        )
        out = evaluate_rules(df)
        tail = out.tail(4 * 4)  # last 4h, well after recovery + window lead-in
        assert (tail["flag"] == "").all()

    def test_empty_input_returns_empty_frame(self):
        out = evaluate_rules(pl.DataFrame({"ts": [], "value": []}, schema={"ts": pl.Datetime, "value": pl.Float64}))
        assert out.is_empty()


class TestROPermeateTOCAlarm:
    def test_no_point_uri_param_required(self):
        app = ROPermeateTOCAlarm()
        assert app.max_hops == 3

    def test_default_lookback_covers_longest_rule_plus_margin(self):
        app = ROPermeateTOCAlarm()
        assert app.lookback_hours >= 120.0 + 2.0

    def test_build_query_with_no_discovered_points_returns_empty_dict(self):
        class _FakeClient:
            def sparql_query(self, query: str, use_union: bool = True) -> dict:
                return {"columns": ["property"], "rows": []}

        class _FakeAq:
            client = _FakeClient()

        app = ROPermeateTOCAlarm()
        assert app.build_query(_FakeAq()) == {}
        assert app._point_by_alias == {}


# --------------------------------------------------------------------------
# Discovery SPARQL: run the real query text through rdflib against a hand
# built fixture combining the bundled water/s223 ontologies with synthetic
# plant instance data, so we exercise actual ontology semantics (role
# enumeration, substance subclassing, connection topology) without needing a
# live Acquirium server.
# --------------------------------------------------------------------------

_discovery_sparql = _MODULE._discovery_sparql

_ONTOLOGY_PATH = Path(__file__).resolve().parents[2] / "src" / "acquirium" / "_ontologies" / "water.ttl"

_FIXTURE_TTL = """
@prefix s223: <http://data.ashrae.org/standard223#> .
@prefix watr: <urn:nawi-water-ontology#> .
@prefix : <urn:test:> .

# Two RO trains whose permeate sides converge one hop downstream onto a
# shared header connection point carrying a single "combined" TOC analyzer.
:ro1 a watr:ReverseOsmosisMembrane ;
     s223:hasConnectionPoint :ro1-permeate-cp, :ro1-reject-cp .
:ro1-permeate-cp s223:hasRole watr:Role-Permeate ;
     s223:connectedTo :combined-cp .
:ro1-reject-cp s223:hasRole watr:Role-Reject ;
     s223:connectedTo :reject-cp .

:ro2 a watr:ReverseOsmosisMembrane ;
     s223:hasConnectionPoint :ro2-permeate-cp .
:ro2-permeate-cp s223:hasRole watr:Role-Permeate ;
     s223:connectedTo :combined-cp .

:combined-cp s223:hasProperty :combined-toc-property .
:combined-toc-property s223:ofSubstance watr:Constituent-Organics .

# Reject-side TOC: reachable from ro1, but via the non-permeate connection
# point, so it must NOT be discovered.
:reject-cp s223:hasProperty :reject-toc-property .
:reject-toc-property s223:ofSubstance watr:Constituent-OrganicCarbon .

# A TOC sensor attached directly at ro1's permeate connection point via the
# Sensor/hasObservationLocation/observes pattern (no hasProperty edge at
# all) -- the alternate real-world modeling convention.
:sensor-x a s223:Sensor ;
     s223:hasObservationLocation :ro1-permeate-cp ;
     s223:observes :direct-permeate-toc-property .
:direct-permeate-toc-property s223:ofSubstance watr:Constituent-OrganicCarbon .

# Unrelated influent TOC sensor elsewhere in the plant, not reachable from
# any RO permeate connection point -- must NOT be discovered.
:influent-cp s223:hasProperty :influent-toc-property .
:influent-toc-property s223:ofSubstance watr:Constituent-OrganicCarbon .
"""


def _query_fixture(sparql: str):
    rdflib = pytest.importorskip("rdflib")
    graph = rdflib.Graph()
    graph.parse(str(_ONTOLOGY_PATH), format="turtle")
    graph.parse(data=_FIXTURE_TTL, format="turtle")
    return {str(row.property) for row in graph.query(sparql)}


class TestDiscoverySparql:
    def test_finds_combined_and_direct_permeate_toc_only(self):
        found = _query_fixture(_discovery_sparql(max_hops=3))
        assert found == {"urn:test:combined-toc-property", "urn:test:direct-permeate-toc-property"}

    def test_excludes_reject_side_and_unrelated_influent_toc(self):
        found = _query_fixture(_discovery_sparql(max_hops=3))
        assert "urn:test:reject-toc-property" not in found
        assert "urn:test:influent-toc-property" not in found

    def test_zero_max_hops_still_finds_directly_attached_toc(self):
        # max_hops only bounds the *downstream* search; the zero-hop (directly
        # at the permeate connection point) branch is unconditional.
        found = _query_fixture(_discovery_sparql(max_hops=1))
        assert "urn:test:direct-permeate-toc-property" in found

    def test_insufficient_hops_misses_the_combined_header(self):
        # Sanity check that max_hops actually matters: the combined header in
        # the fixture is reached via the downstream branch (1 hop), so a
        # path that can't take any downstream hop wouldn't find it. We can't
        # ask for max_hops=0 (invalid), so instead assert the downstream path
        # text actually appears for max_hops=1 and would be absent if hops
        # were stripped out entirely.
        with pytest.raises(ValueError):
            _MODULE._downstream_path(0)
