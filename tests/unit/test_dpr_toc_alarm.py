from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import polars as pl

from acquirium.Apps.dpr_toc_alarm import (
    DprTocAlarmApp,
    RO_RULES,
    TocPoint,
    discover_toc_points,
    evaluate_rule,
    post_aop_rules,
    rules_for_point,
)
from acquirium.internals.models import AppContext


PREFIXES = {
    "s223": "http://data.ashrae.org/standard223#",
    "qudt": "http://qudt.org/schema/qudt/",
    "qk": "http://qudt.org/vocab/quantitykind/",
    "watr": "urn:nawi-water-ontology#",
}


class TocClient:
    def __init__(self, values: list[tuple[datetime, float]] | None = None) -> None:
        self.values = values or []
        self.queries: list[str] = []

    def expand_uri(self, text):
        s = str(text)
        if s.startswith(("urn:", "http://", "https://")):
            return s
        prefix, local = s.split(":", 1)
        return PREFIXES[prefix] + local

    def compact_uri(self, item):
        s = str(item)
        for pfx, ns in PREFIXES.items():
            if s.startswith(ns):
                return f"{pfx}:{s[len(ns):]}"
        return s

    def namespace_manager(self):
        class _NM:
            def namespaces(self):
                return [(p, u) for p, u in PREFIXES.items()]

        return _NM()

    def sparql_query(self, sparql, use_union=True):
        self.queries.append(sparql)
        if "COUNT(" in sparql:
            return {"columns": [], "rows": []}
        if "SELECT DISTINCT" in sparql and " AS ?sensor" in sparql:
            return {
                "columns": ["point", "sensor"],
                "rows": [["urn:plant#ro-toc", "urn:plant#ro-toc-sensor"]],
            }
        if "SELECT DISTINCT" in sparql and " AS ?point" in sparql:
            return {
                "columns": ["point", "ref", "unit", "extunit"],
                "rows": [["urn:plant#ro-toc", "urn:ref#ro-toc", None, None]],
            }
        if "rdf-schema#label" in sparql:
            return {
                "columns": ["p", "label"],
                "rows": [["urn:plant#ro-toc", "RO permeate TOC"]],
            }
        if "hasObservationLocation" in sparql and "SELECT ?sensor ?location" in sparql:
            return {
                "columns": ["sensor", "location"],
                "rows": [["urn:plant#ro-toc-sensor", "urn:plant#ro-permeate"]],
            }
        if "SELECT DISTINCT" in sparql and " AS ?focus" in sparql:
            return {"columns": ["focus"], "rows": [["urn:plant#ro-toc"]]}
        return {"columns": [], "rows": []}

    def timeseries_info_batch(self, uris):
        return {
            str(uri): SimpleNamespace(
                row_count=len(self.values),
                earliest=self.values[0][0] if self.values else None,
                latest=self.values[-1][0] if self.values else None,
            )
            for uri in uris
        }

    def timeseries_df(self, ref_uri, **kwargs):
        return pl.DataFrame(
            {
                "ts": [ts for ts, _ in self.values],
                "uri": [ref_uri for _ in self.values],
                "value": [value for _, value in self.values],
            }
        )


def test_discover_toc_points_uses_sensor_observes_toc_property():
    points = discover_toc_points(TocClient())

    assert points == [
        TocPoint(
            point_uri="urn:plant#ro-toc",
            sensor_uri="urn:plant#ro-toc-sensor",
            location_uri="urn:plant#ro-permeate",
            label="RO permeate TOC",
        )
    ]


def test_evaluate_rule_requires_continuous_duration_strictly_greater():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rule = RO_RULES[0]

    at_duration = [(t0, 0.11), (t0 + timedelta(hours=24), 0.12)]
    after_duration = [(t0, 0.11), (t0 + timedelta(hours=24, minutes=1), 0.12)]

    assert evaluate_rule(at_duration, rule) is None
    assert evaluate_rule(after_duration, rule) is not None


def test_post_aop_critical_limit_fires_on_single_sample():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    critical = post_aop_rules(wwc=1.0)[1]

    result = evaluate_rule([(t0, 0.51)], critical)

    assert result is not None
    assert result.rule.code == "dpr_post_aop_toc_critical_limit"


def test_rules_classify_ro_and_post_aop_points():
    assert rules_for_point(TocPoint(point_uri="urn:plant#ro-permeate-toc"), 1.0) == RO_RULES

    post = rules_for_point(TocPoint(point_uri="urn:plant#post-aop-toc"), wwc=0.5)
    assert [rule.code for rule in post] == [
        "dpr_post_aop_toc_half_limit",
        "dpr_post_aop_toc_critical_limit",
    ]
    assert post[1].threshold_mg_l == 1.0


def test_app_emits_string_intervention_timeseries_for_active_alarm():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = [(t0, 0.11), (t0 + timedelta(hours=25), 0.12)]
    client = TocClient(values)
    query = SimpleNamespace(client=client)
    ctx = AppContext(
        app_id="dpr_toc_alarm",
        started_at=t0,
        start=t0,
        end=t0 + timedelta(hours=25),
        query=query,
        params={},
    )

    outputs = DprTocAlarmApp().run(ctx)

    assert len(outputs) == 1
    assert outputs[0].kind == "timeseries"
    assert outputs[0].payload["value_kind"] == "text"
    rows = outputs[0].payload["rows"]
    assert rows[0][0] == t0 + timedelta(hours=25)
    assert "dpr_ro_toc_tthmfp" in rows[0][1]
    assert "Collect a grab sample" in rows[0][1]
