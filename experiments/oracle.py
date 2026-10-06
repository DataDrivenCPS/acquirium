"""From-scratch recomputation of every view, and the comparison with the server.

The oracle knows which points exist (the harness tracks graph edits), which
views are deployed with which parameters (the harness tracks dataflow
edits), and the canonical data (the replay tracks data edits). It derives
output stream identities the same way the planner does, so expected and
actual outputs can be matched by reference URI.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl

from acquirium.Materialization.models import StreamDescriptor
from acquirium.Materialization.planner import output_port
from experiments import views as V
from experiments.benicia import Point
from experiments.common import derived_nodes, fetch_stream


def derived_ref(app: type, port: str, inputs: dict[str, list[str]]) -> str:
    descriptors = {alias: [StreamDescriptor(ref) for ref in refs] for alias, refs in inputs.items()}
    return output_port(app.name, port, descriptors, app.outputs[port]).ref_uri


@dataclass
class World:
    """What the harness believes the server should contain."""
    points: dict[str, Point]                       # ref_uri -> point currently in the graph
    deployed: dict[str, dict[str, Any]] = field(default_factory=dict)  # view name -> parameters

    def refs_with(self, quantity_kind: str) -> list[str]:
        return sorted(ref for ref, point in self.points.items() if point.quantity_kind == quantity_kind)


def expected(world: World, truth: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """Every derived stream the deployed views should hold, keyed by reference URI."""
    out: dict[str, pl.DataFrame] = {}
    data = lambda ref: truth.get(ref, V.EMPTY)

    v1 = {}
    if V.ConcentrationFiveMinute.name in world.deployed:
        for ref in world.refs_with(V.CONCENTRATION):
            if ref not in truth:
                continue  # no data yet: the server has no binding to compare
            v1[ref] = derived_ref(V.ConcentrationFiveMinute, "mean", {"x": [ref]})
            out[v1[ref]] = V.ConcentrationFiveMinute.oracle(data(ref))
    v2 = {}
    if V.ConcentrationRollingHour.name in world.deployed:
        for ref in v1.values():
            v2[ref] = derived_ref(V.ConcentrationRollingHour, "rolling", {"x": [ref]})
            out[v2[ref]] = V.ConcentrationRollingHour.oracle(out[ref])
    v3 = {}
    if V.ConcentrationHigh.name in world.deployed:
        threshold = float(world.deployed[V.ConcentrationHigh.name].get("threshold", 90.0))
        for ref in v2.values():
            v3[ref] = derived_ref(V.ConcentrationHigh, "alarm", {"x": [ref]})
            out[v3[ref]] = V.ConcentrationHigh.oracle(out[ref], threshold)
    if V.AlarmCountHour.name in world.deployed:
        ref = derived_ref(V.AlarmCountHour, "count", {"alarms": sorted(v3.values())})
        out[ref] = V.AlarmCountHour.oracle([out[r] for r in v3.values()])
    if V.FlowTotalFiveMinute.name in world.deployed:
        flows = [r for r in world.refs_with(V.FLOW) if r in truth]
        ref = derived_ref(V.FlowTotalFiveMinute, "total", {"flow": flows})
        out[ref] = V.FlowTotalFiveMinute.oracle([data(r) for r in flows])
    if V.AcidityDailyRange.name in world.deployed:
        for ref in world.refs_with(V.ACIDITY):
            if ref in truth:
                out[derived_ref(V.AcidityDailyRange, "range", {"x": [ref]})] = V.AcidityDailyRange.oracle(data(ref))
    return out


def _same(expected_frame: pl.DataFrame, actual: pl.DataFrame, tolerance: float) -> str | None:
    e = expected_frame.sort("time")
    a = actual.sort("time")
    if e.height != a.height:
        return f"{e.height} rows expected, {a.height} stored"
    if e.is_empty():
        return None
    if not e["time"].cast(pl.Datetime("us", "UTC")).equals(a["time"].cast(pl.Datetime("us", "UTC"))):
        return "timestamps differ"
    if e["value"].dtype == pl.Utf8:
        if not e["value"].equals(a["value"].cast(pl.Utf8)):
            return "text values differ"
        return None
    diff = (e["value"].cast(pl.Float64) - a["value"].cast(pl.Float64)).abs().fill_nan(0.0)
    worst = diff.max()
    return None if worst is None or worst <= tolerance else f"max abs difference {worst:.3g}"


def compare(client: Any, want: dict[str, pl.DataFrame], *, tolerance: float = 1e-6) -> list[dict[str, Any]]:
    """Mismatches between the server's derived streams and the oracle's."""
    owned: dict[str, str] = {}
    for node in derived_nodes(client):
        for ref in node["outputs"].values():
            owned[ref] = node["application_name"]
    problems = []
    for ref, frame in want.items():
        if ref not in owned:
            problems.append({"ref": ref, "problem": "no binding owns the expected output"})
            continue
        reason = _same(frame, fetch_stream(client, ref), tolerance)
        if reason:
            problems.append({"ref": ref, "app": owned[ref], "problem": reason})
    for ref, app in owned.items():
        if ref not in want:
            problems.append({"ref": ref, "app": app, "problem": "binding exists that the oracle does not expect"})
    return problems
