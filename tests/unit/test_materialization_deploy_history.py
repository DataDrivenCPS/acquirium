"""A view deployed after the model changed is planned against each past version of the model."""
from datetime import timedelta

import pytest

from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW
from tests.unit.test_materialization_effective_time import T, frame, rows
from tests.unit.test_materialization_readiness import CopyEach, FleetGraph
from tests.unit.test_materialization_repair import SumFleet


class VersionedFleet:
    """A history whose versions are fleets: ``{change_point: refs from then on}``; ``start`` is the fleet before any change."""

    def __init__(self, start, changes):
        self.start, self.changes = start, dict(sorted(changes.items()))
        self.builds = 0

    def change_points(self):
        return list(self.changes)

    def as_of(self, when):
        self.builds += 1
        refs = self.start
        for point, fleet in self.changes.items():
            if when is not None and point <= when:
                refs = fleet
        return FleetGraph(refs)

    def unapplied(self):
        return []


def _runtime(tmp_path, refs, history):
    store = DuckDBStore(tmp_path / "deploy-history.duckdb")
    graph = FleetGraph(refs)
    runtime = Materializer(store, graph)
    runtime.history = history
    for ref in {*history.start, *(r for fleet in history.changes.values() for r in fleet)}:
        store.upsert_rows(ref, [(t, 1.0) for t in T], value_kind="numeric")
    return store, graph, runtime


def _bindings(store):
    with store._own_conn() as conn:
        return conn.execute("SELECT application_name, valid_from, valid_to FROM materialization_bindings ORDER BY application_name, valid_from NULLS FIRST").fetchall()


def test_an_aggregate_deployed_after_a_removal_counts_the_removed_sensor_before_it(tmp_path):
    # Model: {a, b} until T[3], {a} after. The view arrives later and knows only {a} from the live graph.
    store, graph, runtime = _runtime(tmp_path, ["urn:a"], VersionedFleet(["urn:a", "urn:b"], {T[3]: ["urn:a"]}))
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet" and b.valid_to is None).outputs["out"].ref_uri
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 2.0, 2.0, 1.0, 1.0, 1.0]
        assert _bindings(store) == [("sum-fleet", None, T[3].isoformat()), ("sum-fleet", T[3].isoformat(), None)]
    finally:
        runtime.close(); store.close()


def test_a_per_match_view_deployed_after_a_removal_covers_the_sensor_until_the_removal(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"], VersionedFleet(["urn:a", "urn:b"], {T[3]: ["urn:a"]}))
    try:
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        outputs = {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings}
        assert rows(store, outputs["urn:a"]) == T
        assert rows(store, outputs["urn:b"]) == T[:3]
    finally:
        runtime.close(); store.close()


def test_a_sensor_that_left_and_came_back_has_two_intervals(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"],
                                     VersionedFleet(["urn:a", "urn:b"], {T[2]: ["urn:a"], T[4]: ["urn:a", "urn:b"]}))
    try:
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        out_b = next(b for b in runtime._dag.bindings if b.inputs["input"][0].ref_uri == "urn:b" and b.valid_to is None).outputs["out"].ref_uri
        assert rows(store, out_b) == T[:2] + T[4:]
    finally:
        runtime.close(); store.close()


def test_a_redeploy_keeps_its_rows_and_a_later_change_is_a_delta_against_them(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"], VersionedFleet(["urn:a", "urn:b"], {T[3]: ["urn:a"]}))
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        assert len(_bindings(store)) == 2
        # A later dated change goes through the ordinary refresh path.
        graph.refs = []
        runtime.history.changes[T[5]] = []
        runtime.note_graph_change(1, effective_at=T[5])
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet" and b.valid_from == T[3]).outputs["out"].ref_uri
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 2.0, 2.0, 1.0, 1.0]
    finally:
        runtime.close(); store.close()


def test_without_change_points_a_deployment_covers_all_history_as_before(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"], VersionedFleet(["urn:a", "urn:b"], {}))
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        assert runtime.history.builds == 0
        assert _bindings(store) == [("sum-fleet", None, None)]
    finally:
        runtime.close(); store.close()
