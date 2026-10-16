"""A model change takes effect at the time its author gives, not when it is written."""
from datetime import timedelta

import polars as pl
import pytest

from acquirium.Materialization import align
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW
from tests.unit.test_materialization_readiness import CopyEach, FleetGraph
from tests.unit.test_materialization_repair import SumFleet

T = [NOW + timedelta(hours=h) for h in range(6)]   # six hourly samples, change effective at T[3]


def frame(store, ref):
    batches = list(store.timeseries(ref, value_mode="numeric"))
    return pl.from_arrow(batches[0]).rename({"ts": "time"}) if batches else pl.DataFrame({"time": [], "value": []})


def rows(store, ref):
    return [t.replace(tzinfo=NOW.tzinfo) if t.tzinfo is None else t for t in frame(store, ref)["time"].to_list()]


def _runtime(tmp_path, refs):
    store = DuckDBStore(tmp_path / "effective.duckdb")
    graph = FleetGraph(refs)
    runtime = Materializer(store, graph)
    for ref in refs:
        store.upsert_rows(ref, [(t, 1.0) for t in T], value_kind="numeric")
    runtime.deploy(Deployment.from_class(CopyEach))
    while runtime.run_once():
        pass
    return store, graph, runtime


def _outputs(runtime):
    return {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings if b.application_name == "copy-each"}


def test_a_removed_sensor_keeps_its_derived_rows_before_the_effective_time(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        out_b = _outputs(runtime)["urn:b"]
        assert rows(store, out_b) == T
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        assert rows(store, out_b) == T[:3]
        assert rows(store, _outputs(runtime)["urn:a"]) == T
        with store._own_conn() as conn:
            closed = conn.execute("SELECT valid_to FROM materialization_bindings WHERE valid_to IS NOT NULL").fetchall()
        assert len(closed) == 1 and closed[0][0].startswith(T[3].isoformat()[:19])
    finally:
        runtime.close(); store.close()


def test_a_removal_defaults_to_now_and_keeps_history(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        out_b = _outputs(runtime)["urn:b"]
        graph.refs = ["urn:a"]
        runtime.note_graph_change(2)
        while runtime.run_once():
            pass
        assert rows(store, out_b) == T   # all of it is before now
    finally:
        runtime.close(); store.close()


def test_an_added_sensor_is_computed_from_the_effective_time_only(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    try:
        store.upsert_rows("urn:c", [(t, 2.0) for t in T], value_kind="numeric")   # history exists before the model knew it
        graph.refs = ["urn:a", "urn:c"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        assert rows(store, _outputs(runtime)["urn:c"]) == T[3:]
        # A later correction before the effective time is not this binding's concern.
        store.upsert_rows("urn:c", [(T[0], 9.0)], value_kind="numeric")
        while runtime.run_once():
            pass
        assert rows(store, _outputs(runtime)["urn:c"]) == T[3:]
        # One after it is.
        store.upsert_rows("urn:c", [(T[5] + timedelta(hours=1), 9.0)], value_kind="numeric")
        while runtime.run_once():
            pass
        assert rows(store, _outputs(runtime)["urn:c"]) == T[3:] + [T[5] + timedelta(hours=1)]
    finally:
        runtime.close(); store.close()


def test_a_new_deployment_still_covers_retained_history(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet")
        assert fleet.valid_from is None and rows(store, fleet.outputs["out"].ref_uri) == T
    finally:
        runtime.close(); store.close()


def test_an_aggregate_moves_to_its_new_membership_at_the_effective_time(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        assert rows(store, fleet_ref) == T
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 2.0, 2.0, 1.0, 1.0, 1.0]
    finally:
        runtime.close(); store.close()


def test_a_context_change_repairs_from_the_effective_time(tmp_path):
    class RelabelGraph(FleetGraph):
        labels: dict = {}
        def sparql_query(self, query, **kwargs):
            return {"columns": ["v0", "ext0", "lbl0"], "rows": [[ref + ":point", ref, self.labels.get(ref)] for ref in self.refs]}

    store = DuckDBStore(tmp_path / "relabel.duckdb")
    graph = RelabelGraph(["urn:a"])
    runtime = Materializer(store, graph)
    try:
        store.upsert_rows("urn:a", [(t, 1.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        graph.labels["urn:a"] = "renamed"
        runtime.note_graph_change(1, effective_at=T[3])
        runtime.refresh()
        with store._own_conn() as conn:
            cursor = conn.execute("SELECT cursor_ts FROM materialization_work").fetchone()[0]
        assert cursor == T[3].isoformat()
    finally:
        runtime.close(); store.close()


def test_validity_survives_restart(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    store.upsert_rows("urn:c", [(t, 2.0) for t in T], value_kind="numeric")
    graph.refs = ["urn:a", "urn:c"]
    runtime.note_graph_change(1, effective_at=T[3])
    while runtime.run_once():
        pass
    runtime.close()
    reborn = Materializer(store, graph)
    try:
        reborn.refresh()
        valid = {b.inputs["input"][0].ref_uri: b.valid_from for b in reborn._dag.bindings}
        assert valid == {"urn:a": None, "urn:c": T[3]}
    finally:
        reborn.close(); store.close()


class HourlyEach(CopyEach):
    name = "hourly-each"
    every = "2h"

    def transform(self, inputs, output, context):
        output["out"] = align(inputs, "2h").rename({"input": "value"})


def test_a_bucketed_view_switches_at_the_start_of_the_bucket(tmp_path):
    store = DuckDBStore(tmp_path / "bucket.duckdb")
    graph = FleetGraph(["urn:a", "urn:b"])
    runtime = Materializer(store, graph)
    try:
        for ref in graph.refs:
            store.upsert_rows(ref, [(t, 1.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(HourlyEach))
        while runtime.run_once():
            pass
        out_b = {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings}["urn:b"]
        assert rows(store, out_b) == [T[0], T[2], T[4]]
        # Effective at T[3], inside the bucket [T[2], T[4]): that whole bucket goes.
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        assert rows(store, out_b) == [T[0]]
        with store._own_conn() as conn:
            closed = conn.execute("SELECT valid_to FROM materialization_bindings WHERE valid_to IS NOT NULL").fetchone()[0]
        assert closed == T[2].isoformat()
    finally:
        runtime.close(); store.close()


def test_a_dated_change_carries_through_the_lineage_it_publishes(tmp_path):
    """Retiring a binding republishes lineage, which advances the graph; the
    bindings that disappear downstream at the refresh that sees it belong to
    the same change, so its effective time is kept for that revision."""
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        graph.refs = ["urn:a"]
        graph.graph_status = lambda: {"published_version": 1, "source_version": 1 + len(graph.published)}
        runtime.note_graph_change(1, effective_at=T[3])
        runtime.refresh()
        with store._own_conn() as conn:
            queued = conn.execute("SELECT effective_at FROM materialization_graph_changes").fetchall()
        assert [row[0] for row in queued] == [T[3].isoformat()]
    finally:
        runtime.close(); store.close()


def test_a_change_the_plan_cannot_see_yet_keeps_its_effective_time(tmp_path):
    """The query cache lags a graph write. A refresh that compiled against the
    older cache must not consume the change, or the binding that appears at
    the next refresh would have no effective time."""
    class LaggingGraph(FleetGraph):
        def __init__(self, refs):
            super().__init__(refs)
            self.version = 1
            self.pending_refs = None

        def graph_status(self):
            return {"published_version": self.version, "source_version": self.version}

        def catch_up(self):
            self.refs, self.pending_refs, self.version = self.pending_refs, None, self.version + 1

    store = DuckDBStore(tmp_path / "lag.duckdb")
    graph = LaggingGraph(["urn:a"])
    runtime = Materializer(store, graph)
    try:
        store.upsert_rows("urn:a", [(t, 1.0) for t in T], value_kind="numeric")
        store.upsert_rows("urn:c", [(t, 2.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        # The write lands at source version 2; the cache still shows version 1.
        graph.pending_refs = ["urn:a", "urn:c"]
        runtime.note_graph_change(2, effective_at=T[3])
        while runtime.run_once():
            pass
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_graph_changes").fetchone()[0] == 1
        graph.catch_up()
        while runtime.run_once():
            pass
        assert rows(store, _outputs(runtime)["urn:c"]) == T[3:]
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_graph_changes").fetchone()[0] == 0
    finally:
        runtime.close(); store.close()


def test_a_graph_write_records_its_effective_time_before_any_plan_sees_it(tmp_path):
    """A refresh racing the write must wait: it may not plan the new binding
    until the change's effective time is recorded."""
    from threading import Thread
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    try:
        store.upsert_rows("urn:c", [(t, 2.0) for t in T], value_kind="numeric")
        racer = Thread(target=runtime.refresh)
        with runtime.graph_change(effective_at=T[3]):
            graph.refs = ["urn:a", "urn:c"]
            runtime._graph_revision = -1
            racer.start()
            racer.join(0.3)
            assert racer.is_alive(), "a refresh must not plan during a graph write"
        racer.join()
        while runtime.run_once():
            pass
        assert rows(store, _outputs(runtime)["urn:c"]) == T[3:]
    finally:
        runtime.close(); store.close()


def test_a_late_reading_before_a_membership_change_reaches_the_aggregate_as_it_was(tmp_path):
    """The closed fleet binding keeps maintaining its own interval."""
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        store.upsert_rows("urn:b", [(T[1], 5.0)], value_kind="numeric")   # a correction before the change
        store.upsert_rows("urn:a", [(T[4], 3.0)], value_kind="numeric")   # and one after
        while runtime.run_once():
            pass
        assert rows(store, fleet_ref) == T
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 6.0, 2.0, 1.0, 3.0, 1.0]
    finally:
        runtime.close(); store.close()


def test_a_removed_app_leaves_no_history_for_a_later_one(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        runtime.remove("copy-each")
        runtime.deploy(Deployment.from_class(CopyEach))
        runtime.refresh()
        assert all(b.valid_to is None for b in runtime._dag.bindings)
    finally:
        runtime.close(); store.close()


def test_bindings_that_disappear_with_an_upstream_view_are_not_kept(tmp_path):
    """Removing a view is a dataflow change: its consumers have no interval to keep."""
    from tests.unit.test_incremental_materialization import LineageGraph
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    try:
        runtime.remove("copy-each")
        runtime.refresh()
        assert not runtime._dag.bindings
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_bindings").fetchone()[0] == 0
    finally:
        runtime.close(); store.close()


def test_a_consumer_orphaned_by_a_view_removal_is_dropped_even_alongside_a_model_change(tmp_path):
    """Removing an upstream view and dating a model change in the same refresh:
    the consumer of the removed view is dropped, the sensor's own bindings
    are kept over their interval."""
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        derived = list(_outputs(runtime).values())
        row = lambda key, app, src, out, until=None: {"key": key, "app": app, "valid_to": until, "definition": {
            "inputs": {"input": [{"ref_uri": src}]}, "outputs": {"out": [out, "x", "urn:p"]}}}
        table = [row("k1", "copy-of-copies", derived[0], "urn:d1"),      # closing now, orphaned
                 row("k2", "copy-each", "urn:b", derived[1]),             # closing now, a sensor change
                 row("k3", "copy-of-copies", derived[1], "urn:d3", T[2]), # closed before, still fed by k2
                 row("k4", "copy-of-copies", "urn:d1", "urn:d4", T[2])]   # closed before, below k1
        deployments = {"copy-each": None, "copy-of-copies": None}
        with store._lock, store._write_conn() as conn:
            conn.execute("INSERT INTO streams VALUES ('urn:d1', NULL, 'derived:copy-of-copies', 'd1', 'numeric')")
        dropped = runtime._dataflow_dropped({"k1": T[3], "k2": T[3]}, table, {}, deployments)
        assert dropped == {"k1", "k4"}
    finally:
        runtime.close(); store.close()


def test_a_dataflow_change_supersedes_the_history_of_a_named_output(tmp_path):
    """After a dated change left a historical fleet binding, an unrecorded
    change gives the fleet a binding over all history; it supersedes the
    historical owner instead of conflicting with it."""
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        store.upsert_rows("urn:c", [(t, 4.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        graph.refs = ["urn:a", "urn:c"]
        runtime._graph_revision = -1          # changed with no model change recorded
        while runtime.run_once():
            pass
        assert "graph" not in runtime.failures()
        fleets = [b for b in runtime._dag.bindings if b.application_name == "sum-fleet"]
        assert len(fleets) == 1 and fleets[0].valid_from is None
        assert frame(store, fleet_ref)["value"].to_list() == [5.0] * 6
    finally:
        runtime.close(); store.close()


def test_a_stream_replacement_rebuilds_each_interval_of_a_split_aggregate(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 2.0, 2.0, 1.0, 1.0, 1.0]
        # Replace urn:a without T[1] and T[4]: both intervals rebuild, each over its own members.
        store.replace_rows("urn:a", [(t, 1.0) for i, t in enumerate(T) if i not in (1, 4)], value_kind="numeric")
        while runtime.run_once():
            pass
        f = frame(store, fleet_ref)
        assert dict(zip(rows(store, fleet_ref), f["value"].to_list())) == {T[0]: 2.0, T[1]: 1.0, T[2]: 2.0, T[3]: 1.0, T[5]: 1.0}
    finally:
        runtime.close(); store.close()


def test_a_membership_that_comes_back_keeps_maintaining_its_earlier_interval(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[2])
        while runtime.run_once():
            pass
        graph.refs = ["urn:a", "urn:b"]          # the same fleet again: the same binding identity
        runtime.note_graph_change(1, effective_at=T[4])
        while runtime.run_once():
            pass
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 2.0, 1.0, 1.0, 2.0, 2.0]
        store.upsert_rows("urn:b", [(T[1], 5.0)], value_kind="numeric")   # before the first change
        store.upsert_rows("urn:b", [(T[5], 5.0)], value_kind="numeric")   # after the second
        while runtime.run_once():
            pass
        assert frame(store, fleet_ref)["value"].to_list() == [2.0, 6.0, 1.0, 1.0, 2.0, 6.0]
    finally:
        runtime.close(); store.close()


def test_an_explicit_reprocess_stays_inside_each_bindings_interval(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a"])
    try:
        runtime.deploy(Deployment.from_class(HourlyEach))
        while runtime.run_once():
            pass
        store.upsert_rows("urn:c", [(t, 2.0) for t in T], value_kind="numeric")
        graph.refs = ["urn:a", "urn:c"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        out_c = {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings
                 if b.application_name == "hourly-each"}["urn:c"]
        assert rows(store, out_c) == [T[2], T[4]]
        runtime.reprocess("hourly-each", T[0], T[5])
        while runtime.run_once():
            pass
        assert rows(store, out_c) == [T[2], T[4]]
    finally:
        runtime.close(); store.close()


def test_a_consumer_left_without_inputs_by_a_view_removal_retracts_all_its_history_even_alongside_a_dated_change(tmp_path):
    """The upstream view goes (dataflow) in the same refresh as an unrelated dated
    model change: the consumer's replacement binding, now with no inputs, is
    dataflow fallout and covers all history, so every stale row is retracted,
    not only those after the change."""
    store = DuckDBStore(tmp_path / "fallout.duckdb")
    graph = FleetGraph(["urn:d1"])
    runtime = Materializer(store, graph)
    try:
        with store._lock, store._write_conn() as conn:
            conn.execute("INSERT INTO streams VALUES ('urn:d1', NULL, 'derived:other-view', 'd1', 'numeric')")
        store.upsert_rows("urn:d1", [(t, 1.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet").outputs["out"].ref_uri
        assert rows(store, fleet_ref) == T
        graph.refs = []                                   # the view that produced urn:d1 was removed...
        runtime.note_graph_change(1, effective_at=T[3])   # ...and an unrelated model change is dated T[3]
        while runtime.run_once():
            pass
        fleet = [b for b in runtime._dag.bindings if b.application_name == "sum-fleet"]
        assert len(fleet) == 1 and fleet[0].valid_from is None
        assert rows(store, fleet_ref) == []
    finally:
        runtime.close(); store.close()


def test_pending_work_is_clipped_to_the_validity_the_binding_has_now(tmp_path):
    """A reprocess is requested while b is in the model; before it runs, b
    leaves at T[3]. The closed binding owns [start, T[2]) only: the chunk must
    not write the buckets after that, which now belong to nobody, and the work
    must still finish."""
    store = DuckDBStore(tmp_path / "clipped-work.duckdb")
    graph = FleetGraph(["urn:a", "urn:b"])
    runtime = Materializer(store, graph)
    try:
        for ref in graph.refs:
            store.upsert_rows(ref, [(t, 1.0) for t in T], value_kind="numeric")
        runtime.deploy(Deployment.from_class(HourlyEach))
        while runtime.run_once():
            pass
        out_b = {b.inputs["input"][0].ref_uri: b.outputs["out"].ref_uri for b in runtime._dag.bindings}["urn:b"]
        assert rows(store, out_b) == [T[0], T[2], T[4]]
        runtime.reprocess("hourly-each", T[0], T[5])          # work rows over every bucket, b included
        graph.refs = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])      # b closes at T[2], before the work ran
        while runtime.run_once():
            pass
        assert rows(store, out_b) == [T[0]]
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_work").fetchone()[0] == 0
    finally:
        runtime.close(); store.close()
