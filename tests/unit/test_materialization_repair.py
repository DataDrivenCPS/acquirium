"""A membership change repairs the bindings whose own context changed, not their siblings."""
import polars as pl

from acquirium.Materialization import output
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW, LineageCopy
from tests.unit.test_materialization_readiness import CopyEach, FleetGraph


class SumFleet(LineageCopy):
    name = "sum-fleet"
    grouping = "all_matches"
    backfill = True
    outputs = {"out": output.named("fleet-sum", value_kind="numeric")}

    def transform(self, inputs, output, context):
        output["out"] = inputs["input"].df().group_by("time").agg(pl.col("value").sum())


def _lineage(store):
    with store._own_conn() as conn:
        return dict(conn.execute("SELECT DISTINCT progress_key, context_hash FROM materialization_lineage").fetchall())


def _work(store):
    with store._own_conn() as conn:
        return {row[0] for row in conn.execute("SELECT progress_key FROM materialization_work").fetchall()}


def test_removing_one_match_leaves_sibling_per_match_bindings_alone(tmp_path):
    refs = ["urn:a", "urn:b", "urn:c"]
    store = DuckDBStore(tmp_path / "repair.duckdb")
    graph = FleetGraph(refs)
    runtime = Materializer(store, graph)
    try:
        for ref in refs:
            store.upsert_rows(ref, [(NOW, 1.0)], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        runtime.deploy(Deployment.from_class(SumFleet))
        while runtime.run_once():
            pass
        before = _lineage(store)
        by_input = {b.inputs["input"][0].ref_uri: b for b in runtime._dag.bindings if b.application_name == "copy-each"}
        fleet = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet")
        assert not _work(store)

        graph.refs = ["urn:a", "urn:c"]
        runtime._graph_revision = -1  # the fake graph has no version counter
        runtime.refresh()

        after = _lineage(store)
        for ref in ("urn:a", "urn:c"):
            key = by_input[ref].progress_key
            assert after[key] == before[key]
        assert by_input["urn:b"].progress_key not in after
        # The aggregate's inputs changed: its named output moves to a new
        # binding, and that binding alone is scheduled for repair.
        new_fleet = next(b for b in runtime._dag.bindings if b.application_name == "sum-fleet")
        assert new_fleet.progress_key != fleet.progress_key
        assert _work(store) == {new_fleet.progress_key}
    finally:
        runtime.close()
        store.close()


def test_a_bindings_own_row_change_still_repairs_it(tmp_path):
    class RelabelGraph(FleetGraph):
        def __init__(self, refs):
            super().__init__(refs)
            self.labels = {}

        def sparql_query(self, query, **kwargs):
            return {"columns": ["v0", "ext0", "lbl0"],
                    "rows": [[ref + ":point", ref, self.labels.get(ref)] for ref in self.refs]}

    store = DuckDBStore(tmp_path / "relabel.duckdb")
    graph = RelabelGraph(["urn:a", "urn:b"])
    runtime = Materializer(store, graph)
    try:
        for ref in graph.refs:
            store.upsert_rows(ref, [(NOW, 1.0)], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        before = _lineage(store)
        keys = {b.inputs["input"][0].ref_uri: b.progress_key for b in runtime._dag.bindings}
        graph.labels["urn:b"] = "renamed"
        runtime._graph_revision = -1
        runtime.refresh()
        after = _lineage(store)
        assert after[keys["urn:a"]] == before[keys["urn:a"]]
        assert after[keys["urn:b"]] != before[keys["urn:b"]]
        assert _work(store) == {keys["urn:b"]}
    finally:
        runtime.close()
        store.close()
