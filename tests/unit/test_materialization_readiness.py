"""A tick reads one change index; only bindings whose inputs changed read a batch."""
from datetime import timedelta

import polars as pl

from acquirium.Materialization import RevisionStore, output
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import NOW, LineageCopy, LineageGraph


class FleetGraph(LineageGraph):
    def __init__(self, refs):
        super().__init__()
        self.refs = refs

    def sparql_query(self, query, **kwargs):
        return {"columns": ["v0", "ext0"], "rows": [[ref + ":point", ref] for ref in self.refs]}


class CopyEach(LineageCopy):
    name = "copy-each"
    grouping = "per_match"
    backfill = True
    outputs = {"out": output.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        output["out"] = inputs["input"].collect().select(["time", "value"])


def _progress(store):
    with store._own_conn() as conn:
        return dict(conn.execute("SELECT progress_key, consumed_revision FROM binding_progress").fetchall())


def _counting(monkeypatch):
    calls = []
    original = RevisionStore.next_batch

    def counted(self, binding):
        calls.append(binding.progress_key)
        return original(self, binding)
    monkeypatch.setattr(RevisionStore, "next_batch", counted)
    return calls


def test_a_write_to_one_stream_reads_one_batch_and_advances_the_rest(tmp_path, monkeypatch):
    refs = [f"urn:s{i}" for i in range(30)]
    store = DuckDBStore(tmp_path / "fleet.duckdb")
    runtime = Materializer(store, FleetGraph(refs))
    try:
        for ref in refs:
            store.upsert_rows(ref, [(NOW, 1.0)], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        calls = _counting(monkeypatch)
        store.upsert_rows("urn:s7", [(NOW + timedelta(minutes=1), 2.0)], value_kind="numeric")
        written = RevisionStore(store).current_revision()
        assert runtime.run_once()
        assert len(calls) == 1
        # Every frontier now covers the write; the one publication allocated a
        # newer revision that no binding reads.
        progress = _progress(store)
        assert len(progress) == 30 and all(value == written for value in progress.values())
        # An unrelated write moves every frontier without a single batch read.
        store.upsert_rows("urn:elsewhere", [(NOW, 5.0)], value_kind="numeric")
        current = RevisionStore(store).current_revision()
        assert not runtime.run_once()
        assert len(calls) == 1
        assert all(value == current for value in _progress(store).values())
    finally:
        runtime.close()
        store.close()


def test_a_write_landing_after_the_index_is_not_skipped(tmp_path, monkeypatch):
    store = DuckDBStore(tmp_path / "race.duckdb")
    runtime = Materializer(store, FleetGraph(["urn:a", "urn:b"]))
    try:
        store.upsert_rows("urn:a", [(NOW, 1.0)], value_kind="numeric")
        store.upsert_rows("urn:b", [(NOW, 1.0)], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        original = RevisionStore.change_index

        def index_then_write(self, floor):
            result = original(self, floor)
            # A writer slips in after the snapshot: its revision is above the
            # target the idle frontiers advance to, so the next tick sees it.
            store.upsert_rows("urn:b", [(NOW + timedelta(minutes=1), 2.0)], value_kind="numeric")
            return result
        monkeypatch.setattr(RevisionStore, "change_index", index_then_write)
        store.upsert_rows("urn:a", [(NOW + timedelta(minutes=1), 3.0)], value_kind="numeric")
        assert runtime.run_once()
        monkeypatch.setattr(RevisionStore, "change_index", original)
        binding_b = next(b for b in runtime._dag.bindings if b.inputs["input"][0].ref_uri == "urn:b")
        # b's frontier stopped at the index snapshot, below the slipped write.
        assert _progress(store)[binding_b.progress_key] < RevisionStore(store).current_revision()
        assert runtime.run_once()
        ref = binding_b.outputs["out"].ref_uri
        rows = pl.from_arrow(next(store.timeseries(ref, value_mode="numeric")))
        assert rows["value"].to_list() == [1.0, 2.0]
    finally:
        runtime.close()
        store.close()


def test_a_failing_read_marks_only_its_binding(tmp_path, monkeypatch):
    store = DuckDBStore(tmp_path / "failing.duckdb")
    runtime = Materializer(store, FleetGraph(["urn:a", "urn:b"]))
    try:
        store.upsert_rows("urn:a", [(NOW, 1.0)], value_kind="numeric")
        store.upsert_rows("urn:b", [(NOW, 1.0)], value_kind="numeric")
        runtime.deploy(Deployment.from_class(CopyEach))
        original = RevisionStore.next_batch

        def broken_for_b(self, binding):
            if binding.inputs["input"][0].ref_uri == "urn:b":
                raise RuntimeError("disk gone")
            return original(self, binding)
        monkeypatch.setattr(RevisionStore, "next_batch", broken_for_b)
        assert runtime.run_once()
        errors = list(runtime.failures().values())
        assert len(errors) == 1 and "disk gone" in errors[0]
        nodes = {node["inputs"]["input"][0]: node for node in runtime.dag()["nodes"]}
        assert nodes["urn:b"]["status"] == "failed" and nodes["urn:a"]["error"] is None
        assert next(iter(store.timeseries(nodes["urn:a"]["outputs"]["out"], value_mode="numeric"))).num_rows == 1
    finally:
        runtime.close()
        store.close()
