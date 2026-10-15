"""A view over another view's output follows that producer's validity, whenever it is planned.

The lineage graph lists the derived streams of closed bindings too. A plan
binds a derived stream only while its producer holds: the live plan binds
what is produced now, and a view's first plan, made against each past
version of the model, binds what was produced then. So a consumer deployed
after a model change covers a removed point until the change, and a point
added mid-bucket counts for an unbucketed consumer from the change itself,
not from the start of the producer's bucket.
"""
from datetime import timedelta

from rdflib import Literal

from acquirium.Materialization import align
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, PRODUCED_BY
from tests.unit.test_incremental_materialization import LineageGraph
from tests.unit.test_materialization_effective_time import T, rows
from tests.unit.test_materialization_readiness import CopyEach
from tests.unit.test_materialization_repair import SumFleet


class ChainGraph(LineageGraph):
    """Raw points by fleet; a query for a view's output (``app=``) finds the derived streams the lineage lists."""

    def __init__(self, refs, *, producer, published=None):
        super().__init__()
        self.refs, self.producer = refs, producer
        if published is not None:
            self.published = published

    def graph_status(self):
        # Publishing lineage advances the graph, as it does in the real store.
        return {"published_version": 1 + len(self.published), "source_version": 1 + len(self.published)}

    def sparql_query(self, query, **kwargs):
        if self.producer in query:
            graph = self.published[-1] if self.published else None
            refs = sorted(str(ref) for point in (graph.subjects(PRODUCED_BY, Literal(self.producer)) if graph is not None else ())
                          for ref in graph.objects(point, HAS_EXTERNAL_REFERENCE))
        else:
            refs = self.refs
        return {"columns": ["v0", "ext0"], "rows": [[ref + ":point", ref] for ref in refs]}


class ChainHistory:
    """Versions are fleets, sharing the live graph's lineage: ``{change_point: refs from then on}``."""

    def __init__(self, live, start, changes):
        self.live, self.start, self.changes = live, start, dict(sorted(changes.items()))

    def change_points(self):
        return list(self.changes)

    def as_of(self, when):
        refs = self.start
        for point, fleet in self.changes.items():
            if when is not None and point <= when:
                refs = fleet
        return ChainGraph(refs, producer=self.live.producer, published=self.live.published)

    def unapplied(self):
        return []


class CopyDerived(CopyEach):
    name = "copy-derived"

    def build_query(self, plant):
        return plant.query().measurement(alias="input", app="copy-each")


class TwoHourEach(CopyEach):
    name = "two-hour-each"
    every = "2h"

    def transform(self, inputs, output, context):
        output["out"] = align(inputs, "2h").rename({"input": "value"})


class CopyTwoHour(CopyEach):
    name = "copy-two-hour"

    def build_query(self, plant):
        return plant.query().measurement(alias="input", app="two-hour-each")


class SumDerived(SumFleet):
    name = "sum-derived"

    def build_query(self, plant):
        return plant.query().measurement(alias="input", app="copy-each")


def _runtime(tmp_path, live_refs, start, changes, producer):
    store = DuckDBStore(tmp_path / "chain.duckdb")
    graph = ChainGraph(live_refs, producer=producer)
    runtime = Materializer(store, graph)
    runtime.history = ChainHistory(graph, start, changes)
    for ref in sorted({*start, *(r for fleet in changes.values() for r in fleet)}):
        store.upsert_rows(ref, [(t, 1.0) for t in T], value_kind="numeric")
    return store, runtime


def _by_input(runtime, app):
    return {b.inputs["input"][0].ref_uri: b for b in runtime._dag.bindings if b.application_name == app}


def test_a_consumer_deployed_after_a_removal_covers_the_producers_closed_interval(tmp_path):
    # Model: {a, b} until T[3], {a} after. The producer is deployed, then the consumer.
    store, runtime = _runtime(tmp_path, ["urn:a"], ["urn:a", "urn:b"], {T[3]: ["urn:a"]}, producer="copy-each")
    try:
        runtime.deploy(Deployment.from_class(CopyEach))
        while runtime.run_once():
            pass
        runtime.deploy(Deployment.from_class(CopyDerived))
        while runtime.run_once():
            pass
        producer = {ref: b.outputs["out"].ref_uri for ref, b in _by_input(runtime, "copy-each").items()}
        consumer = _by_input(runtime, "copy-derived")
        assert rows(store, consumer[producer["urn:a"]].outputs["out"].ref_uri) == T
        # b's producer closed at T[3]; the consumer binds it over that interval only.
        assert (consumer[producer["urn:b"]].valid_from, consumer[producer["urn:b"]].valid_to) == (None, T[3])
        assert rows(store, consumer[producer["urn:b"]].outputs["out"].ref_uri) == T[:3]
        # The lineage lists both, the live plan binds only a.
        assert [b.valid_to for b in runtime._dag.bindings if b.application_name == "copy-derived"
                and b.inputs["input"][0].ref_uri == producer["urn:b"]] == [T[3]]
    finally:
        runtime.close(); store.close()


def test_a_consumer_of_a_bucketed_producer_counts_an_added_point_from_the_change_itself(tmp_path):
    # c joins at T[3] + 30 min, inside the producer's bucket [T[2], T[4]). The
    # producer owns whole buckets, so its binding for c starts at T[2]; the
    # consumer has no bucket, so its binding starts at the change itself and
    # leaves the producer's T[2] row out.
    at = T[3] + timedelta(minutes=30)
    store, runtime = _runtime(tmp_path, ["urn:a", "urn:c"], ["urn:a"], {at: ["urn:a", "urn:c"]}, producer="two-hour-each")
    try:
        runtime.deploy(Deployment.from_class(TwoHourEach))
        while runtime.run_once():
            pass
        runtime.deploy(Deployment.from_class(CopyTwoHour))
        while runtime.run_once():
            pass
        producer = _by_input(runtime, "two-hour-each")
        assert producer["urn:c"].valid_from == T[2]
        assert rows(store, producer["urn:c"].outputs["out"].ref_uri) == [T[2], T[4]]
        consumer = _by_input(runtime, "copy-two-hour")[producer["urn:c"].outputs["out"].ref_uri]
        assert consumer.valid_from == at
        assert rows(store, consumer.outputs["out"].ref_uri) == [T[4]]
        unchanged = _by_input(runtime, "copy-two-hour")[producer["urn:a"].outputs["out"].ref_uri]
        assert unchanged.valid_from is None and rows(store, unchanged.outputs["out"].ref_uri) == [T[0], T[2], T[4]]
    finally:
        runtime.close(); store.close()


def test_an_aggregate_deployed_with_its_producer_is_planned_once_the_producer_is_in_the_graph(tmp_path):
    # Both views are deployed before anything runs. The aggregate's first
    # refresh finds no producer output in the graph and holds only a row with
    # no streams; its plan against history happens at the refresh that sees
    # the producer's streams, and splits at b's removal (T[3]).
    store, runtime = _runtime(tmp_path, ["urn:a"], ["urn:a", "urn:b"], {T[3]: ["urn:a"]}, producer="copy-each")
    try:
        runtime.deploy(Deployment.from_class(CopyEach))
        runtime.deploy(Deployment.from_class(SumDerived))
        while runtime.run_once():
            pass
        fleet = sorted(((b.valid_from, b.valid_to) for b in runtime._dag.bindings if b.application_name == "sum-derived"),
                       key=lambda iv: (iv[0] is not None, iv[0] or T[0]))
        assert fleet == [(None, T[3]), (T[3], None)]
        fleet_ref = next(b for b in runtime._dag.bindings if b.application_name == "sum-derived").outputs["out"].ref_uri
        from tests.unit.test_materialization_effective_time import frame
        assert frame(store, fleet_ref).sort("time")["value"].to_list() == [2.0, 2.0, 2.0, 1.0, 1.0, 1.0]
    finally:
        runtime.close(); store.close()


def test_a_consumer_closes_with_its_producer_when_the_point_is_removed_later(tmp_path):
    # Both views run; then b leaves at T[3]. The consumer's binding for b
    # closes at T[3] and keeps the rows before it.
    store, runtime = _runtime(tmp_path, ["urn:a", "urn:b"], ["urn:a", "urn:b"], {}, producer="copy-each")
    try:
        runtime.deploy(Deployment.from_class(CopyEach))
        runtime.deploy(Deployment.from_class(CopyDerived))
        while runtime.run_once():
            pass
        producer = {ref: b.outputs["out"].ref_uri for ref, b in _by_input(runtime, "copy-each").items()}
        out_b = _by_input(runtime, "copy-derived")[producer["urn:b"]].outputs["out"].ref_uri
        assert rows(store, out_b) == T
        runtime._graph.refs = ["urn:a"]
        runtime.history.changes[T[3]] = ["urn:a"]
        runtime.note_graph_change(1, effective_at=T[3])
        while runtime.run_once():
            pass
        consumer_b = _by_input(runtime, "copy-derived")[producer["urn:b"]]
        assert consumer_b.valid_to == T[3]
        assert rows(store, out_b) == T[:3]
    finally:
        runtime.close(); store.close()
