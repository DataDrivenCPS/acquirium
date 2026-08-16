"""Tests for observed-read recording and the loop-safe ProvenanceWriter."""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import MagicMock

from rdflib import RDF, URIRef

from acquirium.Apps.provenance import ProvenanceWriter, graph_hash, provenance_graph
from acquirium.internals.app_utils import app_uri_for
from acquirium.internals.internals_namespaces import (
    MAY_USE, PROV_ACTIVITY, PROV_USED, PROV_WAS_GENERATED_BY, PROVENANCE_HASH,
)
from acquirium.internals.read_recorder import record_reads, recording_reads
from acquirium.Storage.graph_registry import is_provenance_graph_uri, source_graph_uri


# ─────────────────────── recorder ───────────────────────


class TestReadRecorder:
    def test_records_inside_scope_only(self):
        record_reads(["urn:r0"])                       # no scope: no-op, no error
        with recording_reads() as reads:
            record_reads(["urn:r1", "urn:r2", ""])
            record_reads(["urn:r1"])
        assert reads == {"urn:r1", "urn:r2"}
        record_reads(["urn:r3"])                       # scope closed: dropped
        assert reads == {"urn:r1", "urn:r2"}

    def test_nested_scopes_are_independent(self):
        with recording_reads() as outer:
            record_reads(["urn:outer"])
            with recording_reads() as inner:
                record_reads(["urn:inner"])
            record_reads(["urn:outer2"])
        assert inner == {"urn:inner"}
        assert outer == {"urn:outer", "urn:outer2"}

    def test_threads_do_not_share_scopes(self):
        # The task host runs many bodies in one process; each run's scope
        # must see only its own reads.
        seen = {}

        def worker(tag):
            with recording_reads() as reads:
                record_reads([f"urn:{tag}"])
                seen[tag] = set(reads)

        threads = [threading.Thread(target=worker, args=(t,)) for t in ("a", "b", "c")]
        for t in threads: t.start()
        for t in threads: t.join()
        assert seen == {"a": {"urn:a"}, "b": {"urn:b"}, "c": {"urn:c"}}

    def test_asyncio_tasks_do_not_share_scopes(self):
        async def body(tag):
            with recording_reads() as reads:
                await asyncio.sleep(0.01)
                record_reads([f"urn:{tag}"])
                await asyncio.sleep(0.01)
                return set(reads)

        async def main():
            return await asyncio.gather(body("x"), body("y"))

        assert asyncio.run(main()) == [{"urn:x"}, {"urn:y"}]

    def test_data_object_records_at_materialization(self):
        from acquirium.Client.data_object import BindingInfo, DataObject
        from acquirium.Client.query_graph import QueryGraph

        client = MagicMock()
        client.get_timeseries_batch.return_value = {}
        bindings = [
            BindingInfo(nid=1, point_uri="urn:p1", ref_uri="urn:ref1", alias="m",
                        entity_contexts=[], row_count=0, earliest=None, latest=None,
                        property_unit=None, ref_unit=None),
            BindingInfo(nid=1, point_uri="urn:p2", ref_uri="urn:ref2", alias="m",
                        entity_contexts=[], row_count=0, earliest=None, latest=None,
                        property_unit=None, ref_unit=None),
        ]
        do = DataObject(_bindings=bindings, _entity_columns=[], _query_graph=QueryGraph(),
                        _client=client, _query_params={"cast_value": "float"},
                        _tall=None, _materialized=False)
        with recording_reads() as reads:
            do.units()                                  # metadata only: no values read
            assert reads == set()
            try:
                do.latest("m")                          # values: materializes
            except Exception:
                pass                                    # stubbed client; recording happens first
        assert reads == {"urn:ref1", "urn:ref2"}


# ─────────────────────── graph + hash ───────────────────────


class TestProvenanceGraph:
    def test_shape(self):
        g = provenance_graph("app1", may_use=["urn:r1", "urn:r2"], used=["urn:r1"],
                             output_points=["urn:out"])
        app = URIRef(app_uri_for("app1"))
        assert (app, RDF.type, PROV_ACTIVITY) in g
        assert set(g.objects(app, MAY_USE)) == {URIRef("urn:r1"), URIRef("urn:r2")}
        assert set(g.objects(app, PROV_USED)) == {URIRef("urn:r1")}
        assert (URIRef("urn:out"), PROV_WAS_GENERATED_BY, app) in g
        # Never a per-output derivation claim.
        assert not any(p == PROV_USED and s == URIRef("urn:out") for s, p, o in g)

    def test_hash_is_order_independent(self):
        g1 = provenance_graph("a", may_use=["urn:1", "urn:2"], used=[], output_points=[])
        g2 = provenance_graph("a", may_use=["urn:2", "urn:1"], used=[], output_points=[])
        assert graph_hash(g1) == graph_hash(g2)
        g3 = provenance_graph("a", may_use=["urn:1"], used=[], output_points=[])
        assert graph_hash(g3) != graph_hash(g1)


# ─────────────────────── writer ───────────────────────


def make_aq(seed_hash=None):
    aq = MagicMock()
    rows = [[seed_hash]] if seed_hash else []
    aq.client.sparql_query.return_value = {"rows": rows}
    return aq


class TestProvenanceWriter:
    def test_writes_to_the_prov_graph_with_replace(self):
        aq = make_aq()
        w = ProvenanceWriter("app1", aq, min_write_interval=0)
        w.set_declared(["urn:r1"]); w.set_outputs(["urn:out"])
        assert w.flush() is True
        kw = aq.insert_graph.call_args.kwargs
        assert kw["replace"] is True
        assert kw["source_id"] == "app:app1:prov"
        assert is_provenance_graph_uri(source_graph_uri(kw["source_id"]))
        aq.sparql_update.assert_not_called()          # never the closure-rebuild path
        # The hash is written alongside so a restart can seed from it.
        assert "provenanceHash" in aq.insert_graph.call_args.args[0]

    def test_no_change_no_write(self):
        aq = make_aq()
        w = ProvenanceWriter("app1", aq, min_write_interval=0)
        w.set_declared(["urn:r1"])
        assert w.flush() is True
        assert w.flush() is False
        w.set_declared(["urn:r1"])                     # same set: still clean
        assert w.flush() is False
        assert aq.insert_graph.call_count == 1

    def test_observed_reads_union_and_converge(self):
        aq = make_aq()
        w = ProvenanceWriter("app1", aq, min_write_interval=0)
        w.set_declared(["urn:r1", "urn:r2", "urn:r3"])
        w.add_observed(["urn:r1"]);   assert w.flush() is True
        w.add_observed(["urn:r1"]);   assert w.flush() is False   # nothing new
        w.add_observed(["urn:r3"]);   assert w.flush() is True
        w.add_observed(["urn:r1", "urn:r3"]); assert w.flush() is False  # converged
        assert w.used == {"urn:r1", "urn:r3"}
        assert aq.insert_graph.call_count == 2

    def test_seeded_hash_prevents_restart_rewrite(self):
        # First process writes; a fresh writer (restart) computes the same
        # graph, finds the same hash in the store, and does not re-write.
        aq = make_aq()
        w1 = ProvenanceWriter("app1", aq, min_write_interval=0)
        w1.set_declared(["urn:r1"]); w1.set_outputs(["urn:o"])
        w1.flush()
        digest = w1._last_hash

        aq2 = make_aq(seed_hash=digest)
        w2 = ProvenanceWriter("app1", aq2, min_write_interval=0)
        w2.set_declared(["urn:r1"]); w2.set_outputs(["urn:o"])
        assert w2.flush() is False
        aq2.insert_graph.assert_not_called()

    def test_cadence_floor(self):
        aq = make_aq()
        w = ProvenanceWriter("app1", aq, min_write_interval=1000)
        w.set_declared(["urn:r1"])
        assert w.flush() is True
        w.add_observed(["urn:r1"])
        assert w.flush() is False                      # floored
        assert w.status()["pending"] is True
        assert w.flush(force=True) is True             # explicit override

    def test_write_failure_is_retried_later(self):
        aq = make_aq()
        aq.insert_graph.side_effect = [RuntimeError("server down"), None]
        w = ProvenanceWriter("app1", aq, min_write_interval=0)
        w.set_declared(["urn:r1"])
        assert w.flush() is False                      # failed, still dirty
        assert w.status()["pending"] is True
        assert w.flush() is True                       # retried
