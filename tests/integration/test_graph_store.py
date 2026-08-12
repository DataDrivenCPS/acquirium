"""Integration tests for OxigraphGraphStore — in-process Oxigraph.

No external services required (Oxigraph runs in-process via pyoxigraph).
Note: Oxigraph's file locking may abort on macOS; these tests are primarily
targeted at Linux CI runners.
"""

import platform
import pytest
import json
import pyoxigraph as ox
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from unittest.mock import patch

from rdflib import Graph, URIRef, Literal, RDF, RDFS

from acquirium.Storage.graph_store import OxigraphGraphStore
from acquirium.internals.internals_namespaces import (
    DEFAULT_MAIN_GRAPH,
    ACQUIRIUM_POINT_NS,
)
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI

# Oxigraph file locking can SIGABRT on macOS in temp directories
pytestmark = pytest.mark.skipif(
    platform.system() == "Darwin",
    reason="Oxigraph file locking unreliable on macOS; runs on Linux CI",
)


SAMPLE_TURTLE = """\
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:sensor1 a ex:TemperatureSensor ;
    rdfs:label "Sensor 1" ;
    ex:hasUnit "degC" .

ex:sensor2 a ex:PressureSensor ;
    rdfs:label "Sensor 2" ;
    ex:hasUnit "kPa" .
"""

EXTRA_TURTLE = """\
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:sensor3 a ex:FlowSensor ;
    rdfs:label "Sensor 3" .
"""

IMPORTS_WATER_TURTLE = """\
@prefix ex: <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .

ex:deployment owl:imports <urn:nawi-water-ontology> .
"""


@pytest.fixture
def graph_store(tmp_path):
    store_path = tmp_path / "oxigraph"
    env_root = tmp_path / "ontoenv"
    store_path.mkdir()
    env_root.mkdir()

    gs = OxigraphGraphStore(
        store_path=store_path,
        env_root=env_root,
    )
    yield gs
    gs.close()


# ── Insert & Export Tests ──────────────────────────────────


class TestInsertExport:
    def test_insert_turtle(self, graph_store):
        result = graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        assert result == {"replaced": False, "changed": True}

        exported = graph_store.export_graph(include_dependencies=False, format="turtle")
        assert "sensor1" in exported
        assert "sensor2" in exported

    def test_insert_replace(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=True)

        exported = graph_store.export_graph(include_dependencies=False, format="turtle")
        assert "sensor3" in exported
        # sensor1 should be gone after replace
        assert "sensor1" not in exported

    def test_insert_append(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)

        exported = graph_store.export_graph(include_dependencies=False, format="turtle")
        assert "sensor1" in exported
        assert "sensor3" in exported

    def test_insert_same_graph_is_idempotent(self, graph_store):
        first = graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        second = graph_store.insert_graph(SAMPLE_TURTLE, format="turtle", replace=False)

        assert first["changed"] is True
        assert second["changed"] is True

    def test_graph_status_distinguishes_source_and_published_generations(self, graph_store):
        graph_store.sparql_query("ASK { ?s ?p ?o }", wait_for_fresh=True)
        before = graph_store.graph_status()

        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        after = graph_store.graph_status()

        assert before["is_current"] is True
        assert after["source_version"] == before["source_version"] + 1
        assert after["published_version"] == before["published_version"]
        assert after["is_current"] is False

    def test_write_batch_schedules_one_rebuild_after_its_outermost_scope(self, graph_store):
        with patch.object(graph_store, "_start_query_rebuild_locked") as start_rebuild:
            with graph_store.write_batch():
                graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
                graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)
                start_rebuild.assert_not_called()

            start_rebuild.assert_called_once()

    def test_insert_malformed_raises(self, graph_store):
        with pytest.raises(Exception):
            graph_store.insert_graph("this is not valid turtle {{{}}", format="turtle")

    def test_export_turtle_parseable(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        exported = graph_store.export_graph(include_dependencies=False, format="turtle")
        assert isinstance(exported, str)
        assert len(exported) > 0

    def test_export_n3(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        exported = graph_store.export_graph(include_dependencies=False, format="n3")
        assert isinstance(exported, str)
        assert len(exported) > 0

    def test_export_without_dependencies_includes_every_source_graph(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        graph_store.insert_graph(
            EXTRA_TURTLE,
            format="turtle",
            graph_uri=graph_store.source_graph_uri("test-driver"),
        )

        exported = graph_store.export_graph(include_dependencies=False, format="turtle")

        assert "sensor1" in exported
        assert "sensor3" in exported


# ── SPARQL Tests ───────────────────────────────────────────


class TestSparql:
    def test_sparql_query_serialized_supports_results_and_graph_forms(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")

        select, select_type = graph_store.sparql_query_serialized(
            "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }",
            results_format=ox.QueryResultsFormat.JSON,
        )
        construct, construct_type = graph_store.sparql_query_serialized(
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 1",
            graph_format=ox.RdfFormat.N_TRIPLES,
        )

        assert select_type == "application/sparql-results+json"
        assert json.loads(select)["head"]["vars"] == ["s"]
        assert construct_type == "application/n-triples"
        assert construct

    def test_concurrent_fresh_readers_share_one_rebuild(self, graph_store, monkeypatch):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        query = "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }"
        graph_store.sparql_query(query, include_dependencies=False)  # Warm the cache.
        graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)

        original = graph_store._build_query_views
        started = Event()
        release = Event()
        calls = 0
        calls_lock = Lock()

        def counted_build(data, shapes):
            nonlocal calls
            with calls_lock:
                calls += 1
            started.set()
            assert release.wait(timeout=10)
            return original(data, shapes)

        monkeypatch.setattr(graph_store, "_build_query_views", counted_build)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(graph_store.sparql_query, query, False) for _ in range(4)]
            assert started.wait(timeout=10)
            release.set()
            results = [future.result(timeout=30) for future in futures]

        assert calls == 1
        assert all(len(result["rows"]) == 1 for result in results)

    def test_default_query_uses_last_complete_cache_while_rebuild_runs(
        self, graph_store, monkeypatch,
    ):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        query = "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }"
        graph_store.sparql_query(query, include_dependencies=False, wait_for_fresh=True)

        original = graph_store._build_query_views
        started = Event()
        release = Event()

        def blocked_build(data, shapes):
            started.set()
            assert release.wait(timeout=10)
            return original(data, shapes)

        monkeypatch.setattr(graph_store, "_build_query_views", blocked_build)
        graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)
        assert started.wait(timeout=10)

        # Eventual reads use the old complete cache instead of waiting here.
        assert len(graph_store.sparql_query(query, include_dependencies=False)["rows"]) == 1
        with ThreadPoolExecutor(max_workers=1) as executor:
            fresh = executor.submit(
                graph_store.sparql_query,
                query,
                False,
                wait_for_fresh=True,
            )
            assert not fresh.done()
            release.set()
            assert len(fresh.result(timeout=30)["rows"]) == 1

    def test_write_during_rebuild_is_coalesced_to_one_follow_up(self, graph_store, monkeypatch):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        query = "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }"
        graph_store.sparql_query(query, include_dependencies=False)  # Warm the cache.
        graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)

        original = graph_store._build_query_views
        first_build_started = Event()
        release_first_build = Event()
        calls = 0

        def counted_build(data, shapes):
            nonlocal calls
            calls += 1
            if calls == 1:
                first_build_started.set()
                assert release_first_build.wait(timeout=10)
            return original(data, shapes)

        monkeypatch.setattr(graph_store, "_build_query_views", counted_build)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                graph_store.sparql_query,
                query,
                False,
                wait_for_fresh=True,
            )
            assert first_build_started.wait(timeout=10)
            graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)
            release_first_build.set()
            assert len(future.result(timeout=30)["rows"]) == 1

        assert calls == 2

    def test_select_can_serialize_without_rdflib_result_conversion(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")

        payload = graph_store.sparql_query_json(
            "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }",
            include_dependencies=False,
        )

        assert payload is not None
        assert json.loads(payload)["results"]["bindings"] == [{
            "s": {"type": "uri", "value": "http://example.org/sensor1"},
        }]
    def test_concurrent_cached_queries_are_safe(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        query = "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }"

        # Build the inferred cache before starting concurrent readers.
        assert len(graph_store.sparql_query(query, include_dependencies=False)["rows"]) == 1
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(
                lambda _: graph_store.sparql_query(query, include_dependencies=False),
                range(32),
            ))

        assert all(len(result["rows"]) == 1 for result in results)

    def test_shacl_rules_infer_over_all_registered_data_graphs(self, graph_store):
        shapes = Graph()
        shapes.parse(
            data="""\
@prefix ex: <urn:test:> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

<urn:test:rules> a owl:Ontology .
ex:ThingShape a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:derived ;
        sh:object ex:value
    ] ;
    sh:property [ sh:path ex:derived ; sh:minCount 1 ] .
""",
            format="turtle",
        )
        graph_store.env.add(shapes, fetch_imports=False)
        graph_store.insert_graph(
            """\
@prefix ex: <urn:test:> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .

<urn:test:model> owl:imports <urn:test:rules> .
ex:item a ex:Thing .
""",
            format="turtle",
            # Imports are allowed in any registered deployment-data graph,
            # not just the legacy plant graph.
            graph_uri=graph_store.source_graph_uri("test-driver"),
        )

        result = graph_store.sparql_query(
            "SELECT ?o WHERE { <urn:test:item> <urn:test:derived> ?o }",
            include_dependencies=False,
        )

        assert result["rows"] == [[URIRef("urn:test:value")]]
        assert graph_store.validate()["conforms"] is True

    def test_select_basic(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.sparql_query(
            "SELECT ?s ?label WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }"
        )
        assert "s" in result["columns"]
        assert "label" in result["columns"]
        assert len(result["rows"]) == 2

    def test_select_empty(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/NonExistent> }"
        )
        assert result["rows"] == []

    def test_select_union_graph(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }",
            include_dependencies=True,
        )
        assert len(result["rows"]) >= 1

    def test_union_graph_excludes_unimported_named_graphs(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE + IMPORTS_WATER_TURTLE, format="turtle")
        other = graph_store.source_dataset.graph(URIRef("urn:test:unimported"))
        other.add((
            URIRef("urn:test:stray"),
            RDF.type,
            URIRef("http://example.org/StrayType"),
        ))

        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/StrayType> }",
            include_dependencies=True,
        )

        assert result["rows"] == []
        assert len(graph_store.query_dataset.graph(graph_store.dependency_query_graph_uri)) > 0
        assert len(graph_store.source_dataset.graph(graph_store.dependency_query_graph_uri)) == 0

    def test_dependency_view_uses_the_native_query_graph_union(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE + IMPORTS_WATER_TURTLE, format="turtle")
        query = "SELECT ?ontology WHERE { ?ontology a <http://www.w3.org/2002/07/owl#Ontology> }"

        with_dependencies = graph_store.sparql_query(query, include_dependencies=True)
        without_dependencies = graph_store.sparql_query(query, include_dependencies=False)

        assert with_dependencies["rows"]
        assert without_dependencies["rows"] == []

    def test_ordinary_write_does_not_republish_dependencies(self, graph_store):
        graph_store.sparql_query("ASK { ?s ?p ?o }", wait_for_fresh=True)
        with patch.object(graph_store, "_replace_query_graph", wraps=graph_store._replace_query_graph) as replace:
            graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
            graph_store.sparql_query("ASK { ?s ?p ?o }", wait_for_fresh=True)

        assert [call.kwargs["label"] for call in replace.call_args_list] == ["inferred"]

    def test_union_graph_includes_registered_acquirium_data_graph(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        generated = Graph()
        generated.add((
            URIRef("urn:test:generated"),
            RDF.type,
            URIRef("http://example.org/GeneratedType"),
        ))
        graph_store.insert_graph(
            generated,
            graph_uri=URIRef(ACQUIRIUM_GRAPH_URI),
            replace=False,
        )

        union = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/GeneratedType> }",
            include_dependencies=True,
        )
        inferred_data = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/GeneratedType> }",
            include_dependencies=False,
        )

        assert len(union["rows"]) == 1
        assert len(inferred_data["rows"]) == 1

    def test_source_graph_is_part_of_data_union_after_registration(self, graph_store):
        source_graph = graph_store.source_graph_uri("test-driver")
        contributed = Graph()
        contributed.add((
            URIRef("urn:test:driver"),
            RDF.type,
            URIRef("http://example.org/DriverType"),
        ))
        graph_store.insert_graph(contributed, graph_uri=source_graph, replace=False)

        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/DriverType> }",
            include_dependencies=True,
        )

        assert len(result["rows"]) == 1

    def test_update(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        graph_store.sparql_update(
            "INSERT DATA { <http://example.org/sensor99> a <http://example.org/NewSensor> }"
        )
        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/NewSensor> }"
        )
        assert len(result["rows"]) == 1

    def test_malformed_raises(self, graph_store):
        with pytest.raises(Exception):
            graph_store.sparql_query("SELEKT * WERE { ?s ?p ?o }")

    def test_filter(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.sparql_query(
            'SELECT ?s WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label . FILTER(str(?label) = "Sensor 1") }'
        )
        assert len(result["rows"]) == 1


# ── Lifecycle Tests ────────────────────────────────────────


class TestLifecycle:
    def test_close_and_reopen(self, tmp_path):
        store_path = tmp_path / "oxigraph_persist"
        env_root = tmp_path / "ontoenv_persist"
        store_path.mkdir()
        env_root.mkdir()

        gs = OxigraphGraphStore(
            store_path=store_path, env_root=env_root,
        )
        gs.insert_graph(SAMPLE_TURTLE, format="turtle")
        gs.close()

        gs2 = OxigraphGraphStore(
            store_path=store_path, env_root=env_root,
        )
        result = gs2.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/TemperatureSensor> }"
        )
        gs2.close()
        assert len(result["rows"]) >= 1

    def test_refresh_union(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        counts = graph_store.refresh_union()
        assert counts["main_triples"] > 0
        assert counts["union_triples"] >= counts["main_triples"]
