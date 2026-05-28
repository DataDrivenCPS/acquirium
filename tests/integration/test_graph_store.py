"""Integration tests for OxigraphGraphStore — in-process Oxigraph.

No external services required (Oxigraph runs in-process via pyoxigraph).
Note: Oxigraph's file locking may abort on macOS; these tests are primarily
targeted at Linux CI runners.
"""

import platform
import pytest
from pathlib import Path

from rdflib import URIRef, Literal, RDF, RDFS

from acquirium.Storage.graph_store import OxigraphGraphStore
from acquirium.internals.internals_namespaces import (
    DEFAULT_MAIN_GRAPH,
    ACQUIRIUM_POINT_NS,
)

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
        assert result["main_triples"] > 0

        exported = graph_store.export_graph(include_union=False, format="turtle")
        assert "sensor1" in exported
        assert "sensor2" in exported

    def test_insert_replace(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        result = graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=True)

        exported = graph_store.export_graph(include_union=False, format="turtle")
        assert "sensor3" in exported
        # sensor1 should be gone after replace
        assert "sensor1" not in exported

    def test_insert_append(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        graph_store.insert_graph(EXTRA_TURTLE, format="turtle", replace=False)

        exported = graph_store.export_graph(include_union=False, format="turtle")
        assert "sensor1" in exported
        assert "sensor3" in exported

    def test_insert_same_graph_is_idempotent(self, graph_store):
        first = graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        second = graph_store.insert_graph(SAMPLE_TURTLE, format="turtle", replace=False)

        assert first["changed"] is True
        assert second["changed"] is True
        assert second["main_triples"] == first["main_triples"]
        assert second["union_triples"] == first["union_triples"]

    def test_insert_malformed_raises(self, graph_store):
        with pytest.raises(Exception):
            graph_store.insert_graph("this is not valid turtle {{{}}", format="turtle")

    def test_export_turtle_parseable(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        exported = graph_store.export_graph(include_union=False, format="turtle")
        assert isinstance(exported, str)
        assert len(exported) > 0

    def test_export_n3(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        exported = graph_store.export_graph(include_union=False, format="n3")
        assert isinstance(exported, str)
        assert len(exported) > 0


# ── SPARQL Tests ───────────────────────────────────────────


class TestSparql:
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
            use_union=True,
        )
        assert len(result["rows"]) >= 1

    def test_union_graph_excludes_unimported_named_graphs(self, graph_store):
        graph_store.insert_graph(SAMPLE_TURTLE, format="turtle")
        other = graph_store.source_dataset.graph(URIRef("urn:test:unimported"))
        other.add((
            URIRef("urn:test:stray"),
            RDF.type,
            URIRef("http://example.org/StrayType"),
        ))

        result = graph_store.sparql_query(
            "SELECT ?s WHERE { ?s a <http://example.org/StrayType> }",
            use_union=True,
        )

        assert result["rows"] == []
        assert len(graph_store.query_dataset.graph(graph_store.imports_union_graph_uri)) > 0
        assert len(graph_store.source_dataset.graph(graph_store.imports_union_graph_uri)) == 0

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
