"""Shared lossless documents, independent of model and experiment adapters."""
from datetime import date, datetime, timezone

import pytest
from rdflib import Graph, Literal, RDF, URIRef

from acquirium.Metadata import dumps, loads, read_documents, document_predicate
from acquirium.Client.metadata import plan_write, update_text
from acquirium.Client.explore.core import Query
from acquirium.ExperimentMetadata import _GraphQueryClient
from acquirium.Storage.graph_registry import source_graph_uri
from tests.unit.test_experiment_metadata import MemoryGraphStore


PAYLOAD = {"arbitrary.key": [None, {}, [], {"日本語 key": {"": "value"}}],
           "type": "user classification", "unit": None, "role": "note", "": False}


def test_document_codec_is_lossless_and_typed():
    value = {"metadata": PAYLOAD, "date": date(2026, 1, 1),
             "time": datetime(2026, 1, 1, tzinfo=timezone.utc),
             "uri": URIRef("urn:example:node"), "number": 2**80}
    result = loads(dumps(value))
    assert result == {**value, "uri": "urn:example:node"}
    assert type(result["date"]) is date
    assert type(result["time"]) is datetime
    for invalid in ({1: "key"}, {"value": float("nan")}, {"value": object()}):
        with pytest.raises((ValueError, TypeError)):
            dumps(invalid)


def test_shared_writer_objects_preserve_arbitrary_json_and_null_slots():
    graph = MemoryGraphStore()
    uri, cls = "urn:example:document", "urn:example:Class"
    source = source_graph_uri("test")
    content = Graph()
    content.add((URIRef(uri), RDF.type, URIRef(cls)))
    values = {"metadata": PAYLOAD, "tags": ["A", None, {}, [], "B"], "created": date(2026, 1, 1)}
    write = plan_write(uri, values, role="entity", resolve=lambda *args: None)
    for triple in write.triples:
        content.add(triple)
    graph.insert_graph(content, replace=True, graph_uri=source)
    client = _GraphQueryClient(graph)
    assert read_documents(client, [uri])[uri] == values
    query = Query(client).entity(cls, alias="item")
    saved = query.include("all").metadata_objects()[0]["item"]
    assert saved["metadata"] == PAYLOAD
    assert saved["tags"] == values["tags"]
    assert saved["created"] == values["created"]
    assert query.include("metadata").metadata_objects()[0]["item"]["metadata"] == PAYLOAD
    assert "metadata" not in query.include("all").drop("metadata").metadata_objects()[0]["item"]
    # Replacing a key replaces its document and all old projected leaves.
    patch = plan_write(uri, {"metadata": {"new key": [None, {}]}, "tags": None}, role="entity", resolve=lambda *args: None)
    graph.sparql_update(update_text([patch]), graph_uri=source)
    stored = read_documents(client, [uri])[uri]
    assert stored == {"metadata": {"new key": [None, {}]}, "created": values["created"]}
    assert not list(graph.dataset.objects(URIRef(uri), document_predicate("tags")))
    assert Query(client).entity(cls, alias="item").include("all").metadata_objects()[0]["item"]["metadata"] == stored["metadata"]


def test_semantic_document_retains_order_and_spelling():
    graph = MemoryGraphStore()
    write = plan_write("urn:example:scalar", {"unit": ["psi", "PSI"]}, role="data", resolve=lambda *args: "urn:unit:PSI")
    content = Graph()
    for triple in write.triples:
        content.add(triple)
    graph.insert_graph(content, replace=True, graph_uri=source_graph_uri("test"))
    assert read_documents(_GraphQueryClient(graph), ["urn:example:scalar"])["urn:example:scalar"] == {"unit": ["psi", "PSI"]}


def test_explicit_metadata_edits_take_precedence_over_source_documents():
    from acquirium.Storage.graph_registry import METADATA_SOURCE_ID
    graph = MemoryGraphStore()
    uri = "urn:example:node"
    for source, value in (("driver", "original"), (METADATA_SOURCE_ID, "edited")):
        write = plan_write(uri, {"metadata": {"value": value}}, role="entity", resolve=lambda *args: None)
        graph.sparql_update(update_text([write]), graph_uri=source_graph_uri(source))
    client = _GraphQueryClient(graph)
    assert read_documents(client, [uri])[uri] == {"metadata": {"value": "edited"}}
    from acquirium.Metadata import matching_documents
    assert matching_documents(client, [uri], {"metadata": {"value": "original"}}) == set()
    assert matching_documents(client, [uri], {"metadata": {"value": "edited"}}) == {uri}


def test_http_documents_preserve_types_and_read_asserted_source(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from importlib import import_module
    from fastapi.testclient import TestClient
    from acquirium.Client.client import AcquiriumClient
    from acquirium.Experiments import ExperimentStore
    from acquirium.Storage.duckdb_store import DuckDBStore
    graph = MemoryGraphStore()
    store = DuckDBStore(tmp_path / "documents.duckdb")
    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph)
    uri = "urn:example:document"
    values = {"metadata": PAYLOAD, "date": date(2026, 1, 1)}
    write = plan_write(uri, values, role="entity", resolve=lambda *args: None)
    graph.sparql_update(update_text([write]), graph_uri=source_graph_uri("test"))
    module = import_module("acquirium.Server.app")
    monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger, graph_store=graph), raising=False)
    client = AcquiriumClient()
    client._http.close()
    client._http = TestClient(module.app)
    try:
        assert client.metadata_documents([uri, "urn:missing"]) == {uri: values, "urn:missing": {}}
    finally:
        client._http.close()
        store.close()


def test_source_document_reader_does_not_consult_inferred_graphs():
    from unittest.mock import Mock
    import threading
    from rdflib import Dataset
    from oxrdflib.store import OxigraphStore
    import pyoxigraph
    from acquirium.Storage.graph_store import OxigraphGraphStore
    graph = object.__new__(OxigraphGraphStore)
    graph._lock = threading.RLock()
    graph.source_dataset = Dataset(store=OxigraphStore(store=pyoxigraph.Store()))
    graph.query_dataset = Mock(side_effect=AssertionError("inference must not be consulted"))
    uri = "urn:example:asserted"
    values = {"metadata": PAYLOAD, "date": date(2026, 1, 1)}
    write = plan_write(uri, values, role="entity", resolve=lambda *args: None)
    for triple in write.triples:
        graph.source_dataset.graph(source_graph_uri("test")).add(triple)
    assert graph.metadata_documents([uri]) == {uri: values}


def test_metadata_edits_wait_for_pending_initialization_acknowledgment():
    from unittest.mock import Mock
    from acquirium.Server.manager import Manager
    manager = object.__new__(Manager)
    manager.graph_store = Mock()
    manager.experiments = Mock()
    manager.experiments.sync_metadata_graph.side_effect = OSError("ack failed")
    with pytest.raises(OSError, match="ack failed"):
        manager.sparql_update("INSERT DATA {}", source_id="metadata")
    manager.graph_store.sparql_update.assert_not_called()
