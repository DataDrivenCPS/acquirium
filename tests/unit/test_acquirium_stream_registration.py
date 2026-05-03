from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from acquirium.Client.acquirium import Acquirium
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    FILE_LOCATION,
    HAS_EXTERNAL_REFERENCE,
    STORED_AT,
    VIRTUAL_POINT,
)


def test_register_streams_inserts_one_graph_for_multiple_streams():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()

    aq.register_streams(
        [
            {
                "point_uri": "urn:test:point:temp",
                "source_id": "demo-source",
                "ref_name": "temp",
                "data_source": "CSV",
                "properties": {FILE_LOCATION: Literal("demo.csv")},
            },
            {
                "point_uri": "urn:test:point:rh",
                "source_id": "demo-source",
                "ref_name": "rh",
                "data_source": "CSV",
                "properties": {FILE_LOCATION: Literal("demo.csv")},
            },
        ]
    )

    aq.client.insert_graph.assert_called_once()
    graph_text = aq.client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")

    for ref_name in ("temp", "rh"):
        point_uri = URIRef(f"urn:test:point:{ref_name}")
        ref_uri = compute_ref_uri("demo-source", ref_name)
        assert (point_uri, RDF.type, VIRTUAL_POINT) in g
        assert (point_uri, HAS_EXTERNAL_REFERENCE, ref_uri) in g
        assert (point_uri, DATA_SOURCE, Literal("CSV")) in g
        assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
        assert (ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)) in g
        assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
        assert (ref_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
        assert (ref_uri, FILE_LOCATION, Literal("demo.csv")) in g


def test_register_stream_without_point_uri_writes_only_ref_node():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()

    aq.register_stream(source_id="demo-source", ref_name="cpu_percent")

    aq.client.insert_graph.assert_called_once()
    graph_text = aq.client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")
    ref_uri = compute_ref_uri("demo-source", "cpu_percent")

    assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
    assert (ref_uri, ACQUIRIUM_REF_NAME, Literal("cpu_percent")) in g
    assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    assert (ref_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
    assert list(g.subjects(RDF.type, VIRTUAL_POINT)) == []
    assert list(g.subjects(HAS_EXTERNAL_REFERENCE, ref_uri)) == []


def test_register_streams_without_point_uri_writes_only_ref_nodes():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()

    aq.register_streams([
        {"source_id": "demo-source", "ref_name": "temp"},
        {"source_id": "demo-source", "ref_name": "rh"},
    ])

    aq.client.insert_graph.assert_called_once()
    graph_text = aq.client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")

    for ref_name in ("temp", "rh"):
        ref_uri = compute_ref_uri("demo-source", ref_name)
        assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
        assert (ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)) in g
        assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
        assert (ref_uri, STORED_AT, ACQUIRIUM_DB_URI) in g

    assert list(g.subjects(RDF.type, VIRTUAL_POINT)) == []
    assert list(g.subjects(HAS_EXTERNAL_REFERENCE, None)) == []


def test_insert_timeseries_batch_chunks_at_acquirium_facade():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.insert_batch_rows = 3
    aq.client.insert_timeseries_batch.side_effect = lambda source_id, streams: {
        "ok": True,
        "rows_inserted": sum(len(rows) for rows in streams.values()),
    }

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    result = aq.insert_timeseries_batch(
        "source-a",
        {
            "temp": [(ts, 1.0), (ts, 2.0)],
            "rh": [(ts, 3.0), (ts, 4.0)],
        },
    )

    assert result == {"ok": True, "rows_inserted": 4, "batches": 2}
    assert aq.client.insert_timeseries_batch.call_count == 2
    first_source, first_chunk = aq.client.insert_timeseries_batch.call_args_list[0].args
    second_source, second_chunk = aq.client.insert_timeseries_batch.call_args_list[1].args
    assert first_source == second_source == "source-a"
    assert sum(len(rows) for rows in first_chunk.values()) == 3
    assert sum(len(rows) for rows in second_chunk.values()) == 1
    assert set(first_chunk) == {"temp", "rh"}
    assert set(second_chunk) == {"rh"}
