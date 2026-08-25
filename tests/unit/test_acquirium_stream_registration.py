from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from acquirium.Client.acquirium import Acquirium
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    HAS_MEDIUM,
    HAS_UNIT,
    STORED_AT,
    VIRTUAL_POINT,
)

_TEST_PROP = URIRef("urn:test:prop:fileLocation")


def _client() -> AcquiriumClient:
    client = AcquiriumClient()
    client.insert_graph = MagicMock()
    client._point_metadata = MagicMock(return_value={})
    return client


def test_register_streams_inserts_one_graph_for_multiple_streams():
    client = _client()

    client.register_streams(
        [
            {
                "point_uri": "urn:test:point:temp",
                "source_id": "demo-source",
                "ref_name": "temp",
                "value_kind": "numeric",
                "data_source": "CSV",
                "properties": {_TEST_PROP: Literal("demo.csv")},
            },
            {
                "point_uri": "urn:test:point:rh",
                "source_id": "demo-source",
                "ref_name": "rh",
                "value_kind": "numeric",
                "data_source": "CSV",
                "properties": {_TEST_PROP: Literal("demo.csv")},
            },
        ]
    )

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
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
        assert (ref_uri, _TEST_PROP, Literal("demo.csv")) in g


def test_register_stream_without_point_uri_mints_dummy_point():
    client = _client()

    client.register_streams([{"source_id": "demo-source", "ref_name": "cpu_percent", "value_kind": "numeric"}])

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")
    ref_uri = compute_ref_uri("demo-source", "cpu_percent")

    assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
    assert (ref_uri, ACQUIRIUM_REF_NAME, Literal("cpu_percent")) in g
    assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    assert (ref_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
    points = list(g.subjects(HAS_EXTERNAL_REFERENCE, ref_uri))
    assert len(points) == 1
    assert (points[0], RDF.type, VIRTUAL_POINT) in g


def test_register_streams_without_point_uri_mint_one_dummy_point_each():
    client = _client()

    client.register_streams([
        {"source_id": "demo-source", "ref_name": "temp", "value_kind": "numeric"},
        {"source_id": "demo-source", "ref_name": "rh", "value_kind": "numeric"},
    ])

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")

    for ref_name in ("temp", "rh"):
        ref_uri = compute_ref_uri("demo-source", ref_name)
        assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
        assert (ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)) in g
        assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
        assert (ref_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
        assert len(list(g.subjects(HAS_EXTERNAL_REFERENCE, ref_uri))) == 1

    assert len(list(g.subjects(RDF.type, VIRTUAL_POINT))) == 2


def test_register_stream_without_point_uri_and_ref_name_raises():
    client = _client()

    with pytest.raises(ValueError, match="point_uri or a"):
        client.register_streams([{"source_id": "demo-source"}])
    client.insert_graph.assert_not_called()


_MG_L = "http://qudt.org/vocab/unit/MilliGM-PER-L"
_G_L = "http://qudt.org/vocab/unit/GM-PER-L"
_WATER = "urn:nawi-water-ontology#Water"


def test_register_streams_conflicting_metadata_fails_before_insert():
    client = _client()
    client._point_metadata = MagicMock(return_value={"unit": _G_L})

    with pytest.raises(ValueError, match="unit mismatch"):
        client.register_streams([{
            "point_uri": "urn:test:point:conc",
            "source_id": "demo-source",
            "ref_name": "conc",
            "unit": _MG_L,
        }])
    client.insert_graph.assert_not_called()


def test_register_streams_matching_unit_lands_on_reference():
    client = _client()
    client._point_metadata = MagicMock(return_value={"unit": _MG_L})

    client.register_streams([{
        "point_uri": "urn:test:point:conc",
        "source_id": "demo-source",
        "ref_name": "conc",
        "unit": _MG_L,
        "medium": _WATER,  # the point lacks it -> added to the point
    }])

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    ref_uri = compute_ref_uri("demo-source", "conc")
    point = URIRef("urn:test:point:conc")
    assert (ref_uri, HAS_UNIT, URIRef(_MG_L)) in g
    assert (point, HAS_UNIT, URIRef(_MG_L)) not in g
    assert (point, HAS_MEDIUM, URIRef(_WATER)) in g


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
