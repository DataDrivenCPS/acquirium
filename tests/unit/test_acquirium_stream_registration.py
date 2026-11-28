from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from acquirium.Client.acquirium import Acquirium
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import compute_stream_uri
from acquirium.Storage.graph_registry import METADATA_SOURCE_ID
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    HAS_MEDIUM,
    HAS_UNIT,
    IS_DISCONNECTED,
    STORED_AT,
    VIRTUAL_POINT,
)

_TEST_PROP = URIRef("urn:test:prop:fileLocation")


def _client() -> AcquiriumClient:
    client = AcquiriumClient()
    client.insert_graph = MagicMock()
    client._property_metadata = MagicMock(return_value={})
    client._units_compatible = MagicMock(return_value=False)
    client._stream_properties = MagicMock(return_value={})
    client._node_labels = MagicMock(return_value={})
    client.sparql_update = MagicMock()
    return client


def _point(stream_name: str, source_id: str = "demo-source") -> str:
    return f"{compute_stream_uri(source_id, stream_name)}__point"


def test_register_streams_inserts_one_graph_for_multiple_streams():
    client = _client()

    client.register_streams(
        [
            {
                "property_uri": "urn:test:point:temp",
                "source_id": "demo-source",
                "stream_name": "temp",
                "value_kind": "numeric",
                "data_source": "CSV",
                "properties": {_TEST_PROP: Literal("demo.csv")},
            },
            {
                "property_uri": "urn:test:point:rh",
                "source_id": "demo-source",
                "stream_name": "rh",
                "value_kind": "numeric",
                "data_source": "CSV",
                "properties": {_TEST_PROP: Literal("demo.csv")},
            },
        ]
    )

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")

    for stream_name in ("temp", "rh"):
        property_uri = URIRef(f"urn:test:point:{stream_name}")
        stream_uri = compute_stream_uri("demo-source", stream_name)
        assert (property_uri, RDF.type, VIRTUAL_POINT) in g
        assert (property_uri, HAS_EXTERNAL_REFERENCE, stream_uri) in g
        assert (property_uri, DATA_SOURCE, Literal("CSV")) in g
        assert (stream_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
        assert (stream_uri, ACQUIRIUM_REF_NAME, Literal(stream_name)) in g
        assert (stream_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
        assert (stream_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
        assert (stream_uri, _TEST_PROP, Literal("demo.csv")) in g


def test_register_stream_without_point_uri_mints_dummy_point():
    client = _client()

    client.register_streams([{"source_id": "demo-source", "stream_name": "cpu_percent", "value_kind": "numeric"}])

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")
    stream_uri = compute_stream_uri("demo-source", "cpu_percent")

    assert (stream_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
    assert (stream_uri, ACQUIRIUM_REF_NAME, Literal("cpu_percent")) in g
    assert (stream_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    assert (stream_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
    points = list(g.subjects(HAS_EXTERNAL_REFERENCE, stream_uri))
    assert len(points) == 1
    assert (points[0], RDF.type, VIRTUAL_POINT) in g
    assert (points[0], RDFS.label, Literal("cpu_percent")) in g
    assert (points[0], IS_DISCONNECTED, Literal(True)) in g


def test_register_streams_without_point_uri_mint_one_dummy_point_each():
    client = _client()

    client.register_streams([
        {"source_id": "demo-source", "stream_name": "temp", "value_kind": "numeric"},
        {"source_id": "demo-source", "stream_name": "rh", "value_kind": "numeric"},
    ])

    client.insert_graph.assert_called_once()
    graph_text = client.insert_graph.call_args[0][0]
    g = Graph().parse(data=graph_text, format="turtle")

    for stream_name in ("temp", "rh"):
        stream_uri = compute_stream_uri("demo-source", stream_name)
        assert (stream_uri, ACQUIRIUM_SOURCE_ID, Literal("demo-source")) in g
        assert (stream_uri, ACQUIRIUM_REF_NAME, Literal(stream_name)) in g
        assert (stream_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
        assert (stream_uri, STORED_AT, ACQUIRIUM_DB_URI) in g
        assert len(list(g.subjects(HAS_EXTERNAL_REFERENCE, stream_uri))) == 1

    assert len(list(g.subjects(RDF.type, VIRTUAL_POINT))) == 2


def _registered_labels(client: AcquiriumClient) -> list[str]:
    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    return sorted(str(label) for subject, label in g.subject_objects(RDFS.label) if subject != ACQUIRIUM_DB_URI)


def test_register_again_with_a_label_replaces_the_old_label():
    client = _client()
    client._node_labels = MagicMock(return_value={_point("temp"): ["temp"]})

    client.register_streams([{"source_id": "demo-source", "stream_name": "temp", "label": "Temperature"}])

    point = f"{compute_stream_uri('demo-source', 'temp')}__point"
    own, metadata = client.sparql_update.call_args_list
    assert own[0][0] == f"DELETE WHERE {{ <{point}> <{RDFS.label}> ?label }}"
    assert own[1] == {"source_id": "demo-source"}
    # the label insert_metadata may have written on the generated property goes too
    assert f"?p = <{RDFS.label}>" in metadata[0][0] and f"<{point}>" in metadata[0][0]
    assert metadata[1] == {"source_id": "metadata"}
    assert _registered_labels(client) == ["Temperature"]


def test_register_again_without_a_label_keeps_the_given_label():
    client = _client()
    client._node_labels = MagicMock(return_value={_point("temp"): ["Temperature"]})

    client.register_streams([{"source_id": "demo-source", "stream_name": "temp"}])

    client.sparql_update.assert_not_called()
    assert _registered_labels(client) == ["Temperature"]


def test_register_again_with_the_same_label_deletes_nothing():
    client = _client()
    client._node_labels = MagicMock(return_value={_point("temp"): ["Temperature"]})

    client.register_streams([{"source_id": "demo-source", "stream_name": "temp", "label": "Temperature"}])

    client.sparql_update.assert_not_called()
    assert _registered_labels(client) == ["Temperature"]


def test_register_stream_without_point_uri_and_stream_name_raises():
    client = _client()

    with pytest.raises(ValueError, match="property_uri or a"):
        client.register_streams([{"source_id": "demo-source"}])
    client.insert_graph.assert_not_called()


_MG_L = "http://qudt.org/vocab/unit/MilliGM-PER-L"
_G_L = "http://qudt.org/vocab/unit/GM-PER-L"
_DEG_C = "http://qudt.org/vocab/unit/DEG_C"
_WATER = "https://watermetadata.org/ontology/watr#Water"
_MASS_CONC = "http://qudt.org/vocab/quantitykind/MassConcentration"
_DENSITY = "http://qudt.org/vocab/quantitykind/Density"


def test_register_streams_conflicting_metadata_fails_before_insert():
    client = _client()
    client._property_metadata = MagicMock(return_value={"quantity_kind": _MASS_CONC})

    with pytest.raises(ValueError, match="quantity_kind mismatch"):
        client.register_streams([{
            "property_uri": "urn:test:point:conc",
            "source_id": "demo-source",
            "stream_name": "conc",
            "quantity_kind": _DENSITY,
        }])
    client.insert_graph.assert_not_called()


def test_register_streams_unconvertible_unit_fails():
    client = _client()
    client._property_metadata = MagicMock(return_value={"unit": _DEG_C})

    with pytest.raises(ValueError, match="not convertible"):
        client.register_streams([{
            "property_uri": "urn:test:point:conc",
            "source_id": "demo-source",
            "stream_name": "conc",
            "unit": _MG_L,
        }])
    client._units_compatible.assert_called_once_with(URIRef(_MG_L), _DEG_C)
    client.insert_graph.assert_not_called()


def test_register_streams_convertible_unit_becomes_storage_unit():
    client = _client()
    client._property_metadata = MagicMock(return_value={"unit": _G_L})
    client._units_compatible = MagicMock(return_value=True)

    client.register_streams([{
        "property_uri": "urn:test:point:conc",
        "source_id": "demo-source",
        "stream_name": "conc",
        "unit": _MG_L,
    }])

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    stream_uri = compute_stream_uri("demo-source", "conc")
    point = URIRef("urn:test:point:conc")
    assert (stream_uri, HAS_UNIT, URIRef(_MG_L)) in g
    # the point keeps its own unit; reads convert ref -> point automatically
    assert list(g.objects(point, HAS_UNIT)) == []


def test_register_streams_matching_unit_lands_on_reference():
    client = _client()
    client._property_metadata = MagicMock(return_value={"unit": _MG_L})

    client.register_streams([{
        "property_uri": "urn:test:point:conc",
        "source_id": "demo-source",
        "stream_name": "conc",
        "unit": _MG_L,
        "medium": _WATER,  # the point lacks it -> added to the point
    }])

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    stream_uri = compute_stream_uri("demo-source", "conc")
    point = URIRef("urn:test:point:conc")
    assert (stream_uri, HAS_UNIT, URIRef(_MG_L)) in g
    assert (point, HAS_UNIT, URIRef(_MG_L)) not in g
    assert (point, HAS_MEDIUM, URIRef(_WATER)) in g


def test_insert_timeseries_batch_delegates_chunking_to_receipt_aware_client():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.insert_batch_rows = 3
    aq.client.insert_timeseries_batch.return_value = {"ok": True, "rows_inserted": 4, "receipts": [{}, {}]}
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    streams = {"temp": [(ts, 1.0), (ts, 2.0)], "rh": [(ts, 3.0), (ts, 4.0)]}

    reply = aq.insert_timeseries_batch("source-a", streams, request_id="readings")

    aq.client.insert_timeseries_batch.assert_called_once()
    call = aq.client.insert_timeseries_batch.call_args
    assert call.args == ("source-a", streams)
    assert call.kwargs["request_id"] == "readings"
    assert call.kwargs["batch_rows"] == 3
    assert reply == {"ok": True, "rows_inserted": 4, "batches": 2, "receipts": [{}, {}]}


_BAR = "http://qudt.org/vocab/unit/BAR"
_PA = "http://qudt.org/vocab/unit/PA"
_PLANT = "urn:test:plant:pressure"


def _connecting_client(generated: dict, plant: dict, documents: dict | None = None) -> AcquiriumClient:
    """A client whose graph holds the stream ``pt`` on its generated property."""
    client = _client()
    stream_uri = str(compute_stream_uri("demo-source", "pt"))
    client._stream_properties = MagicMock(return_value={stream_uri: [_point("pt")]})
    client._node_labels = MagicMock(return_value={_point("pt"): ["pt"]})
    client._property_metadata = MagicMock(
        side_effect=lambda uri: generated if uri == _point("pt") else plant)
    client._units_compatible = MagicMock(return_value=True)
    client.metadata_documents = MagicMock(
        return_value=documents or {_point("pt"): {}, _PLANT: {}})
    client._plan_metadata = MagicMock(return_value=[])
    client._write_metadata = MagicMock()
    return client


def _connect(client: AcquiriumClient, **fields) -> None:
    client.register_streams([{
        "source_id": "demo-source", "stream_name": "pt", "property_uri": _PLANT, **fields}])


def test_register_stream_without_property_is_not_marked_once_given_one():
    client = _client()

    client.register_streams([{
        "source_id": "demo-source", "stream_name": "pt", "property_uri": _PLANT}])

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    assert list(g.triples((None, IS_DISCONNECTED, None))) == []


def test_connecting_a_stream_replaces_its_generated_property():
    client = _connecting_client({"unit": _BAR}, {"unit": _PA})

    _connect(client)

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    stream_uri = compute_stream_uri("demo-source", "pt")
    assert (URIRef(_PLANT), HAS_EXTERNAL_REFERENCE, stream_uri) in g
    assert list(g.triples((URIRef(_point("pt")), None, None))) == []
    # a convertible unit stays with the stream, as the unit its rows are stored in
    assert (stream_uri, HAS_UNIT, URIRef(_BAR)) in g
    assert list(g.objects(URIRef(_PLANT), HAS_UNIT)) == []
    removal = f"DELETE WHERE {{ <{_point('pt')}> ?p ?o }}"
    assert [(call.args[0], call.kwargs) for call in client.sparql_update.call_args_list] == [
        (removal, {"source_id": "demo-source"}),
        (removal, {"source_id": METADATA_SOURCE_ID}),
    ]


def test_connecting_a_stream_moves_what_the_property_lacks():
    client = _connecting_client(
        {"unit": _MG_L, "medium": _WATER, "data_source": "Lab"}, {},
        {_point("pt"): {"sampled_by": "night shift", "info": {"year": 2019}}, _PLANT: {}})

    _connect(client)

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    plant = URIRef(_PLANT)
    assert (plant, HAS_UNIT, URIRef(_MG_L)) in g
    assert (plant, HAS_MEDIUM, URIRef(_WATER)) in g
    assert (plant, DATA_SOURCE, Literal("Lab")) in g
    assert client._plan_metadata.call_args[0][0] == {
        _PLANT: {"sampled_by": "night shift", "info": {"year": 2019}}}


def test_connecting_a_stream_with_an_unconvertible_unit_fails():
    client = _connecting_client({"unit": _DEG_C}, {"unit": _PA})
    client._units_compatible = MagicMock(return_value=False)

    with pytest.raises(ValueError, match="not convertible.*pass the key in this call"):
        _connect(client)
    client.insert_graph.assert_not_called()
    client.sparql_update.assert_not_called()


def test_connecting_a_stream_with_a_conflicting_field_fails():
    client = _connecting_client({"quantity_kind": _DENSITY}, {"quantity_kind": _MASS_CONC})

    with pytest.raises(ValueError, match="quantity_kind mismatch"):
        _connect(client)
    client.insert_graph.assert_not_called()
    client.sparql_update.assert_not_called()


def test_connecting_a_stream_keeps_the_property_value_of_a_conflicting_key():
    client = _connecting_client({}, {}, {
        _point("pt"): {"sampled_by": "night shift", "site": "north"},
        _PLANT: {"sampled_by": "day shift"}})

    with pytest.warns(UserWarning, match="'sampled_by' of stream 'demo-source'/'pt' is not moved"):
        _connect(client)

    assert client._plan_metadata.call_args[0][0] == {_PLANT: {"site": "north"}}
    client.insert_graph.assert_called_once()


def test_connecting_a_stream_keeps_the_property_substance_and_data_source():
    chlorine, ammonia = "urn:test:substance:chlorine", "urn:test:substance:ammonia"
    client = _connecting_client(
        {"substance": chlorine, "data_source": "Lab"},
        {"substance": ammonia, "data_source": "SCADA"})

    with pytest.warns(UserWarning) as caught:
        _connect(client)

    assert sorted(str(w.message).split(" of stream")[0] for w in caught) == ["'data_source'", "'substance'"]
    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    assert list(g.objects(URIRef(_PLANT), DATA_SOURCE)) == []
    assert not any(str(o) == chlorine for o in g.objects(URIRef(_PLANT), None))


def test_connecting_a_stream_with_a_conflicting_medium_fails():
    client = _connecting_client({"medium": _WATER}, {"medium": "urn:test:medium:air"})

    with pytest.raises(ValueError, match="medium mismatch"):
        _connect(client)
    client.insert_graph.assert_not_called()
    client.sparql_update.assert_not_called()


def test_a_value_given_while_connecting_settles_a_conflict():
    client = _connecting_client(
        {"quantity_kind": _DENSITY}, {"quantity_kind": _MASS_CONC},
        {_point("pt"): {"sampled_by": "night shift"}, _PLANT: {"sampled_by": "day shift"}})

    _connect(client, quantity_kind=URIRef(_MASS_CONC), metadata={"sampled_by": "both shifts"})

    client.insert_graph.assert_called_once()
    assert client._plan_metadata.call_args[0][0] == {_PLANT: {"sampled_by": "both shifts"}}


def test_a_given_label_moves_when_the_property_has_none():
    client = _connecting_client({}, {})
    client._node_labels = MagicMock(return_value={_point("pt"): ["Feed pressure"]})

    _connect(client)

    assert _registered_labels(client) == ["Feed pressure"]


def test_the_stream_name_label_does_not_move():
    client = _connecting_client({}, {})

    _connect(client)

    assert _registered_labels(client) == []


def test_registering_a_connected_stream_again_describes_its_property():
    client = _client()
    stream_uri = str(compute_stream_uri("demo-source", "pt"))
    client._stream_properties = MagicMock(return_value={stream_uri: [_PLANT]})

    client.register_streams([{"source_id": "demo-source", "stream_name": "pt", "medium": _WATER}])

    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    assert (URIRef(_PLANT), HAS_MEDIUM, URIRef(_WATER)) in g
    assert list(g.triples((URIRef(_point("pt")), None, None))) == []
    client.sparql_update.assert_not_called()


def test_registering_a_stream_on_several_properties_again_needs_a_property_uri():
    client = _client()
    stream_uri = str(compute_stream_uri("demo-source", "pt"))
    client._stream_properties = MagicMock(return_value={stream_uri: [_PLANT, "urn:test:plant:other"]})

    with pytest.raises(ValueError, match="connected to several properties"):
        client.register_streams([{"source_id": "demo-source", "stream_name": "pt"}])
    client.insert_graph.assert_not_called()


def test_a_property_uri_given_as_a_curie_is_expanded():
    client = _client()
    client._expand_curie = MagicMock(return_value="urn:test:plant:pressure")

    client.register_streams([{
        "source_id": "demo-source", "stream_name": "pt", "property_uri": "plant:pressure"}])

    client._expand_curie.assert_called_once_with("plant:pressure")
    g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
    assert (URIRef(_PLANT), HAS_EXTERNAL_REFERENCE, compute_stream_uri("demo-source", "pt")) in g


def test_register_again_replaces_the_unit_of_a_generated_property():
    """A stream registered on its own holds one unit: the new one replaces
    the old one in its own graph and in the metadata graph."""
    client = _client()
    point = _point("temp")
    stream_uri = str(compute_stream_uri("demo-source", "temp"))
    client._stream_properties = MagicMock(return_value={stream_uri: [point]})
    client._node_labels = MagicMock(return_value={point: ["temp"]})
    client._builtin_values = MagicMock(return_value={"unit": {_MG_L}})

    client.register_streams([{"source_id": "demo-source", "stream_name": "temp", "unit": _DEG_C}])

    own, metadata = client.sparql_update.call_args_list
    assert own[1] == {"source_id": "demo-source"} and metadata[1] == {"source_id": "metadata"}
    for update in (own[0][0], metadata[0][0]):
        assert f"DELETE {{ <{point}> ?p ?o }}" in update
        assert "?p = <http://qudt.org/schema/qudt/hasUnit>" in update
    client._units_compatible.assert_not_called()


def test_register_again_with_the_same_unit_removes_nothing():
    client = _client()
    point = _point("temp")
    stream_uri = str(compute_stream_uri("demo-source", "temp"))
    client._stream_properties = MagicMock(return_value={stream_uri: [point]})
    client._node_labels = MagicMock(return_value={point: ["temp"]})
    client._builtin_values = MagicMock(return_value={"unit": {_MG_L}})

    client.register_streams([{"source_id": "demo-source", "stream_name": "temp", "unit": _MG_L}])

    client.sparql_update.assert_not_called()


def test_metadata_on_a_stream_is_written_on_its_property():
    from acquirium.Client.streams import Stream

    client = _client()
    stream_uri = str(compute_stream_uri("demo-source", "temp"))
    client._stream_properties = MagicMock(return_value={stream_uri: [_point("temp")]})
    client.insert_metadata = MagicMock(return_value={"ok": True})

    Stream(stream_uri, client).insert_metadata({"sampled_by": "day shift"})
    client.insert_metadata.assert_called_once_with({_point("temp"): {"sampled_by": "day shift"}})

    client._stream_properties = MagicMock(return_value={})
    with pytest.raises(ValueError, match="is not registered"):
        client.stream_property(stream_uri)
    client._stream_properties = MagicMock(return_value={stream_uri: ["urn:a", "urn:b"]})
    with pytest.raises(ValueError, match="several properties"):
        client.stream_property(stream_uri)
