"""``Query.streams()`` — matching external references directly.

``measurement()`` matches a point that *has* a reference, so it cannot see a
stream no point links to — which is every stream when data is ingested
without a model. ``streams()`` binds the reference itself.

Graph-executing tests use rdflib and mirror pyoxigraph's positional row
serialisation (``None`` in place for an unbound variable), because the
optional point column depends on that.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from acquirium.Client.explore.core import Query
from acquirium.Client.query_graph import QueryGraph, StreamNodeInfo
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    HAS_UNIT,
    S223,
)

MGL = URIRef("http://qudt.org/vocab/unit/MilliGM-PER-L")
DEG_C = URIRef("http://qudt.org/vocab/unit/DEG_C")
BASIN, EQ = URIRef("urn:t#Basin"), URIRef("urn:t#b1")
POINT = URIRef("urn:t#p_do")


@pytest.fixture
def graph() -> Graph:
    """Two references on one point, plus one that no point links to."""
    g = Graph()
    g.add((EQ, RDF.type, BASIN))
    g.add((EQ, S223.hasProperty, POINT))

    def ref(name, source, ref_name, unit, data_source, point=None, value_kind="numeric"):
        r = URIRef(name)
        g.add((r, ACQUIRIUM_SOURCE_ID, Literal(source)))
        g.add((r, ACQUIRIUM_REF_NAME, Literal(ref_name)))
        g.add((r, ACQUIRIUM_VALUE_KIND, Literal(value_kind)))
        g.add((r, HAS_UNIT, unit))
        g.add((r, DATA_SOURCE, Literal(data_source)))
        if point is not None:
            g.add((point, HAS_EXTERNAL_REFERENCE, r))
        return r

    ref("urn:t#r_scada", "svcw-scada", "DOX_1", MGL, "SCADA", point=POINT)
    ref("urn:t#r_lab", "svcw-lab", "DO_lab", MGL, "Lab", point=POINT)
    ref("urn:t#r_orphan", "svcw-scada", "FIT_9", DEG_C, "SCADA")
    return g


def sparql_client(graph: Graph) -> MagicMock:
    client = MagicMock()

    def run(query, **kwargs):
        result = graph.query(query)
        return {
            "columns": [str(v) for v in result.vars],
            "rows": [
                [None if row[v] is None else str(row[v]) for v in result.vars]
                for row in result
            ],
        }

    client.sparql_query.side_effect = run
    client.timeseries_info_batch.return_value = {}
    client.compact_uri.side_effect = lambda u: str(u).split("#")[-1]
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    client.timeseries_df.side_effect = lambda ref_uri, **kwargs: pl.DataFrame({
        "ts": pl.Series("ts", [stamp], dtype=pl.Datetime(time_zone="UTC")),
        "value": [float(len(ref_uri))],
        "uri": [ref_uri],
    })
    return client


def matched(graph: Graph, query: Query) -> list[str]:
    return sorted(str(row[0]).split("#")[-1] for row in graph.query(query.to_sparql()))


# ------------------------------------------------------------ graph shape

def test_root_form_registers_a_stream_node():
    g = Query(client=None).streams().query_graph
    assert list(g.stream_nodes) == [0]
    assert g.stream_nodes[0].source_id is None
    assert g.aliases == {"streams": 0}


def test_stream_node_role_is_stream():
    g = Query(client=None).streams().query_graph
    assert g.node_role(0) == "stream"


def test_chained_form_records_its_source():
    g = Query(client=None).entity("urn:t#A").measurement(alias="m").streams().query_graph
    assert g.stream_nodes[2].source_id == 1
    assert g.aliases_reverse[2] == "m_streams"


def test_explicit_alias_is_honoured():
    g = Query(client=None).streams(alias="tags").query_graph
    assert g.aliases["tags"] == 0


def test_filters_land_on_the_stream_node_not_the_query_node():
    g = Query(client=None).streams(data_source="SCADA").query_graph
    assert g.stream_nodes[0].filters == {"data_source": "SCADA"}
    assert "attrs" not in g.nodes[0].constraints


def test_immutability():
    base = Query(client=None)
    base.streams()
    assert base.query_graph.stream_nodes == {}


# ---------------------------------------------------------------- guards

def test_a_second_stream_node_is_refused():
    with pytest.raises(ValueError, match="only one stream node"):
        Query(client=None).streams().streams()


def test_refuses_to_narrow_several_measurements_at_once():
    """They compile as separate union branches; one stream node cannot
    belong to both."""
    q = (Query(client=None).entity("urn:t#A", alias="a")
         .entity("urn:t#B", alias="b").measurement(frm=["a", "b"]))
    with pytest.raises(ValueError, match="several measurement nodes"):
        q.streams()


def test_entity_only_attributes_are_rejected_on_a_stream():
    with pytest.raises(ValueError, match="does not apply to stream"):
        Query(client=None).streams(process="urn:p#ozonation")


def test_stream_only_attributes_are_rejected_on_a_measurement():
    with pytest.raises(ValueError, match="does not apply to data"):
        Query(client=None).entity("urn:t#A").measurement(source_id="svcw-scada")


# -------------------------------------------------------------- matching

def test_root_form_finds_every_registered_stream(graph):
    assert matched(graph, Query(client=None).streams()) == [
        "r_lab", "r_orphan", "r_scada"
    ]


def test_finds_streams_that_no_point_links_to(graph):
    """The gap measurement() cannot cover."""
    assert "r_orphan" in matched(graph, Query(client=None).streams())
    seen = [str(r[1]) for r in graph.query(
        Query(client=None).entity(str(BASIN)).measurement().to_sparql())]
    assert not any("orphan" in s for s in seen)


@pytest.mark.parametrize("attrs,expected", [
    ({"data_source": "SCADA"}, ["r_orphan", "r_scada"]),
    ({"source_id": "svcw-lab"}, ["r_lab"]),
    ({"ref_name": "FIT_9"}, ["r_orphan"]),
    ({"value_kind": "numeric"}, ["r_lab", "r_orphan", "r_scada"]),
])
def test_stream_attributes_filter(graph, attrs, expected):
    assert matched(graph, Query(client=None).streams(**attrs)) == expected


def test_semantic_attributes_filter_without_any_point(graph):
    assert matched(graph, Query(client=None).streams(unit=str(DEG_C))) == ["r_orphan"]


def test_filters_combine(graph):
    assert matched(graph, Query(client=None).streams(
        data_source="SCADA", unit=str(MGL))) == ["r_scada"]


def test_chained_form_narrows_a_point_to_one_of_its_references(graph):
    """The motivating case: a point with both a SCADA and a lab stream."""
    q = (Query(client=None).entity(str(BASIN)).measurement(alias="m")
         .streams(data_source="SCADA"))
    rows = [(str(r[2]).split("#")[-1], str(r[3]).split("#")[-1])
            for r in graph.query(q.to_sparql())]
    assert rows == [("r_scada", "p_do")]


def test_chained_form_binds_the_point_it_came_from(graph):
    q = Query(client=None).entity(str(BASIN)).measurement(alias="m").streams()
    points = {str(r[3]) for r in graph.query(q.to_sparql())}
    assert points == {str(POINT)}


def test_root_form_leaves_the_point_unbound_when_there_is_none(graph):
    q = Query(client=None).streams(ref_name="FIT_9")
    rows = list(graph.query(q.to_sparql()))
    assert len(rows) == 1 and rows[0][1] is None


def test_root_form_binds_a_point_when_one_exists(graph):
    q = Query(client=None).streams(ref_name="DOX_1")
    rows = list(graph.query(q.to_sparql()))
    assert len(rows) == 1 and str(rows[0][1]) == str(POINT)


# --------------------------------------------------- DataObject access

def test_data_object_covers_point_less_streams(graph):
    data = Query(client=sparql_client(graph)).streams(data_source="SCADA").data()
    assert {b.ref_uri.split("#")[-1] for b in data.bindings} == {"r_scada", "r_orphan"}


def test_point_less_bindings_keep_distinct_identities(graph):
    """point_uri is None for both, so keying dedup on it would collapse the
    two series into one and silently drop half the data."""
    data = Query(client=sparql_client(graph)).streams(source_id="svcw-scada").data()
    series = [s for s, _ in data.iter("streams")]
    assert len(series) == 2 and len(set(series)) == 2


def test_wide_frame_gets_one_column_per_stream(graph):
    data = Query(client=sparql_client(graph)).streams(source_id="svcw-scada").data()
    wide = data.dataframe(shape="wide")
    assert len(wide.columns) == 3  # time + two streams


def test_units_come_from_the_reference(graph):
    data = Query(client=sparql_client(graph)).streams(ref_name="FIT_9").data()
    assert data.units() == {"streams": str(DEG_C)}
    assert data.unit_sources() == {"streams": "reference"}


def test_metadata_exposes_the_optional_point_column(graph):
    frame = Query(client=sparql_client(graph)).streams(ref_name="DOX_1").metadata()
    assert "streams_point" in frame.columns


# ------------------------------------------------------------- to_dict

def test_stream_nodes_round_trip_through_to_dict():
    spec = Query(client=None).streams(data_source="SCADA").to_dict()
    assert spec["stream_nodes"] == [
        {"id": 0, "alias": "streams", "source_id": None,
         "filters": {"data_source": "SCADA"}}
    ]


def test_to_dict_records_the_chained_source():
    spec = (Query(client=None).entity("urn:t#A").measurement(alias="m")
            .streams().to_dict())
    assert spec["stream_nodes"][0]["source_id"] == 1


def test_query_graph_with_stream_node_is_immutable():
    g = QueryGraph().with_stream_node(StreamNodeInfo(node_id=0))
    assert QueryGraph().stream_nodes == {}
    assert list(g.stream_nodes) == [0]
