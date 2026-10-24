"""Stream identity on bindings, ref selection, and deferred stats.

The graph side runs for real: streams are written with the same
``_build_stream_triples`` that ``register_streams`` uses, and the compiled
explore SPARQL is evaluated by rdflib. Only the timeseries store is faked,
so tests can assert exactly when ``timeseries_info_batch`` is called.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest
from rdflib import RDF, Graph, URIRef

from acquirium.Client.client import _build_stream_triples
from acquirium.Client.data_object import BindingInfo, DataObject
from acquirium.Client.explore.attributes import Not
from acquirium.Client.explore.core import Query
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, VIRTUAL_POINT
from acquirium.internals.models import TimeseriesInfo, compute_ref_uri

M3_PER_HR = "http://qudt.org/vocab/unit/M3-PER-HR"
KILOW = "http://qudt.org/vocab/unit/KiloW"
FEED = str(compute_ref_uri("hist", "FT_feed"))
BARE = str(compute_ref_uri("hist", "bare"))
EXT_POINT = "urn:test:ext_point"
EXT_REF = "urn:test:ext_ref"   # an external reference: no acq:sourceId / acq:refName
T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2025, 1, 2, tzinfo=timezone.utc)


def _graph() -> Graph:
    g = Graph()
    _build_stream_triples(
        g,
        {"point_uri": "urn:test:feed", "source_id": "hist", "ref_name": "FT_feed",
         "label": "feed flow", "unit": "M3-PER-HR", "value_kind": "numeric"},
        {"unit": M3_PER_HR}, {}, lambda a, b: False,
    )
    _build_stream_triples(
        g,
        {"source_id": "hist", "ref_name": "bare", "unit": "KiloW", "value_kind": "numeric"},
        {"unit": KILOW}, {}, lambda a, b: False,
    )
    g.add((URIRef(EXT_POINT), RDF.type, VIRTUAL_POINT))
    g.add((URIRef(EXT_POINT), HAS_EXTERNAL_REFERENCE, URIRef(EXT_REF)))
    return g


class GraphClient:
    """Answers SPARQL from an rdflib graph; records every stats request."""

    def __init__(self) -> None:
        self.graph = _graph()
        self.info_calls: list[list[str]] = []

    def sparql_query(self, sparql, include_dependencies=True):
        res = self.graph.query(sparql)
        return {
            "columns": [str(v) for v in res.vars],
            "rows": [[None if x is None else str(x) for x in row] for row in res],
        }

    def timeseries_info_batch(self, uris):
        self.info_calls.append(sorted(uris))
        return {FEED: TimeseriesInfo(table="ts", row_count=5, earliest=T0, latest=T1)}

    def timeseries_df(self, ref_uri, **kwargs):
        if ref_uri != FEED:
            return pl.DataFrame(schema={"ts": pl.Datetime(time_zone="UTC"), "value": pl.Float64, "uri": pl.Utf8})
        return pl.DataFrame({"ts": [T0, T1], "value": [1.0, 2.0], "uri": [FEED, FEED]})

    def resolve_conversion(self, src, tgt):
        return {"factors": {"from_uri": src, "to_uri": tgt, "compatible": True,
                            "from_multiplier": 1.0, "from_offset": 0.0,
                            "to_multiplier": 1.0, "to_offset": 0.0}}

    def compact_uri(self, uri):
        return str(uri)

    def expand_uri(self, curie):
        prefix, _, local = curie.partition(":")
        if prefix != "acq":
            raise ValueError(f"unbound prefix {prefix!r}")
        return f"urn:acquirium#{local}"


@pytest.fixture
def client() -> GraphClient:
    return GraphClient()


def by_ref(data: DataObject) -> dict[str, BindingInfo]:
    return {b.ref_uri: b for b in data.bindings}


class TestBindingIdentity:
    def test_labelled_point_carries_source_id_and_ref_name(self, client):
        [b] = Query(client=client).measurement(label="feed flow").data().bindings
        assert b.ref_uri == FEED
        assert (b.source_id, b.ref_name) == ("hist", "FT_feed")
        assert b.point_label == "feed flow"
        assert M3_PER_HR in (b.property_unit, b.ref_unit)

    def test_stream_without_point_carries_ref_name_and_unit(self, client):
        b = by_ref(Query(client=client).measurement().data())[BARE]
        assert (b.source_id, b.ref_name) == ("hist", "bare")
        assert KILOW in (b.property_unit, b.ref_unit)

    def test_external_reference_has_no_identity(self, client):
        b = by_ref(Query(client=client).measurement().data())[EXT_REF]
        assert (b.source_id, b.ref_name) == (None, None)

    def test_binding_info_positional_construction_unchanged(self):
        b = BindingInfo(0, "urn:p", "urn:r", "a", [{}], 3)
        assert (b.row_count, b.source_id, b.ref_name) == (3, None, None)


class TestCompile:
    def test_identity_optionals_and_select_vars(self):
        s = Query(client=None).measurement().to_sparql()
        assert "OPTIONAL { ?ext0 <urn:acquirium#sourceId> ?extsid0 . }" in s
        assert "OPTIONAL { ?ext0 <urn:acquirium#refName> ?extrname0 . }" in s
        assert "?extunit0 ?extsid0 ?extrname0" in s.splitlines()[0]

    def test_multi_measurement_branches_carry_identity(self):
        s = (Query(client=None).entity("urn:test#A", alias="e")
             .measurement(alias="a").measurement(frm="e", alias="b").to_sparql())
        for nid in (1, 2):
            assert f"?ext{nid} <urn:acquirium#sourceId> ?extsid{nid}" in s
            assert f"?ext{nid} <urn:acquirium#refName> ?extrname{nid}" in s
            assert f"?extsid{nid}" in s.splitlines()[0]

    def test_ref_compiles_to_values_on_the_reference(self):
        s = Query(client=None).measurement(ref=[FEED, BARE]).to_sparql()
        assert f"VALUES ?ext0 {{ <{FEED}> <{BARE}> }}" in s

    def test_metadata_hides_identity_columns(self, client):
        q = Query(client=client).measurement()
        assert not any("sid" in c or "rname" in c for c in q.metadata().columns)
        assert {"extsid0", "extrname0"} & set(q.metadata(include_internals=True).columns)


class TestRefSelection:
    def test_one_ref(self, client):
        data = Query(client=client).measurement(ref=FEED).data(stats=False)
        assert [b.ref_uri for b in data.bindings] == [FEED]

    def test_several_refs(self, client):
        data = Query(client=client).measurement(ref=[FEED, BARE]).data(stats=False)
        assert set(by_ref(data)) == {FEED, BARE}
        assert client.info_calls == []

    def test_unknown_ref_is_empty(self, client):
        data = Query(client=client).measurement(ref="urn:test:nope").data(stats=False)
        assert data.bindings == [] and client.info_calls == []

    def test_curie_and_uriref_accepted(self, client):
        local = FEED.split("#", 1)[1]
        for ref in (f"acq:{local}", URIRef(FEED)):
            data = Query(client=client).measurement(ref=ref).data(stats=False)
            assert [b.ref_uri for b in data.bindings] == [FEED]

    def test_where_ref_intersects(self, client):
        q = Query(client=client).measurement(ref=[FEED, BARE]).where(ref=[BARE, EXT_REF])
        assert q.query_graph.data_nodes[0].refs == (BARE,)
        assert [b.ref_uri for b in q.data(stats=False).bindings] == [BARE]

    def test_ref_combines_with_attribute_filters(self, client):
        data = Query(client=client).measurement(ref=[FEED, BARE], label="bare").data(stats=False)
        assert [b.ref_uri for b in data.bindings] == [BARE]

    def test_not_ref_rejected(self):
        with pytest.raises(ValueError, match="Not"):
            Query(client=None).measurement(ref=Not(FEED))

    def test_ref_on_entity_rejected(self):
        with pytest.raises(ValueError, match="measurement nodes only"):
            Query(client=None).entity("urn:test#A", alias="e").where(ref=FEED)

    def test_to_dict_records_refs(self):
        d = Query(client=None).measurement(ref=FEED).to_dict()
        assert d["data_nodes"][0]["refs"] == [FEED]
        assert Query(client=None).measurement().to_dict()["data_nodes"][0]["refs"] is None


class TestDeferredStats:
    def test_default_fetches_stats_once(self, client):
        data = Query(client=client).measurement().data()
        assert client.info_calls == [sorted([FEED, BARE, EXT_REF])]
        b = by_ref(data)[FEED]
        assert (b.row_count, b.earliest, b.latest) == (5, T0, T1)
        assert data.stats_loaded and data.total_rows == 5
        assert len(client.info_calls) == 1

    def test_stats_false_makes_no_stats_call(self, client):
        data = Query(client=client).measurement().data(stats=False)
        b = by_ref(data)[FEED]
        assert client.info_calls == []
        assert not data.stats_loaded
        assert (b.row_count, b.earliest, b.latest) == (0, None, None)
        assert (b.point_label, b.source_id, b.ref_name) == ("feed flow", "hist", "FT_feed")
        assert M3_PER_HR in (b.property_unit, b.ref_unit)
        # nor do the graph-only readers
        data.metadata(include_ref_uris=True)
        data.units()
        repr(data)
        assert client.info_calls == []

    @pytest.mark.parametrize("reader", [
        lambda d: d.total_rows == 5,
        lambda d: d.time_range == (T0, T1),
        lambda d: d.is_empty() is False,
    ])
    def test_stats_readers_load_lazily(self, client, reader):
        data = Query(client=client).measurement().data(stats=False)
        assert reader(data)
        assert len(client.info_calls) == 1 and data.stats_loaded
        assert by_ref(data)[FEED].row_count == 5
        data.total_rows, data.time_range, data.is_empty()
        assert len(client.info_calls) == 1

    def test_materialization_needs_no_stats(self, client):
        data = Query(client=client).measurement(ref=FEED).data(stats=False)
        assert data.dataframe(shape="narrow")["value_numeric"].to_list() == [1.0, 2.0]
        assert data.latest("data")["value"].to_list() == [2.0]
        assert [p for p, _ in data.iter("data")] == ["urn:test:feed"]
        assert client.info_calls == []

    def test_query_dataframe_skips_stats(self, client):
        Query(client=client).measurement(ref=FEED).dataframe()
        assert client.info_calls == []

    def test_derived_objects_keep_deferral(self, client):
        data = Query(client=client).measurement().data(stats=False)
        converted = data.convert_to(KILOW, from_unit=KILOW)
        assert not converted.stats_loaded
        assert converted.total_rows == 5 and len(client.info_calls) == 1


class TestMetadataIdentity:
    def test_include_ref_uris_adds_identity_columns(self, client):
        meta = Query(client=client).measurement().data(stats=False).metadata(include_ref_uris=True)
        assert meta.columns[:6] == ["data_alias", "point_label", "point_uri", "ref_uri",
                                    "source_id", "ref_name"]
        rows = {r["ref_uri"]: (r["source_id"], r["ref_name"]) for r in meta.iter_rows(named=True)}
        assert rows == {FEED: ("hist", "FT_feed"), BARE: ("hist", "bare"), EXT_REF: (None, None)}

    def test_default_metadata_unchanged(self, client):
        meta = Query(client=client).measurement().data().metadata()
        assert meta.columns == ["data_alias", "point_label", "point_uri"]

    def test_materialized_metadata_includes_identity(self, client):
        data = Query(client=client).measurement(ref=FEED).data(stats=False)
        data.dataframe()
        meta = data.metadata(include_ref_uris=True)
        assert meta.columns == ["data_alias", "point_label", "point_uri", "ref_uri",
                                "source_id", "ref_name"]
        assert meta.row(0, named=True)["ref_name"] == "FT_feed"
        assert data.metadata().columns == ["data_alias", "point_label", "point_uri"]

    def test_identity_columns_are_strings_when_all_null(self, client):
        meta = Query(client=client).measurement(ref=EXT_REF).data(stats=False).metadata(include_ref_uris=True)
        assert meta.schema["source_id"] == pl.Utf8 and meta.schema["ref_name"] == pl.Utf8
