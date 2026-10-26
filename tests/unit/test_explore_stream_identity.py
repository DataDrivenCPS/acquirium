"""Stream identity columns, ``measurement(ref=)``, and deferred stats (no server)."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from acquirium.Client.data_object import BindingInfo
from acquirium.Client.explore.core import Query
from acquirium.Client.streams import Stream

ENTITY = "urn:test#Tank"
R1, R2 = "urn:acquirium#r1", "urn:acquirium#r2"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def q(client=None) -> Query:
    return Query(client=client)


class TestCompile:
    def test_identity_optionals_on_single_data_node(self):
        sparql = q().measurement().to_sparql()
        assert "OPTIONAL { ?ext0 <urn:acquirium#sourceId> ?extsid0 . }" in sparql
        assert "OPTIONAL { ?ext0 <urn:acquirium#refName> ?extrname0 . }" in sparql
        select = sparql.split("\n", 1)[0]
        assert "?extsid0" in select and "?extrname0" in select

    def test_identity_optionals_on_every_branch_of_multi_data_nodes(self):
        sparql = q().entity(ENTITY, alias="a").measurement(alias="m1").measurement(
            frm="a", alias="m2").to_sparql()
        select = sparql.split("\n", 1)[0]
        for nid in (1, 2):
            assert f"OPTIONAL {{ ?ext{nid} <urn:acquirium#sourceId> ?extsid{nid} . }}" in sparql
            assert f"OPTIONAL {{ ?ext{nid} <urn:acquirium#refName> ?extrname{nid} . }}" in sparql
            assert f"?extsid{nid}" in select and f"?extrname{nid}" in select

    def test_ref_compiles_to_values(self):
        sparql = q().measurement(ref=[R1, R2]).to_sparql()
        assert f"VALUES ?ext0 {{ <{R1}> <{R2}> }}" in sparql

    def test_ref_on_multi_data_nodes(self):
        sparql = q().entity(ENTITY, alias="a").measurement(alias="m1", ref=R1).measurement(
            frm="a", alias="m2").to_sparql()
        assert f"VALUES ?ext1 {{ <{R1}> }}" in sparql
        assert "VALUES ?ext2" not in sparql

    def test_no_ref_no_values(self):
        assert "VALUES ?ext" not in q().measurement().to_sparql()

    def test_ref_accepts_stream_handles_and_dedups(self):
        g = q().measurement(ref=[Stream(R1), R1, R2]).query_graph
        assert g.data_nodes[0].refs == (R1, R2)

    def test_empty_ref_matches_nothing(self):
        assert "VALUES ?ext0 {  }" in q().measurement(ref=[]).to_sparql()

    @pytest.mark.parametrize("bad", ["FT_feed", "urn:x> . ?s ?p ?o"])
    def test_ref_rejects_non_uris(self, bad):
        with pytest.raises(ValueError, match="ref"):
            q().measurement(ref=bad)

    def test_to_dict_carries_refs(self):
        [node] = q().measurement(ref=R1).to_dict()["data_nodes"]
        assert node["refs"] == [R1]


def _client(rows):
    client = MagicMock()
    client.sparql_query.return_value = {
        "columns": ["v0", "ext0", "unit0", "extunit0", "extsid0", "extrname0", "lbl0"],
        "rows": rows,
    }
    client.timeseries_info_batch.side_effect = lambda uris: {
        u: SimpleNamespace(row_count=5, earliest=T0, latest=T0) for u in uris}
    return client


ROWS = [
    ["urn:p1", R1, None, "urn:unit#KW", "hist", "FT_feed", "feed flow"],
    ["urn:p2", R2, None, None, None, None, None],
]


class TestBindings:
    def test_identity_read_from_columns(self):
        d = q(_client(ROWS)).measurement().data()
        by_ref = {b.ref_uri: b for b in d.bindings}
        assert (by_ref[R1].source_id, by_ref[R1].ref_name) == ("hist", "FT_feed")
        assert (by_ref[R2].source_id, by_ref[R2].ref_name) == (None, None)

    def test_binding_info_positional_construction_unchanged(self):
        b = BindingInfo(0, "urn:p", "urn:r", "a", [{}], 1, T0, T0, None, None, "lbl")
        assert (b.point_label, b.source_id, b.ref_name) == ("lbl", None, None)

    def test_default_fetches_stats_once(self):
        client = _client(ROWS)
        d = q(client).measurement().data()
        assert client.timeseries_info_batch.call_count == 1
        assert {b.row_count for b in d.bindings} == {5}

    def test_stats_off_defers_until_read(self):
        client = _client(ROWS)
        d = q(client).measurement().data(stats=False)
        assert {(b.row_count, b.earliest, b.latest) for b in d.bindings} == {(0, None, None)}
        d.metadata(include_ref_uris=True)
        assert "stats not fetched" in repr(d)
        client.timeseries_info_batch.assert_not_called()

        assert d.total_rows == 10
        assert d.time_range == (T0, T0)
        assert not d.is_empty()
        assert client.timeseries_info_batch.call_count == 1
        assert {b.row_count for b in d.bindings} == {5}

    def test_stats_off_carries_through_by_and_convert_to(self):
        client = _client(ROWS)
        client.resolve_conversion.return_value = {"factors": {
            "from_uri": "urn:unit#KW", "to_uri": "urn:unit#W", "compatible": True,
            "from_multiplier": 1000.0, "from_offset": 0.0, "to_multiplier": 1.0, "to_offset": 0.0}}
        d = q(client).measurement().data(stats=False)
        converted = d.convert_to("urn:unit#W", from_unit="urn:unit#KW")
        assert converted._stats_loaded is False
        assert converted.total_rows == 10
        client.timeseries_info_batch.assert_called_once()

    def test_metadata_ref_columns(self):
        d = q(_client(ROWS)).measurement().data(stats=False)
        assert d.metadata().columns == ["data_alias", "point_label", "point_uri"]
        meta = d.metadata(include_ref_uris=True).sort("ref_uri")
        assert meta.columns == ["data_alias", "point_label", "point_uri",
                                "ref_uri", "source_id", "ref_name"]
        assert meta["source_id"].to_list() == ["hist", None]
        assert meta["ref_name"].to_list() == ["FT_feed", None]

    def test_query_metadata_hides_identity_columns(self):
        cols = q(_client(ROWS)).measurement().metadata().columns
        assert not any(c.startswith(("extsid", "extrname")) for c in cols)
