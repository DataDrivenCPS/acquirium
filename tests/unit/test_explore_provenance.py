"""Tests for executed-SPARQL retention and Query.provenance()."""

from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.traverse import clear_segment_cache

CLS_A = "urn:test#TypeA"
TANK = "urn:test#Tank"
CONNECTED_TO = "http://data.ashrae.org/standard223#connectedTo"


@pytest.fixture(autouse=True)
def clean_cache():
    clear_segment_cache()
    yield
    clear_segment_cache()


def make_client(responses):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 7
    client.sparql_query.side_effect = responses
    return client


class TestExecutedSparqlRetention:
    def test_plain_query_matches_to_sparql(self):
        client = make_client([{"columns": ["v0"], "rows": []}])
        b = Query(client=client).entity(CLS_A, alias="a")
        b.execute()
        sent = client.sparql_query.call_args.args[0]
        assert b.cache["executed_sparql"] == sent
        # No program edges: what ran is exactly the preview.
        assert b.cache["executed_sparql"] == b.to_sparql()
        assert b.cache["resolved_graph"] == b.query_graph

    def test_program_edge_records_resolved_query(self):
        client = make_client([
            {"columns": ["v0"], "rows": [["urn:p#ro1"]]},        # source fetch
            {"columns": ["v1"], "rows": [["urn:p#tankA"]]},      # target accept set
            {"columns": ["s", "t"], "rows": [["urn:p#ro1", "urn:p#tankA"]]},
            {"columns": ["v0", "v1"], "rows": []},               # final query
        ])
        b = (Query(client=client).entity(CLS_A, alias="ro")
             .related(TANK, alias="tank", via=CONNECTED_TO, nearest=True))
        b.execute()

        final_sparql = client.sparql_query.call_args.args[0]
        assert b.cache["executed_sparql"] == final_sparql
        # The preview compiles the unresolved pattern; the executed text
        # carries the BFS matches as paired VALUES. They must differ, and
        # only the retained one is faithful.
        assert b.cache["executed_sparql"] != b.to_sparql()
        assert "VALUES (?v0 ?v1)" in b.cache["executed_sparql"]

        resolved = b.cache["resolved_graph"]
        assert resolved != b.query_graph
        (edge,) = resolved.edges
        assert edge.value_pairs == (("urn:p#ro1", "urn:p#tankA"),)


class TestProvenance:
    def _query_and_client(self):
        # entity a (node 0) -> measurement m (node 1); response columns follow
        # the compiler convention: v<nid>, ext<nid>, unit<nid>.
        client = make_client([{
            "columns": ["v0", "v1", "ext1", "unit1"],
            "rows": [
                ["urn:e1", "urn:p1", "urn:r1", "urn:u1"],
                ["urn:e2", "urn:p1", "urn:r1", "urn:u1"],
                ["urn:e1", "urn:p1", "urn:r1", "urn:u1"],   # duplicate context
                ["urn:e1", "urn:p2", "urn:r2", None],
            ],
        }])
        b = Query(client=client).entity(CLS_A, alias="a").measurement(alias="m")
        return b, client

    def test_points_with_context(self):
        b, client = self._query_and_client()
        prov = b.provenance()

        assert prov["query_spec"] == b.to_dict()
        assert prov["executed_sparql"] == client.sparql_query.call_args.args[0]

        by_ref = {p["ref_uri"]: p for p in prov["points"]}
        assert set(by_ref) == {"urn:r1", "urn:r2"}
        p1 = by_ref["urn:r1"]
        assert p1["point_uri"] == "urn:p1"
        assert p1["alias"] == "m"
        # Deduplicated, order-preserving entity paths.
        assert p1["entity_contexts"] == [
            {"entity__a": "urn:e1"},
            {"entity__a": "urn:e2"},
        ]
        assert by_ref["urn:r2"]["entity_contexts"] == [{"entity__a": "urn:e1"}]

    def test_reuses_cached_execution(self):
        b, client = self._query_and_client()
        b.execute()
        b.provenance()
        b.provenance()
        assert client.sparql_query.call_count == 1

    def test_no_data_nodes_yields_no_points(self):
        client = make_client([{"columns": ["v0"], "rows": [["urn:e1"]]}])
        b = Query(client=client).entity(CLS_A, alias="a")
        prov = b.provenance()
        assert prov["points"] == []
        assert prov["executed_sparql"] is not None
