"""Tests for options(): two-phase facet counting (pattern once, flat lookup).

Phase 1 is the ordinary execute() (cached, program edges BFS-resolved);
phase 2 is a VALUES-anchored attribute lookup; counting happens in Python.
"""

from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Client.explore.core import Q

CLS_A = "urn:test#TypeA"
OF_MEDIUM = "http://data.ashrae.org/standard223#ofMedium"
HAS_MEDIUM = "http://data.ashrae.org/standard223#hasMedium"
QK_PRED = "http://qudt.org/schema/qudt/hasQuantityKind"

M1, M2 = "urn:p#m1", "urn:p#m2"


def make_client(pattern_rows, lookup_rows):
    """Pattern query -> [v0, v1, ext1, ...]; lookup -> [v, opt]."""
    client = MagicMock()

    def respond(sparql, use_union=True):
        if sparql.startswith("SELECT ?v ?opt"):
            return {"columns": ["v", "opt"], "rows": lookup_rows}
        return {"columns": ["v0", "v1", "ext1", "unit1", "extunit1"],
                "rows": pattern_rows}

    client.sparql_query.side_effect = respond
    client.compact_uri.side_effect = lambda x: str(x).rsplit("#", 1)[-1]
    return client


def base(client) -> Q:
    return Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m")


def pattern_row(m):
    return ["urn:p#ro1", m, "urn:p#ref", None, None]


class TestOptionsQueries:
    def test_pattern_runs_through_execute_and_is_shared(self):
        client = make_client([pattern_row(M1)], [])
        b = base(client)
        b.metadata()
        b.options("quantity_kind")
        # 1 pattern query (shared via cache) + 1 lookup query
        assert client.sparql_query.call_count == 2

    def test_lookup_is_values_anchored_flat_query(self):
        client = make_client([pattern_row(M1), pattern_row(M2)], [])
        base(client).options("quantity_kind")
        lookup = client.sparql_query.call_args.args[0]
        assert lookup.startswith("SELECT ?v ?opt")
        assert f"VALUES ?v {{ <{M1}> <{M2}> }}" in lookup
        assert f"?v (<{QK_PRED}>) ?opt ." in lookup
        assert "GROUP BY" not in lookup and "count" not in lookup.lower()

    def test_multi_predicate_attr_unions(self):
        client = make_client([pattern_row(M1)], [])
        base(client).options("medium")
        lookup = client.sparql_query.call_args.args[0]
        assert f"(<{OF_MEDIUM}>|<{HAS_MEDIUM}>)" in lookup

    def test_of_targets_entity_node(self):
        client = make_client([pattern_row(M1)], [])
        base(client).options("process", of="ro")
        lookup = client.sparql_query.call_args.args[0]
        assert "VALUES ?v { <urn:p#ro1> }" in lookup
        assert "hasProcess" in lookup

    def test_empty_pattern_skips_lookup(self):
        client = make_client([], [])
        df = base(client).options("quantity_kind")
        assert df.height == 0
        assert client.sparql_query.call_count == 1  # only the pattern query

    def test_chunks_large_value_sets(self):
        rows = [pattern_row(f"urn:p#m{i}") for i in range(1200)]
        client = make_client(rows, [])
        base(client).options("quantity_kind")
        lookups = [c.args[0] for c in client.sparql_query.call_args_list
                   if c.args[0].startswith("SELECT ?v ?opt")]
        assert len(lookups) == 3  # 500 + 500 + 200


class TestOptionsCounting:
    def test_counts_distinct_nodes_per_value_in_python(self):
        lookup_rows = [
            [M1, "urn:qk#Flow"],
            [M1, "urn:qk#Flow"],   # duplicate binding must not double-count
            [M2, "urn:qk#Flow"],
            [M2, "urn:qk#PH"],
        ]
        client = make_client([pattern_row(M1), pattern_row(M2)], lookup_rows)
        df = base(client).options("quantity_kind")
        assert df["quantity_kind"].to_list() == ["Flow", "PH"]
        assert df["count"].to_list() == [2, 1]
        assert df.schema["count"] == pl.Int64

    def test_cached_per_attr_and_node(self):
        client = make_client([pattern_row(M1)], [])
        b = base(client)
        b.options("quantity_kind")
        b.options("quantity_kind")
        assert client.sparql_query.call_count == 2  # pattern + one lookup
        b.options("unit")
        assert client.sparql_query.call_count == 3  # one more lookup only


class TestOptionsErrors:
    def test_unknown_attr(self):
        with pytest.raises(ValueError, match="unknown attribute"):
            base(make_client([], [])).options("flavour")

    def test_unknown_alias(self):
        with pytest.raises(ValueError, match="unknown alias"):
            base(make_client([], [])).options("medium", of="nope")

    def test_role_mismatch(self):
        with pytest.raises(ValueError, match="does not apply to data"):
            base(make_client([], [])).options("process")
        with pytest.raises(ValueError, match="does not apply to entity"):
            base(make_client([], [])).options("quantity_kind", of="ro")
