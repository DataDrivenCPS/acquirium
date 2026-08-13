"""Tests for server helper functions in acquirium.Server.app."""

import pytest
from datetime import datetime, timezone

from acquirium.Server.app import _accepted_sparql_formats, _parse_dt, _sparql_results_to_rows


class TestParseDt:
    def test_none_returns_none(self):
        assert _parse_dt(None) is None

    def test_iso_with_z(self):
        result = _parse_dt("2025-12-17T10:30:00Z")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 12
        assert result.hour == 10

    def test_iso_with_offset(self):
        result = _parse_dt("2025-12-17T10:30:00+05:00")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_invalid_raises(self):
        with pytest.raises(Exception):
            _parse_dt("not-a-date")


class TestSparqlResultsToRows:
    def test_select_preserves_columns_rows_contract(self):
        result = _sparql_results_to_rows(
            b'{"head":{"vars":["s","label"]},"results":{"bindings":['
            b'{"s":{"type":"uri","value":"urn:test"},'
            b'"label":{"type":"literal","value":"Pump"}}]}}'
        )

        assert result == {"columns": ["s", "label"], "rows": [["urn:test", "Pump"]]}

    def test_ask_preserves_rows_contract(self):
        assert _sparql_results_to_rows(b'{"head":{},"boolean":true}') == {
            "columns": [], "rows": [[True]],
        }


class TestSparqlProtocolNegotiation:
    def test_defaults_to_results_json_and_turtle(self):
        results, graph = _accepted_sparql_formats("*/*")

        assert results.media_type == "application/sparql-results+json"
        assert graph.media_type == "text/turtle"

    def test_honors_weighted_graph_format(self):
        results, graph = _accepted_sparql_formats(
            "application/sparql-results+json;q=0.5, application/n-triples;q=1",
        )

        assert results.media_type == "application/sparql-results+json"
        assert graph.media_type == "application/n-triples"


class TestPickConvertiblePair:
    def _compat(self, pairs):
        return lambda a, b: (a, b) in pairs

    def test_first_ranked_pair_wins(self):
        from acquirium.Server.manager import pick_convertible_pair
        out = pick_convertible_pair(["a1", "a2"], ["b1", "b2"],
                                    self._compat({("a1", "b1"), ("a2", "b2")}))
        assert out == ("a1", "b1")

    def test_skips_incompatible_top_matches(self):
        from acquirium.Server.manager import pick_convertible_pair
        out = pick_convertible_pair(["a1", "a2"], ["b1", "b2"],
                                    self._compat({("a2", "b1")}))
        assert out == ("a2", "b1")

    def test_minimizes_rank_sum(self):
        from acquirium.Server.manager import pick_convertible_pair
        # (a1,b2) sum=1 beats (a2,b1) sum=1? tie -> from side favored: order of
        # iteration finds (a1,b2) first and (a2,b1) cannot beat it (>=).
        out = pick_convertible_pair(["a1", "a2"], ["b1", "b2"],
                                    self._compat({("a1", "b2"), ("a2", "b1"), ("a2", "b2")}))
        assert out == ("a1", "b2")

    def test_none_when_nothing_compatible(self):
        from acquirium.Server.manager import pick_convertible_pair
        assert pick_convertible_pair(["a"], ["b"], self._compat(set())) is None
