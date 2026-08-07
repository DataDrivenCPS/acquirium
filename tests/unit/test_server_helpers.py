"""Tests for server helper functions in acquirium.Server.app."""

import pytest
from datetime import datetime, timezone

from acquirium.Server.app import _parse_dt


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
