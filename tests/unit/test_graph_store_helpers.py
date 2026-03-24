"""Tests for pure helper functions in acquirium.Storage.graph_store."""

import pytest
from datetime import datetime, timezone, timedelta

from rdflib import Literal, URIRef
from rdflib.namespace import XSD

from acquirium.Storage.graph_store import _literal_dt, _maybe_literal_dt, _external_uri
from acquirium.internals.internals_namespaces import ACQUIRIUM_POINT_NS


class TestLiteralDt:
    def test_naive_datetime_becomes_utc(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        lit = _literal_dt(dt)
        assert isinstance(lit, Literal)
        assert lit.datatype == XSD.dateTime
        assert "+00:00" in str(lit) or "Z" in str(lit)

    def test_aware_datetime_converted_to_utc(self):
        eastern = timezone(timedelta(hours=-5))
        dt = datetime(2025, 6, 15, 10, 30, 0, tzinfo=eastern)
        lit = _literal_dt(dt)
        parsed = datetime.fromisoformat(str(lit))
        assert parsed == dt.astimezone(timezone.utc)

    def test_value_is_valid_iso(self):
        dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        lit = _literal_dt(dt)
        # Should not raise
        datetime.fromisoformat(str(lit))


class TestMaybeLiteralDt:
    def test_none_returns_none(self):
        assert _maybe_literal_dt(None) is None

    def test_valid_xsd_datetime(self):
        lit = Literal("2025-06-15T10:30:00+00:00", datatype=XSD.dateTime)
        result = _maybe_literal_dt(lit)
        assert isinstance(result, datetime)
        assert result.year == 2025

    def test_invalid_literal_returns_none(self):
        lit = Literal("not-a-date", datatype=XSD.dateTime)
        result = _maybe_literal_dt(lit)
        assert result is None


class TestExternalUri:
    def test_point_ns_stripped(self):
        uri = URIRef(str(ACQUIRIUM_POINT_NS) + "some_point_id")
        result = _external_uri(uri)
        assert result == "some_point_id"

    def test_other_uri_unchanged(self):
        uri = URIRef("http://example.org/sensor1")
        result = _external_uri(uri)
        assert result == "http://example.org/sensor1"
