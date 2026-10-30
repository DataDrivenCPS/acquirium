"""Tests for acquirium.internals.models."""

from acquirium.internals.models import Point


# ── Point ──────────────────────────────────────────────────


class TestPoint:
    def test_defaults(self):
        p = Point(uri="urn:test:point1")
        assert p.uri == "urn:test:point1"
        assert p.ref_uri is None
        assert p.types == []
        assert p.unit is None
        assert p.last_reported is None
        assert p.stream is None

    def test_full(self):
        p = Point(
            uri="urn:test:point1",
            ref_uri="h1",
            types=["TypeA"],
            unit="degC",
        )
        assert p.ref_uri == "h1"
        assert p.types == ["TypeA"]
        assert p.unit == "degC"
