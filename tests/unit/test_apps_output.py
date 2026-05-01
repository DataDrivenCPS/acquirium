"""Tests for Output factory methods in acquirium.Apps.base."""

import pytest
from datetime import datetime, timezone

from acquirium.Apps.base import Output


# ── Output.timeseries ──────────────────────────────────────


class TestOutputTimeseries:
    def test_with_rows(self):
        rows = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0),
            (datetime(2025, 1, 2, tzinfo=timezone.utc), 43.0),
        ]
        out = Output.timeseries(point_uri="urn:test:p1", rows=rows)
        assert out.kind == "timeseries"
        assert out.payload["point_uri"] == "urn:test:p1"
        assert len(out.payload["rows"]) == 2

    def test_with_list_of_tuples_as_series(self):
        tuples = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), 10.0),
            (datetime(2025, 1, 2, tzinfo=timezone.utc), 20.0),
        ]
        out = Output.timeseries(point_uri="urn:test:p1", series=tuples)
        assert out.kind == "timeseries"
        assert len(out.payload["rows"]) == 2

    def test_no_rows_no_series_raises(self):
        with pytest.raises(ValueError, match="rows or series"):
            Output.timeseries(point_uri="urn:test:p1")


# ── Output.event ───────────────────────────────────────────


class TestOutputEvent:
    def test_valid(self):
        out = Output.event(
            point_uri="urn:test:p1",
            severity="warning",
            message="threshold exceeded",
        )
        assert out.kind == "event"
        assert out.payload["severity"] == "warning"
        assert out.payload["message"] == "threshold exceeded"
        assert out.payload["point_uri"] == "urn:test:p1"

    def test_no_point_uri_raises(self):
        with pytest.raises(ValueError, match="point_uri"):
            Output.event(severity="info", message="test")

    def test_default_ts(self):
        out = Output.event(
            point_uri="urn:test:p1",
            severity="info",
            message="test",
        )
        assert isinstance(out.payload["ts"], datetime)

    def test_custom_data(self):
        out = Output.event(
            point_uri="urn:test:p1",
            severity="error",
            message="fail",
            data={"detail": "something"},
        )
        assert out.payload["data"]["detail"] == "something"


# ── Output.trigger ─────────────────────────────────────────


class TestOutputTrigger:
    def test_valid(self):
        out = Output.trigger(url="http://example.com/hook", message="alert")
        assert out.kind == "trigger"
        assert out.payload["url"] == "http://example.com/hook"
        assert out.payload["message"] == "alert"

    def test_defaults(self):
        out = Output.trigger(url="http://example.com", message="x")
        assert out.payload["point_uri"] is None
        assert out.payload["ts"] is None
        assert out.payload["headers"] == {}
        assert out.payload["timeout"] is None
