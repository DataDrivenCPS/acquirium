"""Tests for Output factory methods in acquirium.Apps.base."""

import json
import pytest
from datetime import datetime, timezone

from acquirium.Apps.base import Output
from acquirium.Apps.output_emission import emit_outputs, normalize_trigger_url


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


class TestEmitOutputs:
    def test_timeseries_output_uses_insert_callback(self):
        calls = []
        rows = [(datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)]

        emit_outputs(
            "app-a",
            [Output.timeseries(point_uri="urn:test:p1", rows=rows)],
            insert_timeseries=lambda **kwargs: calls.append(kwargs),
        )

        assert calls == [
            {
                "source_id": "app-a",
                "ref_name": "urn:test:p1",
                "rows": rows,
                "point_uri": "urn:test:p1",
            }
        ]

    def test_event_output_serializes_json_text_row(self):
        calls = []
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        emit_outputs(
            "app-a",
            [
                Output.event(
                    point_uri="urn:test:event",
                    severity="warning",
                    message="threshold",
                    ts=ts,
                    data={"value": 10},
                )
            ],
            insert_timeseries=lambda **kwargs: calls.append(kwargs),
        )

        assert calls[0]["source_id"] == "app-a"
        assert calls[0]["ref_name"] == "urn:test:event"
        assert calls[0]["point_uri"] == "urn:test:event"
        row_ts, row_value = calls[0]["rows"][0]
        assert row_ts == ts
        assert json.loads(row_value) == {
            "severity": "warning",
            "message": "threshold",
            "data": {"value": 10},
        }

    def test_trigger_output_posts_webhook(self, monkeypatch):
        calls = []

        class _Response:
            status_code = 202

            def raise_for_status(self):
                calls.append(("raise_for_status",))

        def fake_post(url, json, headers, timeout):
            calls.append((url, json, headers, timeout))
            return _Response()

        monkeypatch.setattr("acquirium.Apps.output_emission.requests.post", fake_post)
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        emit_outputs(
            "app-a",
            [
                Output.trigger(
                    url="hooks.local/notify",
                    message="hello",
                    ts=ts,
                    point_uri="urn:test:p1",
                    headers={"X-Test": "1"},
                    timeout=3,
                )
            ],
            insert_timeseries=lambda **kwargs: pytest.fail("trigger should not insert timeseries"),
        )

        assert calls == [
            (
                "http://hooks.local/notify",
                {"message": "hello", "ts": ts.isoformat(), "point_uri": "urn:test:p1"},
                {"X-Test": "1"},
                3,
            ),
            ("raise_for_status",),
        ]

    def test_normalize_trigger_url_adds_default_scheme(self):
        assert normalize_trigger_url("example.com/hook") == "http://example.com/hook"
        assert normalize_trigger_url("https://example.com/hook") == "https://example.com/hook"
