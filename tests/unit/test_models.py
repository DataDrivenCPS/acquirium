"""Tests for acquirium.internals.models — Pydantic models and TimeInterval."""

import pytest
from datetime import datetime, timezone, timedelta

from acquirium.internals.models import (
    TimeInterval,
    TimeIntervalModel,
    LogEntry,
    Point,
    AppSpec,
    AppOutputSpec,
)


# ── TimeInterval ───────────────────────────────────────────


class TestTimeIntervalStartEnd:
    def test_valid(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        ti = TimeInterval(start=start, end=end)
        assert ti.start == start
        assert ti.end == end

    def test_end_before_start_raises(self):
        start = datetime(2025, 1, 2, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="end must be after start"):
            TimeInterval(start=start, end=end)

    def test_equal_start_end_raises(self):
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="end must be after start"):
            TimeInterval(start=dt, end=dt)


class TestTimeIntervalDuration:
    def test_start_and_duration(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        ti = TimeInterval(start=start, duration=3600)
        assert ti.end == start + timedelta(seconds=3600)

    def test_end_and_duration(self):
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        ti = TimeInterval(end=end, duration=3600)
        assert ti.start == end - timedelta(seconds=3600)

    def test_all_three_raises(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="only one of start or end"):
            TimeInterval(start=start, end=end, duration=3600)


class TestTimeIntervalEdgeCases:
    def test_neither_start_nor_end_raises(self):
        with pytest.raises(ValueError, match="Either start or end"):
            TimeInterval()

    def test_only_start_no_duration_raises(self):
        with pytest.raises(ValueError, match="Both start and end"):
            TimeInterval(start=datetime(2025, 1, 1, tzinfo=timezone.utc))

    def test_frozen(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        ti = TimeInterval(start=start, end=end)
        with pytest.raises(AttributeError):
            ti.start = datetime(2025, 6, 1, tzinfo=timezone.utc)


class TestTimeIntervalSerialization:
    def test_serialize(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        ti = TimeInterval(start=start, end=end)
        result = ti.serialize()
        assert result["start"] == start.isoformat()
        assert result["end"] == end.isoformat()

    def test_to_str_from_str_roundtrip(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)
        ti = TimeInterval(start=start, end=end)
        restored = TimeInterval.from_str(ti.to_str())
        assert restored.start == ti.start
        assert restored.end == ti.end


# ── LogEntry ───────────────────────────────────────────────


class TestLogEntry:
    def test_to_dict_full(self):
        entry = LogEntry(
            point_uri="urn:test:point1",
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(
                start=datetime(2025, 6, 15, 9, 0, tzinfo=timezone.utc),
                end=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            ),
            message="test message",
        )
        d = entry.to_dict()
        assert d["point_uri"] == "urn:test:point1"
        assert d["message"] == "test message"
        assert d["log_time"] is not None
        assert d["observation_start"] is not None
        assert d["observation_end"] is not None

    def test_to_dict_none_period(self):
        entry = LogEntry(
            point_uri="urn:test:point1",
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(),
            message="test",
        )
        d = entry.to_dict()
        assert d["observation_start"] is None
        assert d["observation_end"] is None


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


# ── AppSpec ────────────────────────────────────────────────


class TestAppSpec:
    def test_minimal(self):
        spec = AppSpec(name="my_app")
        assert spec.name == "my_app"
        assert spec.version == "0.0"
        assert spec.outputs == []

    def test_with_outputs(self):
        out = AppOutputSpec(kind="timeseries", point_uri="urn:test:p1")
        spec = AppSpec(name="my_app", outputs=[out])
        assert len(spec.outputs) == 1
        assert spec.outputs[0].kind == "timeseries"


# ── TimeIntervalModel ─────────────────────────────────────


class TestTimeIntervalModel:
    def test_to_interval_valid(self):
        m = TimeIntervalModel(
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        ti = m.to_interval()
        assert ti.start == m.start
        assert ti.end == m.end

    def test_to_interval_missing_raises(self):
        m = TimeIntervalModel(start=datetime(2025, 1, 1, tzinfo=timezone.utc))
        with pytest.raises(ValueError):
            m.to_interval()
