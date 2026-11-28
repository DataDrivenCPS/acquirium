"""Migrate and verify published readings through a real destination server."""

from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

import polars as pl
import pytest
from requests import HTTPError, Response

import acquirium as aq
import acquirium.migration as migration
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import TimeseriesInfo, compute_stream_uri
from acquirium.migration import migrate_readings


@pytest.mark.usefixtures("separate_server")
def test_migration_merges_with_live_readings_and_refuses_conflicts(tmp_path, monkeypatch):
    # Both raw streams include legacy rows with no numeric or text value.
    legacy = create_autospec(AcquiriumClient, instance=True)
    legacy.address = "http://legacy:8000"
    missing = Response()
    missing.status_code = 404
    legacy.engine_status.side_effect = HTTPError(response=missing)
    uri = str(compute_stream_uri("gateway", "temperature"))
    unused = str(compute_stream_uri("gateway", "unused"))
    legacy.sparql_query.return_value = {
        "columns": ["uri", "source_id", "stream_name", "property_uri", "value_kind", "unit", "quantity_kind"],
        "rows": [
            [uri, "gateway", "temperature", "urn:example:temperature", "numeric",
             "http://qudt.org/vocab/unit/DEG_C", "http://qudt.org/vocab/quantitykind/Temperature"],
            [unused, "gateway", "unused", None, "numeric", None, None],
            [str(compute_stream_uri("derived:test", "mean")), "derived:test", "mean", None, "numeric", None, None],
            [str(compute_stream_uri("experiment/old", "samples")), "experiment/old", "samples",
             None, "numeric", None, None],
        ],
    }
    legacy.timeseries_info_batch.return_value = {
        uri: TimeseriesInfo(row_count=5),
        unused: TimeseriesInfo(row_count=1),
    }

    at = datetime(2026, 1, 1, microsecond=123456, tzinfo=timezone.utc)
    numeric = [(at, 21.5), (at + timedelta(seconds=1), 22.0), (at + timedelta(seconds=2), 23.0)]
    faults = [(at + timedelta(seconds=3), "Fault")]
    null_at = at + timedelta(seconds=4)
    live_numeric = [
        (at - timedelta(seconds=1), 20.0),
        (at + timedelta(milliseconds=1500), 44.0),
        (at + timedelta(seconds=5), 45.0),
    ]
    live_text = [(null_at, "Live status")]

    def legacy_batches(stream_uri, *, value_mode, view):
        assert stream_uri in {uri, unused} and view == "stored"
        if value_mode == "coalesce":
            rows = [(null_at, None)]
            if stream_uri == uri:
                rows = [(stamp, str(value)) for stamp, value in numeric + faults] + rows
            yield pl.DataFrame(rows, schema=["ts", "value"], orient="row")
            return
        if stream_uri == unused:
            return
        rows = numeric if value_mode == "numeric" else faults
        yield pl.DataFrame(rows, schema=["ts", "value"], orient="row")

    legacy.timeseries_batches.side_effect = legacy_batches
    # Flush a missing batch while the merge still has destination rows to read.
    monkeypatch.setattr(migration, "MAX_REQUEST_ROWS", 1)

    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    try:
        report = []
        migrate_readings(legacy, client.client, dry_run=True, report=report.append)
        assert report == ["Found 2 raw streams and 6 legacy rows."]
        assert client.client.sparql_query(
            "SELECT ?stream WHERE { ?stream <urn:acquirium#sourceId> 'gateway' }",
            wait_for_fresh=True,
        )["rows"] == []

        # Leave a historical gap and add destination-only readings on both sides of it.
        client.register_streams([{
            "source_id": "gateway",
            "stream_name": "temperature",
            "property_uri": "urn:example:temperature",
            "value_kind": "numeric",
            "unit": "http://qudt.org/vocab/unit/DEG_C",
            "quantity_kind": "http://qudt.org/vocab/quantitykind/Temperature",
        }, {
            "source_id": "live",
            "stream_name": "other",
            "value_kind": "numeric",
        }])
        client.insert_timeseries("gateway", "temperature", [numeric[0], numeric[2], *live_numeric, *live_text])
        client.insert_timeseries("live", "other", [(at, 100.0)])
        wave = client.stream("gateway", "temperature")

        # Detect a conflict after the missing timestamp before writing into the gap.
        client.insert_timeseries("gateway", "temperature", [(numeric[2][0], 99.0)])
        applied = client.engine_status()["applied"]
        with pytest.raises(ValueError, match="Destination readings differ"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        assert numeric[1] not in wave.dataframe(value_mode="numeric").rows()
        client.insert_timeseries("gateway", "temperature", [numeric[2]])

        migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert "Verified 2 already copied readings." in report
        assert "Skipped 1 legacy rows with no value for gateway/temperature." in report
        assert "Skipped 1 legacy rows with no value for gateway/unused." in report
        assert report[-2:] == [
            "Verified 4 readings in 2 destination streams.",
            "Skipped 2 legacy rows with no value in total.",
        ]
        assert str(wave) == uri
        assert wave.dataframe(value_mode="numeric").rows() == sorted(numeric + live_numeric)
        assert wave.dataframe(value_mode="text").rows() == faults + live_text
        assert client.stream("live", "other").dataframe().rows() == [(at, 100.0)]
        assert client.client.sparql_query(
            f"SELECT ?name WHERE {{ <{unused}> <urn:acquirium#refName> ?name }}",
            wait_for_fresh=True,
        )["rows"] == [["unused"]]

        # An older Escapement source must finish publishing before it exports readings.
        applied = client.engine_status()["applied"]
        legacy.engine_status.side_effect = None
        legacy.engine_status.return_value = {"accepted": 2, "applied": 1, "fault": None}
        with pytest.raises(ValueError, match="The source has pending or failed writes"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied

        # A published Escapement source supports the same retry without rewriting readings.
        legacy.engine_status.return_value["applied"] = 2
        migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied

        # An unexplained count difference remains a failure.
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=6)
        with pytest.raises(RuntimeError, match="Legacy counts disagree"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=5)

        # Text at a legacy numeric timestamp is a conflict too.
        client.insert_timeseries("gateway", "temperature", [(at, "Sensor fault")])
        with pytest.raises(ValueError, match="Destination readings differ"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert wave.dataframe(value_mode="numeric").rows() == sorted(numeric[1:] + live_numeric)
        assert wave.dataframe(value_mode="text").rows() == [(at, "Sensor fault"), *faults, *live_text]
    finally:
        aq.shutdown()
