"""Migrate and verify published readings through a real destination server."""

from datetime import datetime, timedelta, timezone
from unittest.mock import create_autospec

import polars as pl
import pytest
from requests import HTTPError, Response

import acquirium as aq
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import TimeseriesInfo, compute_stream_uri
from acquirium.migration import migrate_readings


@pytest.mark.usefixtures("separate_server")
def test_migration_previews_resumes_and_refuses_conflicting_readings(tmp_path):
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
        uri: TimeseriesInfo(row_count=4),
        unused: TimeseriesInfo(row_count=1),
    }

    at = datetime(2026, 1, 1, microsecond=123456, tzinfo=timezone.utc)
    numeric = [(at, 21.5), (at + timedelta(seconds=1), 22.0)]
    faults = [(at + timedelta(seconds=2), "Fault")]

    def legacy_batches(stream_uri, *, value_mode, view):
        assert stream_uri in {uri, unused} and view == "stored"
        if value_mode == "coalesce":
            rows = [(at + timedelta(seconds=3), None)]
            if stream_uri == uri:
                rows = [(stamp, str(value)) for stamp, value in numeric + faults] + rows
            yield pl.DataFrame(rows, schema=["ts", "value"], orient="row")
            return
        if stream_uri == unused:
            return
        rows = numeric if value_mode == "numeric" else faults
        yield pl.DataFrame(rows, schema=["ts", "value"], orient="row")

    legacy.timeseries_batches.side_effect = legacy_batches

    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    try:
        report = []
        migrate_readings(legacy, client.client, dry_run=True, report=report.append)
        assert report == ["Found 2 raw streams and 5 legacy rows."]
        assert client.client.sparql_query(
            "SELECT ?stream WHERE { ?stream <urn:acquirium#sourceId> 'gateway' }",
            wait_for_fresh=True,
        )["rows"] == []

        # Simulate an interrupted migration that published the first numeric reading.
        client.register_streams([{
            "source_id": "gateway",
            "stream_name": "temperature",
            "property_uri": "urn:example:temperature",
            "value_kind": "numeric",
            "unit": "http://qudt.org/vocab/unit/DEG_C",
            "quantity_kind": "http://qudt.org/vocab/quantitykind/Temperature",
        }])
        client.insert_timeseries("gateway", "temperature", numeric[:1])
        migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert "Verified 1 already copied readings." in report
        assert "Skipped 1 legacy rows with no value for gateway/temperature." in report
        assert "Skipped 1 legacy rows with no value for gateway/unused." in report
        assert report[-2:] == [
            "Verified 3 readings in 2 destination streams.",
            "Skipped 2 legacy rows with no value in total.",
        ]
        wave = client.stream("gateway", "temperature")
        assert str(wave) == uri
        assert wave.dataframe(value_mode="numeric").rows() == numeric
        assert wave.dataframe(value_mode="text").rows() == faults
        assert client.client.sparql_query(
            f"SELECT ?name WHERE {{ <{unused}> <urn:acquirium#refName> ?name }}",
            wait_for_fresh=True,
        )["rows"] == [["unused"]]

        # A complete retry publishes no additional reading writes.
        applied = client.engine_status()["applied"]
        migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied

        # An unexplained count difference remains a failure.
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=5)
        with pytest.raises(RuntimeError, match="Legacy counts disagree"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=4)

        client.insert_timeseries("gateway", "temperature", [(at, 99.0)])
        with pytest.raises(ValueError, match="Destination readings differ"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert wave.dataframe(value_mode="numeric").rows() == [(at, 99.0), numeric[1]]
        assert wave.dataframe(value_mode="text").rows() == faults
    finally:
        aq.shutdown()
