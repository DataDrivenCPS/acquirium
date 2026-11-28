"""Migrate and verify published readings through a real destination server."""

from datetime import datetime, timedelta, timezone
from math import inf, nextafter
from unittest.mock import create_autospec

import polars as pl
import pytest
from requests import HTTPError, Response

import acquirium as aq
import acquirium.migration as migration
from acquirium.Client.client import AcquiriumClient
from acquirium.core import time_ns
from acquirium.internals.models import TimeseriesInfo, compute_stream_uri
from acquirium.migration import migrate_readings


@pytest.mark.usefixtures("separate_server")
def test_migration_merges_with_live_readings_and_refuses_conflicts(tmp_path, monkeypatch):
    # The first two raw streams include legacy rows with no numeric or text value.
    legacy = create_autospec(AcquiriumClient, instance=True)
    legacy.address = "http://legacy:8000"
    missing = Response()
    missing.status_code = 404
    legacy.engine_status.side_effect = HTTPError(response=missing)
    uri = str(compute_stream_uri("gateway", "temperature"))
    unused = str(compute_stream_uri("gateway", "unused"))
    later_uri = str(compute_stream_uri("gateway", "zz-later"))
    legacy.sparql_query.return_value = {
        "columns": ["uri", "source_id", "stream_name", "property_uri", "value_kind", "unit", "quantity_kind"],
        "rows": [
            [uri, "gateway", "temperature", "urn:example:temperature", "numeric",
             "http://qudt.org/vocab/unit/DEG_C", "http://qudt.org/vocab/quantitykind/Temperature"],
            [unused, "gateway", "unused", None, "numeric", None, None],
            [later_uri, "gateway", "zz-later", None, "numeric", None, None],
            [str(compute_stream_uri("derived:test", "mean")), "derived:test", "mean", None, "numeric", None, None],
            [str(compute_stream_uri("experiment/old", "samples")), "experiment/old", "samples",
             None, "numeric", None, None],
        ],
    }
    legacy.timeseries_info_batch.return_value = {
        uri: TimeseriesInfo(row_count=5),
        unused: TimeseriesInfo(row_count=1),
        later_uri: TimeseriesInfo(row_count=1),
    }

    at = datetime(2026, 1, 1, microsecond=123456, tzinfo=timezone.utc)
    numeric = [(at, 21.5), (at + timedelta(microseconds=1), 22.0), (at + timedelta(microseconds=2), 23.0)]
    faults = [(at + timedelta(seconds=3), "Fault")]
    null_at = at + timedelta(seconds=4)
    live_numeric = [
        (at - timedelta(seconds=1), 20.0),
        (at + timedelta(milliseconds=1500), 44.0),
        (at + timedelta(seconds=5), 45.0),
    ]
    live_text = [(null_at, "Live status")]

    def legacy_batches(stream_uri, *, value_mode, view, start=None, end=None):
        assert stream_uri in {uri, unused, later_uri} and view == "stored"
        legacy_end = datetime.fromisoformat(end) if end is not None else None
        if value_mode == "coalesce":
            rows = [(null_at, None)]
            if stream_uri == uri:
                rows = [(stamp, str(value)) for stamp, value in numeric + faults] + rows
        elif stream_uri == uri:
            rows = numeric if value_mode == "numeric" else faults
        elif stream_uri == later_uri and value_mode == "numeric":
            rows = [(at, 30.0)]
        else:
            rows = []
        if start is not None:
            rows = [(ts, value) for ts, value in rows if ts >= datetime.fromisoformat(start)]
        if legacy_end is not None:
            inclusive = legacy.engine_status.side_effect is not None
            rows = [(ts, value) for ts, value in rows if ts < legacy_end or (inclusive and ts == legacy_end)]
        # Split the source at different timestamps from the destination.
        frame = pl.DataFrame(rows, schema=["ts", "value"], orient="row")
        yield from frame.iter_slices(2)

    legacy.timeseries_batches.side_effect = legacy_batches
    # Flush a missing batch while the merge still has destination rows to read.
    monkeypatch.setattr(migration, "MAX_REQUEST_ROWS", 1)

    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    try:
        report = []
        migrate_readings(legacy, client.client, dry_run=True, report=report.append)
        assert report == ["Found 3 raw streams and 7 legacy rows."]
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
            "source_id": "gateway",
            "stream_name": "zz-later",
            "value_kind": "numeric",
        }, {
            "source_id": "live",
            "stream_name": "other",
            "value_kind": "numeric",
        }])
        client.insert_timeseries("gateway", "temperature", [numeric[0], numeric[2], *live_numeric, *live_text])
        client.insert_timeseries("gateway", "zz-later", [(at, 30.0)])
        client.insert_timeseries("live", "other", [(at, 100.0)])
        wave = client.stream("gateway", "temperature")

        # Detect a one-ULP conflict in the next source batch before filling the gap.
        conflict_at, exact = numeric[2]
        client.insert_timeseries("gateway", "temperature", [(conflict_at, nextafter(exact, inf))])
        applied = client.engine_status()["applied"]
        with pytest.raises(ValueError, match=f"Destination readings differ for {uri} at {time_ns(conflict_at)}"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        assert numeric[1] not in wave.dataframe(value_mode="numeric").rows()
        client.insert_timeseries("gateway", "temperature", [numeric[2]])

        # The global preflight checks later streams before it writes any gaps.
        client.insert_timeseries("gateway", "zz-later", [(at, 31.0)])
        applied = client.engine_status()["applied"]
        with pytest.raises(ValueError, match=f"Destination readings differ for {later_uri}"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        assert numeric[1] not in wave.dataframe(value_mode="numeric").rows()
        client.insert_timeseries("gateway", "zz-later", [(at, 30.0)])

        # Interrupt the run after its first missing batch has published.
        insert = client.client.insert_timeseries_arrow

        def interrupted(source_id, table):
            """Let the numeric gap publish and interrupt the fault write."""
            if table["value"].to_pylist() == ["Fault"]:
                raise ConnectionError("interrupted copy")
            return insert(source_id, table)

        applied = client.engine_status()["applied"]
        legacy.timeseries_batches.reset_mock()
        with monkeypatch.context() as patch:
            # Two sparse gaps exceed this budget and use a whole-stream pass.
            patch.setattr(migration, "_MAX_WINDOWS", 1)
            patch.setattr(client.client, "insert_timeseries_arrow", interrupted)
            with pytest.raises(ConnectionError, match="interrupted copy"):
                migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert not any("start" in call.kwargs for call in legacy.timeseries_batches.call_args_list)
        assert client.engine_status()["applied"] == applied + 1
        assert numeric[1] in wave.dataframe(value_mode="numeric").rows()
        assert wave.dataframe(value_mode="text").rows() == live_text

        # The retry reads that remaining window twice, for copy and verification.
        legacy.timeseries_batches.reset_mock()
        applied = client.engine_status()["applied"]
        migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied + 1
        bounded_reads = [call.kwargs for call in legacy.timeseries_batches.call_args_list if "start" in call.kwargs]
        fault_time = faults[0][0]
        assert [(read["start"], read["end"]) for read in bounded_reads] == [
            (fault_time.isoformat(), (fault_time + timedelta(microseconds=1)).isoformat())
        ] * 4
        assert "Verified 4 already copied readings." in report
        assert "Skipped 1 legacy rows with no value for gateway/temperature." in report
        assert "Skipped 1 legacy rows with no value for gateway/unused." in report
        assert report[-2:] == [
            "Verified 5 readings in 3 destination streams.",
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
        assert client.engine_status()["accepted"] == applied

        # An unexplained count difference remains a failure.
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=6)
        with pytest.raises(RuntimeError, match="Legacy counts disagree"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["applied"] == applied
        legacy.timeseries_info_batch.return_value[uri] = TimeseriesInfo(row_count=5)

        # A legacy text reading conflicts with a numeric reading of the same digits.
        first = numeric.pop(0)
        faults.insert(0, (at, str(first[1])))
        with pytest.raises(ValueError, match="Destination readings differ"):
            migrate_readings(legacy, client.client, dry_run=False, report=report.append)
        assert client.engine_status()["accepted"] == applied
        assert wave.dataframe(value_mode="numeric").rows() == sorted([first, *numeric, *live_numeric])
        assert wave.dataframe(value_mode="text").rows() == faults[1:] + live_text
    finally:
        aq.shutdown()
