from itertools import count
from unittest.mock import create_autospec

import polars as pl
from typer.testing import CliRunner

import acquirium.migration as migration
from acquirium.cli import app
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import TimeseriesInfo, compute_stream_uri


def test_migrate_prints_live_progress_to_stderr_and_summary_to_stdout(tmp_path, monkeypatch):
    source_config, target_config = tmp_path / "source.toml", tmp_path / "target.toml"
    source_config.write_text('[server]\nport = 8001\n')
    target_config.write_text('[server]\nport = 8002\n')

    # The source has one reading and the destination starts empty.
    source = create_autospec(AcquiriumClient, instance=True)
    target = create_autospec(AcquiriumClient, instance=True)
    for client, port in ((source, 8001), (target, 8002)):
        client.address = f"http://127.0.0.1:{port}"
        client.engine_status.return_value = {"accepted": 0, "applied": 0, "fault": None}
    target.list_apps.return_value = {"apps": []}
    uri = str(compute_stream_uri("gateway", "flow"))
    columns = ["uri", "source_id", "stream_name", "value_kind"]
    source.sparql_query.return_value = {"columns": columns, "rows": [[uri, "gateway", "flow", "numeric"]]}
    target.sparql_query.return_value = {"columns": columns, "rows": []}
    source.timeseries_info_batch.return_value = {uri: TimeseriesInfo(row_count=1)}

    batch = pl.DataFrame({"ts": pl.Series([10], dtype=pl.Datetime("ns", "UTC")), "value": [1.0]})
    def source_batches(_uri, *, value_mode, **_bounds):
        if value_mode == "numeric":
            yield batch

    def target_batches(uri, **options):
        if target.insert_timeseries_arrow.called:
            yield from source_batches(uri, **options)

    source.timeseries_batches.side_effect = source_batches
    target.timeseries_batches.side_effect = target_batches
    target.insert_timeseries_arrow.return_value = {"receipts": [{"published": True}]}
    monkeypatch.setattr("acquirium.Client.client.AcquiriumClient", lambda host, port, ssl: {
        8001: source, 8002: target,
    }[port])
    # Advance the clock at each update so periodic progress is visible without a wait.
    monkeypatch.setattr(migration, "monotonic", count(0, 6).__next__)

    result = CliRunner().invoke(app, ["migrate", "--from", str(source_config), "--to", str(target_config)])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "Found 1 raw streams and 1 legacy rows.",
        "Verified 1 readings in 1 destination streams.",
    ]
    assert result.stderr.splitlines() == [
        "Checking source and destination servers...",
        "Reading stream registrations and row counts...",
        "Registering destination streams (1)...",
        "Copying (1/1) gateway/flow (empty destination)...",
        "  Copying (1/1) gateway/flow (empty destination) (1 processed, 6.0s elapsed)",
        "Copying (1/1) gateway/flow (empty destination) done (1 processed in 12.0s)",
        "Verifying (1/1) gateway/flow...",
        "  Verifying (1/1) gateway/flow (1 processed, 6.0s elapsed)",
        "Verifying (1/1) gateway/flow done (1 processed in 12.0s)",
        "Migration finished (42.0s).",
    ]
