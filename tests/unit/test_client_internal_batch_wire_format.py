"""Confirms AcquiriumClient's next_app_batch/commit_app_batch build and parse
the exact wire format Server/app.py's internal endpoints produce/expect
(continuous_batch_plan.md Decision 6) -- mocking `requests` so no live
server is needed, but reusing the server's own `_arrow_response` helper and
endpoint parsing logic as the source of truth for the format.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow.ipc as ipc
import pyarrow as pa

import acquirium.Server.app as server_app
from acquirium.Client.client import AcquiriumClient
from acquirium.Storage.continuous.types import MUTATION_SCHEMA


def _mutation_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "operation": [r[0] for r in rows],
            "ref_uri": [r[1] for r in rows],
            "ts": [r[2] for r in rows],
            "numeric_value": [r[3] for r in rows],
            "text_value": [r[4] for r in rows],
        },
        schema=MUTATION_SCHEMA,
    )


@patch("acquirium.Client.client.requests")
def test_next_app_batch_parses_the_servers_own_arrow_response(mock_requests):
    rows = _mutation_table([("upsert", "s1", datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0, None)])
    server_bytes = server_app._arrow_response(
        rows, "acquirium_batch",
        {
            "batch_id": "b1", "batch_kind": "tail", "generation": 4, "has_more": False,
            "inputs": [{"ref_uri": "s1", "from_version": 0, "to_version": 1}],
            "bootstrap_id": None, "end_ordinal": None,
        },
    ).body

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = server_bytes
    mock_requests.post.return_value = mock_resp

    client = AcquiriumClient(server_url="localhost", server_port=8000)
    result = client.next_app_batch("app1", 4)

    assert result["batch_id"] == "b1"
    assert result["batch_kind"] == "tail"
    assert result["generation"] == 4
    assert result["has_more"] is False
    assert result["inputs"] == [{"ref_uri": "s1", "from_version": 0, "to_version": 1}]
    assert result["rows"].num_rows == 1


@patch("acquirium.Client.client.requests")
def test_next_app_batch_returns_none_on_204(mock_requests):
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    mock_requests.post.return_value = mock_resp

    client = AcquiriumClient(server_url="localhost", server_port=8000)
    assert client.next_app_batch("app1", 1) is None


@patch("acquirium.Client.client.requests")
def test_commit_app_batch_request_parses_via_the_servers_own_logic(mock_requests):
    """Build a commit request with the client, then feed the exact bytes it
    would have sent over the wire through the server's own parsing path."""
    import json as _json

    outputs = _mutation_table([("upsert", "out1", datetime(2026, 1, 1, tzinfo=timezone.utc), 2.0, None)])

    sent: dict = {}

    def fake_post(url, *, data=None, headers=None, **kwargs):
        sent["url"] = url
        sent["body"] = data.read() if hasattr(data, "read") else data
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"rows_inserted": 1, "already_committed": False, "output_versions": {}}
        return resp

    mock_requests.post.side_effect = fake_post

    client = AcquiriumClient(server_url="localhost", server_port=8000)
    client.commit_app_batch(
        "app1", "b1", generation=2, batch_kind="tail", rows=outputs,
        inputs=[{"ref_uri": "s1", "from_version": 0, "to_version": 1}],
    )

    assert sent["url"].endswith("/internal/apps/app1/batches/b1/commit")
    reader = ipc.RecordBatchStreamReader(pa.BufferReader(sent["body"]))
    table = reader.read_all()
    meta = _json.loads(table.schema.metadata[b"acquirium_commit"])
    assert meta["generation"] == 2
    assert meta["batch_kind"] == "tail"
    assert meta["inputs"] == [{"ref_uri": "s1", "from_version": 0, "to_version": 1}]
    assert table.num_rows == 1
