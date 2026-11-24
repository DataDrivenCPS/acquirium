"""Both insert routes feed the periodic insert summary."""

import asyncio
import io
from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi.testclient import TestClient

from acquirium.core import Core
from acquirium.internals.models import StreamInsert, compute_stream_uri
from acquirium.Server.app import app as api, insert_timeseries, insert_timeseries_arrow
from acquirium.Server.insert_stats import insert_stats
from acquirium.Server.manager import Manager


def test_insert_replies_count_normalized_rows_and_project_arrow_columns(tmp_path, monkeypatch):
    manager = Manager.__new__(Manager)
    manager.core = Core(tmp_path / "engine.duckdb")
    ref = str(compute_stream_uri("plant", "a"))
    manager._validate_refs = lambda source, refs: dict.fromkeys(refs, "numeric")
    monkeypatch.setattr(api.state, "manager", manager, raising=False)
    monkeypatch.setattr(insert_stats, "record", lambda **kwargs: None)
    try:
        result = insert_timeseries([
            StreamInsert(source_id="plant", stream_name="a", request_id="json",
                         values=[(1, 1), (1, 2), (2, None)])
        ], wait=5)
        assert result["rows_inserted"] == 1

        table = pa.table({
            "source_id": ["plant"] * 3,
            "stream_name": ["a"] * 3,
            "ts": pa.array([1, 1, 3], pa.timestamp("ns", tz="UTC")),
            "value": [3, 4, None],
            # Extra columns must not influence the ingestion operation.
            "operation": pa.nulls(3, pa.string()),
        })
        buffer = io.BytesIO()
        with ipc.new_stream(buffer, table.schema) as writer:
            writer.write_table(table)

        async def body():
            return buffer.getvalue()

        request = SimpleNamespace(body=body, headers={"X-Acquirium-Request-Id": "arrow"})
        result = asyncio.run(insert_timeseries_arrow(request, wait=5))
        assert result["rows_inserted"] == 1
        assert next(manager.core.batches(ref))["value"].to_pylist() == [4.0]
        result = insert_timeseries([
            StreamInsert(source_id="plant", stream_name="a", request_id="null", values=[(4, None)])
        ], wait=5)
        assert result["rows_inserted"] == 0
    finally:
        manager.core.close()


def test_json_and_arrow_inserts_are_counted(tmp_path, monkeypatch):
    manager = Manager(data_dir=tmp_path, exact_only=True)
    try:
        manager.insert_graph(
            "".join(f'<{compute_stream_uri("plant", name)}> <urn:acquirium#sourceId> "plant" ; '
                    f'<urn:acquirium#refName> "{name}" .\n' for name in ("a", "b")),
            source_id="plant",
        )
        monkeypatch.setattr(api.state, "manager", manager, raising=False)
        insert_stats.snapshot_and_reset()
        http = TestClient(api)
        try:
            response = http.post("/insert_timeseries", params={"wait": 0}, json=[{
                "source_id": "plant", "stream_name": "a",
                "values": [[1, 1.0], [2, 2.0]], "request_id": "json",
            }])
            assert response.status_code == 200, response.text

            table = pa.table({
                "source_id": ["plant"] * 3,
                "ts": pa.array([datetime(2026, 1, 1, second=s, tzinfo=timezone.utc) for s in range(3)],
                               pa.timestamp("us", tz="UTC")),
                "stream_name": ["a", "b", "b"],
                "value": [1.0, 2.0, 3.0],
            })
            buffer = io.BytesIO()
            with ipc.new_stream(buffer, table.schema) as writer:
                writer.write_table(table)
            response = http.post("/insert_timeseries_arrow", params={"wait": 0},
                                 content=buffer.getvalue(),
                                 headers={"Content-Type": "application/vnd.apache.arrow.stream",
                                          "X-Acquirium-Request-Id": "arrow"})
            assert response.status_code == 200, response.text
        finally:
            http.close()
        assert sorted(insert_stats.snapshot_and_reset()) == [("arrow", 3, 2), ("http", 2, 1)]
    finally:
        manager.close()
