"""Both insert routes feed the periodic insert summary."""

import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi.testclient import TestClient

from acquirium.internals.models import compute_ref_uri
from acquirium.Server.app import app as api
from acquirium.Server.insert_stats import insert_stats
from acquirium.Server.manager import Manager


def test_json_and_arrow_inserts_are_counted(tmp_path, monkeypatch):
    manager = Manager(data_dir=tmp_path, exact_only=True)
    try:
        manager.insert_graph(
            "".join(f'<{compute_ref_uri("plant", name)}> <urn:acquirium#sourceId> "plant" ; '
                    f'<urn:acquirium#refName> "{name}" .\n' for name in ("a", "b")),
            source_id="plant",
        )
        monkeypatch.setattr(api.state, "manager", manager, raising=False)
        insert_stats.snapshot_and_reset()
        http = TestClient(api)
        try:
            response = http.post("/insert_timeseries", params={"wait": 0}, json=[{
                "source_id": "plant", "ref_name": "a",
                "values": [[1, 1.0], [2, 2.0]], "request_id": "json",
            }])
            assert response.status_code == 200, response.text

            table = pa.table({
                "source_id": ["plant"] * 3,
                "ts": pa.array([datetime(2026, 1, 1, second=s, tzinfo=timezone.utc) for s in range(3)],
                               pa.timestamp("us", tz="UTC")),
                "ref_name": ["a", "b", "b"],
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
