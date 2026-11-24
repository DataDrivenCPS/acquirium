"""Insert responses count normalized writes and metrics count submitted rows."""

import pyarrow as pa
import pyarrow.ipc as ipc

from acquirium.Server.insert_stats import insert_stats
from tests.epoch_helpers import epoch_server


def test_json_and_arrow_count_normalized_writes_and_measure_submitted_rows(tmp_path):
    with epoch_server(tmp_path) as (_, http, client):
        client.register_streams([
            {"source_id": "metrics", "stream_name": name, "value_kind": "numeric"}
            for name in ("a", "b")
        ])
        insert_stats.snapshot_and_reset()
        try:
            # The second JSON request contains no reading and counts zero.
            for request_id, rows, expected in (
                ("json", [[1, 1.0], [1, 2.0], [2, None]], 1),
                ("null", [[3, None]], 0),
            ):
                response = http.post("/insert_timeseries", json=[{
                    "source_id": "metrics", "stream_name": "a",
                    "values": rows, "request_id": request_id,
                }])
                assert response.status_code == 200, response.text
                assert response.json()["rows_inserted"] == expected
            assert client.stream("metrics", "a").dataframe()["value"].to_list() == [2.0]

            table = pa.table({
                "source_id": ["metrics"] * 4,
                "stream_name": ["a", "a", "a", "b"],
                "ts": pa.array([1, 1, 3, 4], pa.timestamp("ns", tz="UTC")),
                "value": [3.0, 4.0, None, 5.0],
                # Extra columns are outside the Arrow insert contract.
                "operation": pa.nulls(4, pa.string()),
            })
            buffer = pa.BufferOutputStream()
            with ipc.new_stream(buffer, table.schema) as writer:
                writer.write_table(table)
            response = http.post(
                "/insert_timeseries_arrow", content=buffer.getvalue().to_pybytes(),
                headers={"X-Acquirium-Request-Id": "arrow"},
            )
            assert response.status_code == 200, response.text
            assert response.json()["rows_inserted"] == 2
            assert client.stream("metrics", "a").dataframe()["value"].to_list() == [4.0]
            assert client.stream("metrics", "b").dataframe()["value"].to_list() == [5.0]
        finally:
            summary = insert_stats.snapshot_and_reset()
        assert sorted(summary) == [("arrow", 4, 2), ("http", 4, 1)]
