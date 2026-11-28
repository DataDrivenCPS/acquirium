"""The v0.4.0 kind contract on Escapement's ingestion and read paths."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from acquirium.internals.models import compute_stream_uri
from tests.epoch_helpers import epoch_server


@pytest.mark.parametrize("transport", ["json", "arrow"])
def test_booleans_follow_the_stream_kind(own_acquirium, transport):
    acq = own_acquirium
    source = f"kind-{transport}"
    at = datetime(2026, 11, 1, tzinfo=timezone.utc)
    names = ["numeric", "text", "inferred"]
    acq.register_streams(
        [
            {
                "source_id": source,
                "stream_name": name,
                **({"value_kind": name} if name != "inferred" else {}),
            }
            for name in names
        ]
    )

    if transport == "arrow":
        acq.insert_timeseries_arrow(
            source,
            pa.table(
                {
                    "ts": [at, at + timedelta(seconds=1)] * len(names),
                    "value": [True, False] * len(names),
                    "stream_name": [name for name in names for _ in range(2)],
                }
            ),
        )
    else:
        acq.insert_timeseries_batch(
            source,
            {name: [(at, True), (at + timedelta(seconds=1), False)] for name in names},
        )

    expected = {
        "numeric": [1.0, 0.0],
        "text": ["True", "False"],
        "inferred": [1.0, 0.0],
    }
    for name, values in expected.items():
        uri = str(compute_stream_uri(source, name))
        assert acq.client.timeseries_df(uri)["value"].to_list() == values


@pytest.mark.parametrize(
    ("transport", "exact", "inexact"),
    [
        pytest.param("json", 2**53 + 2, 2**53 + 1, id="json-mixed"),
        pytest.param("json", -(2**53 + 2), -(2**53 + 1), id="json-negative"),
        pytest.param("json", 2**64 - 2048, 2**64 - 1, id="json-outside-int64"),
        pytest.param("int64", -(2**53 + 2), -(2**53 + 1), id="arrow-signed"),
        pytest.param("uint64", 2**64 - 2048, 2**64 - 1, id="arrow-unsigned"),
    ],
)
def test_numeric_ingest_rejects_inexact_integers_before_admission(tmp_path, transport, exact, inexact):
    with epoch_server(tmp_path) as (_, http, acq):
        source = "precision"
        acq.register_streams([
            {"source_id": source, "stream_name": kind, "value_kind": kind}
            for kind in ("numeric", "text")
        ])

        def write(request_id, numeric, text):
            """Send both stream kinds in the same request."""
            if transport == "json":
                return http.post("/insert_timeseries", json=[
                    {"source_id": source, "stream_name": kind, "request_id": request_id, "values": rows}
                    for kind, rows in (("numeric", [*numeric, (10, "Fault")]), ("text", text))
                ])
            rows = [*numeric, *text]
            table = pa.table({
                "source_id": [source] * len(rows),
                "stream_name": ["numeric"] * len(numeric) + ["text"] * len(text),
                "ts": pa.array([ts for ts, _ in rows], type=pa.timestamp("ns", tz="UTC")),
                "value": pa.array([value for _, value in rows], type=pa.type_for_alias(transport)),
            })
            buffer = pa.BufferOutputStream()
            with pa.ipc.new_stream(buffer, table.schema) as writer:
                writer.write_table(table)
            return http.post("/insert_timeseries_arrow", content=buffer.getvalue().to_pybytes(),
                             headers={"X-Acquirium-Request-Id": request_id})

        numeric = acq.stream(source, "numeric")
        text = acq.stream(source, "text")
        valid = [(1, 2**53), (2, 2**53 + 2), (3, exact)]
        if transport == "json":
            # An identical sequence of digits in a string keeps its parsing rules.
            valid.append((4, str(inexact)))
        response = write("exact", valid, [(1, inexact)])
        assert response.status_code == 200, response.text
        assert response.json()["receipts"][0]["published"]
        expected = [float(value) for _, value in valid]
        assert numeric.dataframe()["value"].to_list() == expected
        assert text.dataframe()["value"].to_list() == [str(inexact)]
        faults = ["Fault"] if transport == "json" else []
        assert numeric.dataframe(value_mode="text")["value"].to_list() == faults

        # The invalid row also prevents the valid rows in its request from publishing.
        status = acq.engine_status()
        response = write("inexact", [(5, 7), (6, inexact)], [(5, exact)])
        assert response.status_code == 400, response.text
        assert response.json()["detail"] == "integer reading cannot be represented exactly as Float64"
        assert acq.engine_status() == status
        assert numeric.dataframe()["value"].to_list() == expected
        assert text.dataframe()["value"].to_list() == [str(inexact)]


def test_read_modes_use_declared_and_inferred_stream_kinds(own_acquirium):
    acq = own_acquirium
    source = "kind-late"
    registration = {"source_id": source, "stream_name": "late"}
    acq.register_streams([
        registration,
        {"source_id": source, "stream_name": "mode", "value_kind": "text"},
    ])
    at = datetime(2026, 11, 1, tzinfo=timezone.utc)
    stream = acq.stream(source, "late")
    later = at + timedelta(seconds=1)
    # The declared text stream stringifies numbers. The other stream starts with a fault code.
    acq.insert_timeseries_batch(source, {"mode": [(at, "AUTO"), (later, 3)], "late": [(at, "Bad")]})
    assert acq.stream(source, "mode").dataframe()["value"].to_list() == ["AUTO", "3"]
    assert stream.dataframe()["value"].to_list() == ["Bad"]

    # The first numeric value changes the default view while the fault code remains readable.
    acq.insert_timeseries(source, "late", [(later, " 70.5 ")])
    acq.register_streams([registration])
    assert stream.dataframe()["value"].to_list() == [70.5]
    assert stream.dataframe(value_mode="text")["value"].to_list() == ["Bad"]
    assert stream.dataframe(value_mode="coalesce")["value"].to_list() == ["Bad", "70.5"]
