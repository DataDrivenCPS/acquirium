"""The v0.4.0 kind contract on Escapement's ingestion and read paths."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from acquirium.internals.models import compute_stream_uri


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


def test_undeclared_text_stream_switches_to_numeric_later(own_acquirium):
    acq = own_acquirium
    source = "kind-late"
    registration = {"source_id": source, "stream_name": "late"}
    acq.register_streams([registration])
    at = datetime(2026, 11, 1, tzinfo=timezone.utc)
    stream = acq.stream(source, "late")
    acq.insert_timeseries(source, "late", [(at, "Bad")])
    assert stream.dataframe()["value"].to_list() == ["Bad"]

    acq.insert_timeseries(source, "late", [(at + timedelta(seconds=1), " 3.0 ")])
    acq.register_streams([registration])
    assert stream.dataframe()["value"].to_list() == [3.0]
    assert stream.dataframe(value_mode="text")["value"].to_list() == ["Bad"]
