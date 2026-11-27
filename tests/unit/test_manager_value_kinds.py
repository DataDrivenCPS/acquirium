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
