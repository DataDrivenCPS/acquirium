from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
import pytest
from escapement import Input, Operator, Plan, Pointwise, RetryConflictError

from acquirium.core import Core, SCHEMA


def frame(times, values, stream="urn:sensor"):
    return pl.DataFrame(
        {
            "ref_uri": [stream] * len(times),
            "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
            "value": pl.Series(values, dtype=pl.Object),
        }
    )


@pytest.fixture
def core(tmp_path):
    owner = Core(tmp_path / "core.duckdb")
    try:
        yield owner
    finally:
        owner.close()


def test_native_atomic_dag_correction_deletion_and_pinned_snapshot(core):
    plan = Plan(
        "one",
        [
            Operator(
                "mean",
                [Input("urn:sensor", "temperature")],
                {"mean": "urn:mean"},
                Pointwise(),
                {"kind": "mean", "port": "mean", "per_stream": False},
            )
        ],
    )
    core.wait(core.activate("deploy", plan))
    receipt = core.ingest("gateway", "initial", frame([1001, 2002], [20, 25]))
    pinned = core.wait(receipt)
    assert pinned.value("urn:mean", 1001) == 20
    assert pinned.value("urn:sensor", 1001) == 20
    core.wait(core.ingest("gateway", "correction", frame([1001], [30])))
    assert core.engine.snapshot().value("urn:mean", 1001) == 30
    assert pinned.value("urn:mean", 1001) == 20
    deletion = frame([1001], [None]).with_columns(pl.lit("delete").alias("operation"))
    core.wait(core.ingest("gateway", "delete", deletion))
    assert core.engine.snapshot().value("urn:mean", 1001) is None
    versions = core.engine.versions("urn:mean")
    assert [item.value for item in versions if item.event_time_ns == 1001] == [20, 30]


def test_restart_restores_readings_and_native_retry_identity(tmp_path):
    path = tmp_path / "persist.duckdb"
    owner = Core(path)
    payload = frame([1001], [20])
    receipt = owner.ingest("gateway", "batch", payload)
    owner.wait(receipt)
    sequence = receipt.sequence
    owner.close()
    owner = Core(path)
    try:
        assert owner.ingest("gateway", "batch", payload).sequence == sequence
        with pytest.raises(RetryConflictError):
            owner.ingest("gateway", "batch", frame([1001], [21]))
        assert owner.engine.snapshot().value("urn:sensor", 1001) == 20
    finally:
        owner.close()


def test_pages_preserve_nanoseconds_and_return_more_than_ten_thousand(core):
    core.wait(core.ingest("gateway", "a", frame(list(range(10_000)), [1] * 10_000)))
    core.wait(core.ingest("gateway", "b", frame(list(range(10_000, 10_007)), [2] * 7)))
    batches = list(core.batches("urn:sensor", batch_size=307))
    table = pa.Table.from_batches(batches)
    assert table.schema == SCHEMA
    assert table.num_rows == 10_007
    assert table["ts"].cast(pa.int64()).to_pylist() == list(range(10_007))
    descending = pa.Table.from_batches(
        list(
            core.batches(
                "urn:sensor", start=7, end=1003, limit=400, order="desc", batch_size=91
            )
        )
    )
    assert descending["ts"].cast(pa.int64()).to_pylist() == list(range(1002, 602, -1))
    assert core.info(["urn:sensor"])["urn:sensor"]["row_count"] == 10_007


def test_values_are_stored_as_their_stream_kind(core):
    values = [20, "21.5", "Bad", True, None, "", float("nan"), float("inf")]
    numeric = frame(list(range(len(values))), values).with_columns(
        pl.lit("numeric").alias("value_kind")
    )
    text_values = [20, "21.5", None, "", "  ", float("nan"), float("inf")]
    text = frame(list(range(len(text_values))), text_values, stream="urn:status").with_columns(
        pl.lit("text").alias("value_kind")
    )
    snapshot = core.wait(core.ingest("gateway", "kinds", pl.concat([numeric, text])))
    # Numbers (and numeric strings) are numbers on a numeric stream, anything
    # else is kept as text, and rows with no reading are dropped on either kind.
    assert [r.value for r in snapshot.readings("urn:sensor")] == [20, 21.5, "Bad", "True"]
    assert [r.value for r in snapshot.readings("urn:status")] == ["20", "21.5"]
    read = lambda mode: pa.Table.from_batches(list(core.batches("urn:sensor", mode=mode)))
    assert read("numeric")["value"].to_pylist() == [20, 21.5]
    assert read("text")["value"].to_pylist() == ["Bad", "True"]
    assert read("coalesce")["value"].to_pylist() == ["20.0", "21.5", "Bad", "True"]
    assert read("text").schema.field("value").type == pa.string()
    assert pa.Table.from_batches(list(core.batches("urn:none", mode="text"))).num_rows == 0


def test_duplicate_key_last_wins_and_transport_order_does_not_change_retry(core):
    receipt = core.ingest("gateway", "batch", frame([1001, 2002, 1001], [10, 20, 30]))
    core.wait(receipt)
    assert core.ingest("gateway", "batch", frame([2002, 1001], [20, 30])) == receipt
    assert core.engine.snapshot().value("urn:sensor", 1001) == 30


def test_snapshot_is_captured_before_iterator_is_consumed(core):
    core.wait(core.ingest("gateway", "a", frame([1001], [10])))
    pages = core.batches("urn:sensor")
    core.wait(core.ingest("gateway", "b", frame([1001], [20])))
    assert next(pages)["value"].to_pylist() == [10]


def test_historical_epoch_read(core):
    old = core.wait(core.ingest("gateway", "a", frame([1001], [10])))
    core.wait(core.ingest("gateway", "b", frame([1001], [20])))
    assert next(core.batches("urn:sensor", epoch=old.epoch))["value"].to_pylist() == [
        10
    ]
    with pytest.raises(ValueError):
        core.batches("urn:sensor", epoch=old.epoch + 100)


def test_fixed_buckets_repair_complete_groups_and_clear_downstream(core):
    from escapement import FixedBucket

    plan = Plan(
        "buckets",
        [
            Operator(
                "bucket",
                [Input("urn:sensor", "temperature")],
                {"mean": "urn:bucket"},
                FixedBucket(1000),
                {"kind": "mean", "port": "mean", "per_stream": False},
            ),
            Operator(
                "child",
                [Input("urn:bucket", "mean")],
                {"mean": "urn:child"},
                Pointwise(),
                {"kind": "mean", "port": "mean", "per_stream": False},
            ),
        ],
    )
    core.wait(core.activate("deploy-buckets", plan))
    snapshot = core.wait(
        core.ingest(
            "gateway",
            "initial-buckets",
            frame([-1500, -1, 0, 999, 1000], [10, 20, 30, 40, 50]),
        )
    )
    assert snapshot.value("urn:bucket", -2000) == 10
    assert snapshot.value("urn:bucket", -1000) == 20
    assert snapshot.value("urn:child", 0) == 35
    assert snapshot.value("urn:child", 1000) == 50
    snapshot = core.wait(core.ingest("gateway", "late", frame([999], [70])))
    assert snapshot.value("urn:child", 0) == 50
    deletion = frame([0, 999], [None, None]).with_columns(
        pl.lit("delete").alias("operation")
    )
    snapshot = core.wait(core.ingest("gateway", "clear-bucket", deletion))
    assert snapshot.value("urn:bucket", 0) is None
    assert snapshot.value("urn:child", 0) is None
    assert snapshot.value("urn:child", 1000) == 50
