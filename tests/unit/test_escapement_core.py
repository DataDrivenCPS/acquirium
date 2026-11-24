from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import time

import polars as pl
import pyarrow as pa
import pytest
from escapement import Closed, Refused

from acquirium.core import Core, SCHEMA
from tests.epoch_helpers import plan, readings, value


def frame(times, values, stream="urn:sensor"):
    return pl.DataFrame(
        {
            "stream_uri": [stream] * len(times),
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


@pytest.fixture
def epoch_core(tmp_path):
    owner = Core(tmp_path / "epoch.duckdb", module_paths=[str(tmp_path)], batch_rows=2)
    try:
        yield owner
    finally:
        owner.close()


def test_epoch_arrow_transport_preserves_selected_limits_and_pinned_reads(epoch_core):
    core = epoch_core
    old = core.wait(core.ingest("gateway", "initial", frame([1001, 2002, 3003], [1, "Bad", 3])).receipt)
    pages = core.batches("urn:sensor", order="desc", limit=2, batch_size=1)
    latest = core.wait(core.ingest(
        "gateway", "replace", frame([1001], [99]), replace=["urn:sensor"]
    ).receipt)

    batches = list(pages)
    assert [batch.num_rows for batch in batches] == [1, 1]
    table = pa.Table.from_batches(batches)
    assert table.schema == SCHEMA
    assert table["ts"].cast(pa.int64()).to_pylist() == [3003, 1001]
    assert table["value"].to_pylist() == [3, 1]
    assert next(core.batches("urn:sensor", mode="text", epoch=old.epoch))["value"].to_pylist() == ["Bad"]
    assert next(core.batches("urn:sensor", epoch=latest.epoch))["value"].to_pylist() == [99]
    assert core.info(["urn:none", "urn:sensor"], snapshot=old) == {
        "urn:none": {"row_count": 0, "earliest": None, "latest": None},
        "urn:sensor": {
            "row_count": 3,
            "earliest": "1970-01-01T00:00:00.000001001+00:00",
            "latest": "1970-01-01T00:00:00.000003003+00:00",
        },
    }
    assert next(core.batches("urn:none")).num_rows == 0
    with pytest.raises(ValueError, match="not published"):
        core.batches("urn:sensor", epoch=latest.epoch + 1)


def test_epoch_arrow_transport_repairs_and_follows_versions(epoch_core, tmp_path):
    core = epoch_core
    module = tmp_path / "passthrough.py"
    module.write_text(
        'def calculate(frame, context):\n'
        '    return {"out": frame.select("time", "value", "text")}\n',
        encoding="utf-8",
    )
    core.wait(core.activate("application", plan(module)))
    old = core.wait(core.ingest("gateway", "initial", frame([1001, 2002], [10, 20])).receipt)
    assert next(core.batches("urn:derived"))["value"].to_pylist() == [10, 20]

    correction = frame([2002, 1001, 1001], [None, 15, 30]).with_columns(
        pl.Series("operation", ["delete", "upsert", "upsert"])
    )
    receipt = core.ingest("gateway", "correction", correction).receipt
    latest = core.wait(receipt)
    assert core.ingest("gateway", "correction", correction[[2, 0]]).receipt == receipt
    assert next(core.batches("urn:derived"))["value"].to_pylist() == [30]
    pinned = core.engine.snapshot()
    frontier = pinned.epoch
    reader = pa.RecordBatchReader.from_stream(pinned.versions(["urn:derived"], after=old.epoch))
    core.wait(core.clear("gateway", "clear", ["urn:sensor"]))
    assert next(core.batches("urn:derived")).num_rows == 0
    assert frontier == latest.epoch
    with reader:
        table = reader.read_all()
    assert table["number"].to_pylist() == [10, 30, 20]
    assert table["until"].to_pylist() == [latest.epoch, None, latest.epoch]


def test_epoch_store_reopens_with_receipts_and_enforces_retention(tmp_path):
    path = tmp_path / "epoch.duckdb"
    core = Core(path)
    try:
        original = frame([1001], [1])
        receipt = core.ingest("gateway", "one", original).receipt
        before = core.wait(receipt).epoch
        latest = core.wait(core.ingest("gateway", "two", frame([1001], [2])).receipt).epoch
        assert core.engine.prune(latest) == latest
        assert core.close()
    finally:
        core.close()

    core = Core(path)
    try:
        assert next(core.batches("urn:sensor"))["value"].to_pylist() == [2]
        assert core.engine.epochs_of([receipt]) == [before]
        with pytest.raises(Refused, match="retention"):
            core.batches("urn:sensor", epoch=before)
        with pytest.raises(Refused, match="retention"):
            core.engine.snapshot().versions(after=before)
    finally:
        core.close()


def test_configured_retention_advances_and_drops_expired_history(tmp_path):
    core = Core(tmp_path / "retention.duckdb", history_epochs=1)
    try:
        first = core.wait(core.ingest("gateway", "first", frame([1001], [1])).receipt).epoch
        latest = core.wait(core.ingest("gateway", "latest", frame([1001], [2])).receipt).epoch
        deadline = time.monotonic() + 5
        while core.engine.status()["retention"] < latest:
            assert time.monotonic() < deadline
            time.sleep(0.02)
        assert value(core.engine.snapshot(), "urn:sensor", 1001) == 2
        with pytest.raises(Refused, match="retention"):
            core.batches("urn:sensor", epoch=first)
        with pytest.raises(Refused, match="ordinals"):
            core.changes(epoch=latest)
        assert core.changes(epoch=latest, ordinal=2**64 - 1)["changes"] == []
    finally:
        core.close()


def test_restart_restores_readings_and_native_retry_identity(tmp_path):
    path = tmp_path / "persist.duckdb"
    owner = Core(path)
    payload = frame([1001], [20])
    receipt = owner.ingest("gateway", "batch", payload).receipt
    owner.wait(receipt)
    sequence = receipt.sequence
    owner.close()
    owner = Core(path)
    try:
        assert owner.ingest("gateway", "batch", payload).receipt.sequence == sequence
        with pytest.raises(Refused):
            owner.ingest("gateway", "batch", frame([1001], [21]))
        assert value(owner.engine.snapshot(), "urn:sensor", 1001) == 20
    finally:
        owner.close()


def test_concurrent_ingests_share_admission_commits(core):
    def send(sender):
        return [core.ingest(f"gateway-{sender}", str(i), frame([sender * 1000 + i], [i])).receipt
                for i in range(20)]

    with ThreadPoolExecutor(8) as pool:
        receipts = [receipt for sent in pool.map(send, range(8)) for receipt in sent]
    assert sorted(receipt.sequence for receipt in receipts) == list(range(1, 161))
    status = core.engine.status()
    assert status["accepted"] == 160
    assert status["admissions"] < 160


def test_close_lets_running_submissions_finish_and_refuses_later_ones(tmp_path):
    path = tmp_path / "core.duckdb"
    owner = Core(path)
    receipts = []

    def send(sender):
        for i in range(10**6):
            try:
                receipts.append(owner.ingest(f"gateway-{sender}", str(i), frame([i], [i])).receipt)
            except Closed:
                return

    with ThreadPoolExecutor(4) as pool:
        senders = [pool.submit(send, sender) for sender in range(4)]
        time.sleep(0.3)
        assert owner.close()
        for sender in senders:
            sender.result(timeout=30)
    with pytest.raises(Closed):
        owner.ingest("gateway", "late", frame([1], [1]))
    # Every request accepted before close returned was drained by it.
    reopened = Core(path)
    try:
        status = reopened.engine.status()
        assert receipts and status["accepted"] == status["applied"] == len(receipts)
    finally:
        reopened.close()


STUCK_CLOSE = """
import sys, time
import polars as pl
from pathlib import Path
from acquirium.core import Core
from tests.epoch_helpers import plan
from tests.unit.test_escapement_core import frame

path, module, flag = map(Path, sys.argv[1:])
core = Core(path, module_paths=[str(module.parent)])
core.wait(core.activate("deploy", plan(module, output="urn:out")))
open(flag, "w").close()
core.ingest("gateway", "one", frame([1], [10]))
core.ingest("gateway", "two", frame([2], [20]))
started = time.monotonic()
print(core.close(timeout=0.5), time.monotonic() - started)
"""


def test_close_gives_up_on_a_stuck_function_and_keeps_its_work(tmp_path):
    import subprocess
    import sys

    path, module, flag = tmp_path / "core.duckdb", tmp_path / "calculation.py", tmp_path / "stuck"
    module.write_text(
        "import pathlib, time\n"
        "def calculate(frame, context):\n"
        f"    while pathlib.Path({str(flag)!r}).exists():\n"
        "        time.sleep(0.05)\n"
        '    return {"out": frame.select("time", "value", "text")}\n'
    )
    # The abandoned engine keeps its lock until its process exits, so the
    # stuck close runs in a process of its own.
    try:
        child = subprocess.run(
            [sys.executable, "-c", STUCK_CLOSE, str(path), str(module), str(flag)],
            capture_output=True, text=True, timeout=60,
        )
    finally:
        flag.unlink(missing_ok=True)  # Lets the abandoned worker return and exit.
    assert child.returncode == 0, child.stderr
    closed, elapsed = child.stdout.split()
    assert closed == "False" and float(elapsed) < 5
    owner = Core(path)
    try:
        snapshot = owner.wait(owner.ingest("gateway", "three", frame([3], [30])).receipt)
        assert [value(snapshot, "urn:out", time) for time in (1, 2, 3)] == [10, 20, 30]
        assert owner.close(timeout=10)
    finally:
        owner.close()


def test_pages_preserve_nanoseconds_and_return_more_than_ten_thousand(core):
    core.wait(core.ingest("gateway", "a", frame(list(range(10_000)), [1] * 10_000)).receipt)
    core.wait(core.ingest("gateway", "b", frame(list(range(10_000, 10_007)), [2] * 7)).receipt)
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
    snapshot = core.wait(core.ingest("gateway", "kinds", pl.concat([numeric, text])).receipt)
    # Numbers (and numeric strings) are numbers on a numeric stream, anything
    # else is kept as text, and rows with no reading are dropped on either kind.
    assert readings(snapshot, "urn:sensor") == [20, 21.5, "Bad", 1.0]
    assert readings(snapshot, "urn:status") == ["20", "21.5"]
    read = lambda mode: pa.Table.from_batches(list(core.batches("urn:sensor", mode=mode)))
    assert read("numeric")["value"].to_pylist() == [20, 21.5, 1.0]
    assert read("text")["value"].to_pylist() == ["Bad"]
    assert read("coalesce")["value"].to_pylist() == ["20.0", "21.5", "Bad", "1.0"]
    assert read("text").schema.field("value").type == pa.string()
    assert pa.Table.from_batches(list(core.batches("urn:none", mode="text"))).num_rows == 0


def test_duplicate_key_last_wins_and_transport_order_does_not_change_retry(core):
    receipt = core.ingest("gateway", "batch", frame([1001, 2002, 1001], [10, 20, 30])).receipt
    core.wait(receipt)
    assert core.ingest("gateway", "batch", frame([2002, 1001], [20, 30])).receipt == receipt
    assert value(core.engine.snapshot(), "urn:sensor", 1001) == 30


@pytest.mark.parametrize("operations, values, expected", [
    (["upsert", "delete"], [10, None], None),
    (["delete", "upsert"], [None, 20], 20),
    (["delete", "upsert", "delete"], [None, 20, None], None),
    (["upsert", "delete", "upsert"], [10, None, 30], 30),
    # Rows with no reading remain absent from normalization.
    (["upsert", "upsert"], [10, None], 10),
    (["delete", "upsert"], [None, None], None),
])
def test_mixed_mutations_keep_the_last_surviving_operation(core, operations, values, expected):
    core.wait(core.ingest("gateway", "seed", frame([1001], [5])).receipt)
    mixed = frame([1001] * len(values), values).with_columns(pl.Series("operation", operations))
    receipt = core.ingest("gateway", "mixed", mixed).receipt
    assert value(core.wait(receipt), "urn:sensor", 1001) == expected
    canonical = frame([1001], [expected]).with_columns(
        pl.lit("delete" if expected is None else "upsert").alias("operation")
    )
    assert core.ingest("gateway", "mixed", canonical).receipt == receipt


def test_snapshot_is_captured_before_iterator_is_consumed(core):
    core.wait(core.ingest("gateway", "a", frame([1001], [10])).receipt)
    pages = core.batches("urn:sensor")
    core.wait(core.ingest("gateway", "b", frame([1001], [20])).receipt)
    assert next(pages)["value"].to_pylist() == [10]


def test_historical_epoch_read(core):
    old = core.wait(core.ingest("gateway", "a", frame([1001], [10])).receipt)
    core.wait(core.ingest("gateway", "b", frame([1001], [20])).receipt)
    assert next(core.batches("urn:sensor", epoch=old.epoch))["value"].to_pylist() == [
        10
    ]
    with pytest.raises(ValueError):
        core.batches("urn:sensor", epoch=old.epoch + 100)


def test_fixed_buckets_repair_complete_groups_and_clear_downstream(epoch_core, tmp_path):
    core = epoch_core
    module = tmp_path / "mean.py"
    module.write_text(
        'import polars as pl\n'
        'def calculate(frame, ctx):\n'
        '    return {"out": frame.select(pl.lit(ctx["anchor_ns"]).cast(pl.Datetime("ns", "UTC")).alias("time"), pl.col("value").mean()).drop_nulls("value")}\n',
        encoding="utf-8",
    )
    first = plan(module, name="bucket", output="urn:bucket", width=1000)
    second = plan(module, name="child", source="urn:bucket", output="urn:child")
    core.wait(core.activate("deploy-buckets", {key: first[key] + second[key] for key in first}))
    snapshot = core.wait(
        core.ingest(
            "gateway",
            "initial-buckets",
            frame([-1500, -1, 0, 999, 1000], [10, 20, 30, 40, 50]),
        ).receipt
    )
    assert value(snapshot, "urn:bucket", -2000) == 10
    assert value(snapshot, "urn:bucket", -1000) == 20
    assert value(snapshot, "urn:child", 0) == 35
    assert value(snapshot, "urn:child", 1000) == 50
    snapshot = core.wait(core.ingest("gateway", "late", frame([999], [70])).receipt)
    assert value(snapshot, "urn:child", 0) == 50
    deletion = frame([0, 999], [None, None]).with_columns(
        pl.lit("delete").alias("operation")
    )
    snapshot = core.wait(core.ingest("gateway", "clear-bucket", deletion).receipt)
    assert value(snapshot, "urn:bucket", 0) is None
    assert value(snapshot, "urn:child", 0) is None
    assert value(snapshot, "urn:child", 1000) == 50


def test_timestamp_inputs_assume_utc_and_preserve_nanoseconds():
    from datetime import datetime, timezone

    from acquirium.core import time_ns

    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stamp = time_ns(instant)
    assert time_ns(instant.replace(tzinfo=None)) == stamp
    assert time_ns("2026-01-01") == stamp
    assert time_ns("2026-01-01T00:00:00.000000001") == stamp + 1
    assert time_ns("2026-01-01T01:00:00.000000001+01:00") == stamp + 1


def test_iso_timestamps_reject_nanosecond_wraparound():
    from acquirium.core import time_ns

    assert time_ns("1677-09-21T00:12:43.145224192Z") == -(2**63)
    assert time_ns("2262-04-11T23:47:16.854775807Z") == 2**63 - 1
    for timestamp in ("1677-09-21T00:12:43.145224191Z", "2262-04-11T23:47:16.854775808Z", "2500-01-01"):
        with pytest.raises(ValueError, match="signed UTC nanoseconds"):
            time_ns(timestamp)
