"""Unit tests for DriverState, WriteAheadLog, ExponentialBackoff, and WAL
integration in IngestDriver.  No server required."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests.exceptions

from acquirium.DriverState import DriverState, ExponentialBackoff, WriteAheadLog


# ---------------------------------------------------------------------------
# DriverState — key-value store
# ---------------------------------------------------------------------------


def test_kv_get_missing_returns_default(tmp_path):
    s = DriverState(tmp_path)
    assert s.get("missing") is None
    assert s.get("missing", 42) == 42


def test_kv_set_and_get(tmp_path):
    s = DriverState(tmp_path)
    s.set("key", "value")
    assert s.get("key") == "value"


def test_kv_set_various_types(tmp_path):
    s = DriverState(tmp_path)
    s.set("num", 123)
    s.set("lst", [1, 2, 3])
    s.set("nested", {"a": 1})
    assert s.get("num") == 123
    assert s.get("lst") == [1, 2, 3]
    assert s.get("nested") == {"a": 1}


def test_kv_delete(tmp_path):
    s = DriverState(tmp_path)
    s.set("k", "v")
    s.delete("k")
    assert s.get("k") is None


def test_kv_delete_missing_is_noop(tmp_path):
    s = DriverState(tmp_path)
    s.delete("nonexistent")  # must not raise


def test_kv_persistence_across_instances(tmp_path):
    s1 = DriverState(tmp_path)
    s1.set("offset", 99)

    s2 = DriverState(tmp_path)
    assert s2.get("offset") == 99


def test_kv_write_is_atomic(tmp_path):
    """tmp file must not persist after a successful save."""
    s = DriverState(tmp_path)
    s.set("x", 1)
    assert not (tmp_path / "state.tmp").exists()
    assert (tmp_path / "state.json").exists()


# ---------------------------------------------------------------------------
# WriteAheadLog
# ---------------------------------------------------------------------------


def _obs_df(n: int = 3) -> pl.DataFrame:
    now = datetime.now(timezone.utc)
    return pl.DataFrame(
        {
            "source_id": ["src"] * n,
            "ts": [now] * n,
            "ref_name": [f"ref_{i}" for i in range(n)],
            "value": [str(i) for i in range(n)],
        }
    )


def test_wal_initially_empty(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal")
    assert wal.is_empty()
    assert wal.pending() == []


def test_wal_append_creates_file(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal")
    seq = wal.append(_obs_df())
    assert seq == 0
    assert not wal.is_empty()
    assert (tmp_path / "wal" / "00000000.parquet").exists()


def test_wal_pending_returns_ordered_entries(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal")
    wal.append(_obs_df(1))
    wal.append(_obs_df(2))
    wal.append(_obs_df(3))

    pending = wal.pending()
    assert [seq for seq, _ in pending] == [0, 1, 2]
    assert [len(df) for _, df in pending] == [1, 2, 3]


def test_wal_ack_removes_entry(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal")
    seq = wal.append(_obs_df())
    wal.ack(seq)
    assert wal.is_empty()
    assert not (tmp_path / "wal" / "00000000.parquet").exists()


def test_wal_ack_missing_is_noop(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal")
    wal.ack(999)  # must not raise


def test_wal_survives_reconstruction(tmp_path):
    """WAL entries written by one instance are visible to a fresh instance."""
    wal1 = WriteAheadLog(tmp_path / "wal")
    wal1.append(_obs_df(5))

    wal2 = WriteAheadLog(tmp_path / "wal")
    pending = wal2.pending()
    assert len(pending) == 1
    assert len(pending[0][1]) == 5


def test_wal_seq_continues_after_reconstruction(tmp_path):
    wal1 = WriteAheadLog(tmp_path / "wal")
    wal1.append(_obs_df())  # seq=0

    wal2 = WriteAheadLog(tmp_path / "wal")
    seq = wal2.append(_obs_df())  # should be seq=1
    assert seq == 1


# ---------------------------------------------------------------------------
# ExponentialBackoff
# ---------------------------------------------------------------------------


def test_backoff_initially_ready():
    b = ExponentialBackoff()
    assert b.ready()
    assert b.next_delay() == 0.0


def test_backoff_not_ready_after_failure():
    b = ExponentialBackoff(base=10.0, jitter=0.0)
    b.record_failure()
    assert not b.ready()
    assert b.next_delay() > 0.0


def test_backoff_delay_grows_with_failures():
    b = ExponentialBackoff(base=2.0, jitter=0.0)
    b.record_failure()  # delay = 2^1 = 2s
    delay1 = b.next_delay()
    b.record_failure()  # delay = 2^2 = 4s (cumulative, so harder to test directly)
    delay2 = b.next_delay()
    assert delay2 > delay1


def test_backoff_capped_at_max_delay():
    b = ExponentialBackoff(base=2.0, max_delay=5.0, jitter=0.0)
    for _ in range(20):
        b.record_failure()
    assert b.next_delay() <= 5.0 + 0.001  # allow tiny float imprecision


def test_backoff_reset_on_success():
    b = ExponentialBackoff(base=10.0, jitter=0.0)
    b.record_failure()
    assert not b.ready()
    b.record_success()
    assert b.ready()
    assert b.next_delay() == 0.0


# ---------------------------------------------------------------------------
# IngestDriver WAL integration
# ---------------------------------------------------------------------------


def _make_ingest_driver(tmp_path: Path, aq=None):
    """Create a minimal concrete IngestDriver bound to *tmp_path* as state dir."""
    from acquirium.Driver import PollingIngestDriver

    if aq is None:
        aq = MagicMock()
        aq.insert_timeseries_polars.return_value = {"ok": True, "rows_inserted": 3}

    cfg = {"__config_dir": str(tmp_path), "driver": {"state_dir": str(tmp_path / "state")}}

    class _TestDriver(PollingIngestDriver):
        def setup(self):
            self.source_id = "test-src"

        def collect(self):
            return pl.DataFrame(
                {
                    "ts": [datetime.now(timezone.utc)],
                    "ref_name": ["sensor"],
                    "value": ["1.0"],
                }
            )

    return _TestDriver(aq, cfg)


def test_insert_observations_live_success(tmp_path):
    aq = MagicMock()
    aq.insert_timeseries_polars.return_value = {"ok": True, "rows_inserted": 1}
    driver = _make_ingest_driver(tmp_path, aq)
    driver.setup()

    df = pl.DataFrame(
        {"ts": [datetime.now(timezone.utc)], "ref_name": ["x"], "value": ["1"]}
    )
    result = driver.insert_observations(df)
    assert result["ok"]
    assert result["rows_inserted"] == 1
    assert driver._wal.is_empty()


def test_insert_observations_buffers_on_connection_error(tmp_path):
    aq = MagicMock()
    aq.insert_timeseries_polars.side_effect = requests.exceptions.ConnectionError("refused")
    driver = _make_ingest_driver(tmp_path, aq)
    driver.setup()

    df = pl.DataFrame(
        {"ts": [datetime.now(timezone.utc)], "ref_name": ["x"], "value": ["1"]}
    )
    result = driver.insert_observations(df)
    assert result["ok"]
    assert result["rows_inserted"] == 0
    assert result.get("buffered", 0) == 1
    assert not driver._wal.is_empty()


def test_wal_drains_on_next_tick(tmp_path):
    aq = MagicMock()

    # First call fails; subsequent calls succeed.
    aq.insert_timeseries_polars.side_effect = [
        requests.exceptions.ConnectionError("refused"),
        {"ok": True, "rows_inserted": 1},
        {"ok": True, "rows_inserted": 1},
    ]
    driver = _make_ingest_driver(tmp_path, aq)
    driver.setup()

    df = pl.DataFrame(
        {"ts": [datetime.now(timezone.utc)], "ref_name": ["x"], "value": ["1"]}
    )
    # First insert: goes to WAL.
    driver.insert_observations(df)
    assert not driver._wal.is_empty()

    # Force backoff to be ready immediately.
    driver._backoff.record_success()

    # Second insert: WAL drains, then live insert succeeds.
    result = driver.insert_observations(df)
    assert driver._wal.is_empty()
    assert result["ok"]


def test_wal_buffers_new_data_while_server_down(tmp_path):
    aq = MagicMock()
    aq.insert_timeseries_polars.side_effect = requests.exceptions.ConnectionError("refused")
    driver = _make_ingest_driver(tmp_path, aq)
    driver.setup()

    df = pl.DataFrame(
        {"ts": [datetime.now(timezone.utc)], "ref_name": ["x"], "value": ["1"]}
    )
    driver.insert_observations(df)
    driver._backoff.record_failure()  # keep backoff not ready

    # Second call while backoff not ready: new data is also buffered.
    with patch.object(driver._backoff, "ready", return_value=False):
        driver.insert_observations(df)

    pending = driver._wal.pending()
    assert len(pending) == 2


def test_wal_persists_across_driver_restart(tmp_path):
    """WAL entries survive a driver restart (new instance, same state dir)."""
    aq_down = MagicMock()
    aq_down.insert_timeseries_polars.side_effect = requests.exceptions.ConnectionError("refused")

    driver1 = _make_ingest_driver(tmp_path, aq_down)
    driver1.setup()

    df = pl.DataFrame(
        {"ts": [datetime.now(timezone.utc)], "ref_name": ["x"], "value": ["1"]}
    )
    driver1.insert_observations(df)
    assert not driver1._wal.is_empty()

    # New driver instance pointed at same state dir; server is back.
    aq_up = MagicMock()
    aq_up.insert_timeseries_polars.return_value = {"ok": True, "rows_inserted": 1}

    driver2 = _make_ingest_driver(tmp_path, aq_up)
    driver2.setup()
    assert not driver2._wal.is_empty()  # loaded from disk

    driver2._backoff.record_success()  # backoff ready
    driver2.insert_observations(pl.DataFrame(schema={"ts": pl.Datetime("us", "UTC"), "ref_name": pl.Utf8, "value": pl.Utf8}))
    assert driver2._wal.is_empty()
