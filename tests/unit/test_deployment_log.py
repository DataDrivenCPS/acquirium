"""The deployment log in the time-series database."""
from __future__ import annotations

import pytest

from acquirium.internals.intervals import NEG_INF, POS_INF, now_micros
from acquirium.Materialization.deployments import DeploymentLog, deployment
from acquirium.Storage.duckdb_store import DuckDBStore

DAY = 86_400 * 1_000_000


def test_record_and_fold(tmp_path):
    log = DeploymentLog(DuckDBStore(tmp_path / "log.duckdb"))
    now = now_micros()
    first = log.record([("v", "A", now - 10 * DAY, POS_INF)])
    second = log.record([("v", None, now - 5 * DAY, POS_INF), ("w", "B", NEG_INF, POS_INF)])
    assert (first, second) == (1, 2) and log.head() == 2
    assert deployment(log.changes(), "v") == [(now - 10 * DAY, now - 5 * DAY, "A")]
    assert log.current() == {"w": "B"}
    assert log.current(at=now - 7 * DAY) == {"v": "A", "w": "B"}


def test_future_endpoints_and_empty_intervals_are_rejected(tmp_path):
    log = DeploymentLog(DuckDBStore(tmp_path / "log.duckdb"))
    now = now_micros()
    with pytest.raises(ValueError, match="future"):
        log.record([("v", "A", now + DAY, POS_INF)])
    with pytest.raises(ValueError, match="future"):
        log.record([("v", "A", now - DAY, now + DAY)])
    with pytest.raises(ValueError, match="precede"):
        log.record([("v", "A", now - DAY, now - DAY)])
    assert log.head() == 0


def test_the_former_current_state_table_is_adopted_over_all_time(tmp_path):
    store = DuckDBStore(tmp_path / "log.duckdb")
    with store._lock, store._write_conn() as conn:
        conn.execute("CREATE TABLE materialization_deployments (name VARCHAR PRIMARY KEY, deployment_json VARCHAR NOT NULL)")
        conn.execute("INSERT INTO materialization_deployments VALUES ('a', '{}'), ('b', '{\"x\": 1}')")
    log = DeploymentLog(store)
    assert deployment(log.changes(), "a") == [(NEG_INF, POS_INF, "{}")]
    assert log.current() == {"a": "{}", "b": '{"x": 1}'}
    with store._own_conn() as conn:
        tables = {row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    assert "materialization_deployments" not in tables
    # reopening runs no migration twice
    assert DeploymentLog(store).current() == {"a": "{}", "b": '{"x": 1}'}
