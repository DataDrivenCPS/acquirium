"""Dedicated service execution and snapshot semantics."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Materialization.service_runtime import ServiceSupervisor
from acquirium.Materialization.services import ChangeHint
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


class Dashboard:
    received: list[tuple[str, object]] = []
    last_context = None

    def on_change(self, change, context) -> None:
        Dashboard.last_context = context
        Dashboard.received.append((change.token, context.snapshot(["urn:input"])))


def test_service_executes_coalesced_hint_and_reports_restart_health(tmp_path):
    Dashboard.received.clear()
    Dashboard.last_context = None
    store = MaterializationDuckDB(DuckDBStore(tmp_path / "service.duckdb", recreate=True))
    definition = MaterializationDefinition("dashboard", "digest", "test_service_runtime:Dashboard", kind="service")
    store.register_definition(definition)
    store.register_service("dashboard", definition.definition_id)
    snapshots = []
    supervisor = ServiceSupervisor(store, lambda refs, since=None: snapshots.append(refs) or {"current": True})
    try:
        assert supervisor.start("dashboard").status == "running"
        now = datetime.now(timezone.utc)
        store.coalesce_service_hint(ChangeHint("dashboard", "first", {"urn:input": 1}, None, now))
        store.coalesce_service_hint(ChangeHint("dashboard", "latest", {"urn:input": 2}, 3, now))
        assert supervisor.run_next()
        assert Dashboard.received == [("latest", {"current": True})]
        assert snapshots == [("urn:input",)]
        assert store.service("dashboard").health == "healthy"
        supervisor.restore()
        store.coalesce_service_hint(ChangeHint("dashboard", "after-restart", {"urn:input": 3}, 3, now))
        assert supervisor.run_next()
        assert Dashboard.received[-1][0] == "after-restart"
    finally:
        supervisor.close()


def test_service_context_has_no_materialized_output_writer(tmp_path):
    store = MaterializationDuckDB(DuckDBStore(tmp_path / "service.duckdb", recreate=True))
    definition = MaterializationDefinition("dashboard", "digest", "test_service_runtime:Dashboard", kind="service")
    store.register_definition(definition)
    store.register_service("dashboard", definition.definition_id)
    supervisor = ServiceSupervisor(store, lambda refs, since=None: pa.table({}))
    try:
        supervisor.start("dashboard")
        now = datetime.now(timezone.utc)
        store.coalesce_service_hint(ChangeHint("dashboard", "hint", {}, None, now))
        assert supervisor.run_next()
        assert Dashboard.last_context is not None
        assert not hasattr(Dashboard.last_context, "commit_replacement")
        assert not hasattr(Dashboard.last_context, "publish")
    finally:
        supervisor.close()


class CrashingService:
    calls = 0
    fail = True

    def on_change(self, change, context) -> None:
        type(self).calls += 1
        if type(self).fail:
            raise ValueError("boom")


def test_failing_service_is_stopped_not_retried_forever(tmp_path):
    CrashingService.calls = 0
    CrashingService.fail = True
    store = MaterializationDuckDB(DuckDBStore(tmp_path / "service-fail.duckdb", recreate=True))
    definition = MaterializationDefinition("crasher", "digest", "test_service_runtime:CrashingService", kind="service")
    store.register_definition(definition)
    store.register_service("crasher", definition.definition_id)
    supervisor = ServiceSupervisor(store, lambda refs, since=None: pa.table({}))
    try:
        supervisor.start("crasher")
        now = datetime.now(timezone.utc)
        store.coalesce_service_hint(ChangeHint("crasher", "hint", {"urn:input": 1}, None, now))
        # One failure stops the service instead of re-running it every tick.
        assert not supervisor.run_next()
        record = store.service("crasher")
        assert record.status == "failed"
        assert "ValueError" in record.health
        assert CrashingService.calls == 1
        # The hint is retained, and a stopped service is not retried.
        assert store.next_service_hint("crasher") is not None
        assert not supervisor.run_next()
        assert CrashingService.calls == 1
        # After the bug is fixed, an explicit restart redelivers the retained hint.
        CrashingService.fail = False
        supervisor.start("crasher")
        assert supervisor.run_next()
        assert CrashingService.calls == 2
        assert store.next_service_hint("crasher") is None
        assert store.service("crasher").status == "running"
    finally:
        supervisor.close()


def test_service_snapshot_returns_only_latest_row_per_stream(tmp_path):
    store = DuckDBStore(tmp_path / "service-latest.duckdb", recreate=True)
    try:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        PublicationDuckDB(store).publish(PublicationRequest("data", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base.replace(day=2), "numeric_value": 2.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base.replace(day=3), "numeric_value": 3.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:b", "ts": base, "numeric_value": 10.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:b", "ts": base.replace(day=2), "numeric_value": 20.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        versions, inputs = MaterializationDuckDB(store).service_input_snapshot(["urn:a", "urn:b"])
        rows = list(zip(inputs.column("ref_uri").to_pylist(), inputs.column("numeric_value").to_pylist()))
        # Only the newest row of each stream, regardless of retained history.
        assert rows == [("urn:a", 3.0), ("urn:b", 20.0)]
        assert versions == {"urn:a": 1, "urn:b": 1}
    finally:
        store.close()


def test_service_snapshot_window_returns_rows_since(tmp_path):
    store = DuckDBStore(tmp_path / "service-window.duckdb", recreate=True)
    try:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        PublicationDuckDB(store).publish(PublicationRequest("data", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base.replace(day=2), "numeric_value": 2.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:a", "ts": base.replace(day=3), "numeric_value": 3.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        # A window from day 2 onward returns those rows, oldest first.
        _, windowed = runtime.service_input_snapshot(["urn:a"], since=base.replace(day=2))
        rows = list(zip(windowed.column("ts").to_pylist(), windowed.column("numeric_value").to_pylist()))
        assert rows == [(base.replace(day=2), 2.0), (base.replace(day=3), 3.0)]
        # The default remains bounded to the single latest row.
        _, latest = runtime.service_input_snapshot(["urn:a"])
        assert latest.column("numeric_value").to_pylist() == [3.0]
    finally:
        store.close()


def test_snapshot_token_distinguishes_window():
    from acquirium.Materialization.services import snapshot_token
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    versions = {"urn:a": 3}
    default = snapshot_token(versions, 7)
    windowed = snapshot_token(versions, 7, since=base)
    assert default != windowed
    assert windowed == snapshot_token(versions, 7, since=base)
