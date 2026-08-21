"""Dedicated service execution and snapshot semantics."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Materialization.service_runtime import ServiceSupervisor
from acquirium.Materialization.services import ChangeHint
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB


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
    supervisor = ServiceSupervisor(store, lambda refs: snapshots.append(refs) or {"current": True})
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
    supervisor = ServiceSupervisor(store, lambda refs: pa.table({}))
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
