from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Materialization.services import ServiceRecord
from acquirium.Storage.publication.types import PublicationReceipt
from acquirium.Server.manager import Manager


def _manager(materialization) -> Manager:
    manager = Manager.__new__(Manager)
    manager.materialization = materialization
    manager.graph_store = MagicMock()
    manager.graph_store.graph_status.return_value = {"published_version": 7}
    return manager


def test_safety_scan_recreates_only_missing_service_hints():
    storage = MagicMock()
    storage.all_stream_versions.return_value = {"urn:input": 4}
    storage.services_needing_hint.return_value = ("dashboard",)
    manager = _manager(storage)
    Manager.service_safety_scan(manager)
    hint = storage.coalesce_service_hint.call_args.args[0]
    assert hint.service_name == "dashboard"
    assert hint.data_versions == {"urn:input": 4}
    assert hint.graph_revision == 7


def test_publish_notifies_services_with_complete_version_vector():
    manager = _manager(MagicMock())
    manager.materialization.all_stream_versions.return_value = {"urn:a": 2, "urn:b": 7}
    manager.publication = MagicMock()
    manager.publication.publish.return_value = PublicationReceipt(
        "publication", "digest", 1, {"urn:a": 2}
    )
    manager.epoch_materialization = MagicMock()
    manager.notify_service_changes = MagicMock()
    mutations = pa.table({})
    Manager.publish(manager, mutations, publication_id="publication")
    manager.notify_service_changes.assert_called_once_with({"urn:a": 2})


def test_change_notification_expands_partial_hint_to_complete_version_vector():
    storage = MagicMock()
    storage.all_stream_versions.return_value = {"urn:a": 2, "urn:b": 7}
    storage.services.return_value = (
        ServiceRecord("dashboard", "definition", "running", "healthy", datetime.now(timezone.utc)),
    )
    manager = _manager(storage)
    Manager.notify_service_changes(manager, {"urn:a": 2})
    hint = storage.coalesce_service_hint.call_args.args[0]
    assert hint.data_versions == {"urn:a": 2, "urn:b": 7}


def test_service_snapshot_returns_current_authoritative_vector_and_token():
    storage = MagicMock()
    values = pa.table({"ref_uri": ["urn:input"],
        "ts": pa.array([datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        "numeric_value": [42.0], "text_value": [None]})
    storage.service_input_snapshot.return_value = ({"urn:input": 4}, values)
    storage.stream_versions.return_value = {"urn:input": 4}
    manager = _manager(storage)
    snapshot = Manager.service_snapshot(manager, ("urn:input",))
    assert snapshot.data_versions == {"urn:input": 4}
    assert snapshot.inputs == values
    assert len(snapshot.token) == 64


def test_service_registration_rejects_materialized_output_ownership():
    storage = MagicMock()
    manager = _manager(storage)
    definition = MaterializationDefinition("dashboard", "digest", "demo:Dashboard", kind="service", outputs={"out": "urn:derived"})
    with pytest.raises(ValueError, match="cannot declare"):
        Manager.register_service(manager, definition)


def test_registered_value_kind_gates_resync_on_graph_revision():
    manager = Manager.__new__(Manager)
    manager.timescale = MagicMock()
    manager.timescale.stream_value_kind.return_value = None  # never registered
    manager.graph_store = MagicMock()
    manager.graph_store.graph_status.return_value = {"published_version": 3}
    manager._sync_stream_refs_from_graph = MagicMock()
    manager._refs_synced_revision = None
    with pytest.raises(ValueError, match="not registered"):
        Manager._registered_value_kind(manager, "urn:missing")
    assert manager._sync_stream_refs_from_graph.call_count == 1
    assert manager._refs_synced_revision == 3
    # Same graph revision: a second unknown ref must not rebuild the graph.
    with pytest.raises(ValueError, match="not registered"):
        Manager._registered_value_kind(manager, "urn:missing-again")
    assert manager._sync_stream_refs_from_graph.call_count == 1
    # Graph advanced: the rebuild runs again.
    manager.graph_store.graph_status.return_value = {"published_version": 4}
    with pytest.raises(ValueError, match="not registered"):
        Manager._registered_value_kind(manager, "urn:missing")
    assert manager._sync_stream_refs_from_graph.call_count == 2
