from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from acquirium.Materialization.definitions import MaterializationDefinition
from acquirium.Materialization.services import ServiceRecord
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
