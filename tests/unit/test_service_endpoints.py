from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

import acquirium.Server.app as server_app
from acquirium.Materialization.services import ServiceRecord


def test_service_registration_lifecycle_and_health_endpoints():
    manager = MagicMock()
    record = ServiceRecord("dashboard", "definition", "running", "healthy", datetime(2026, 1, 1, tzinfo=timezone.utc))
    manager.register_service.return_value = {"name": "dashboard", "definition_id": "definition", "status": "registered", "health": "unknown"}
    manager.start_service.return_value = record
    manager.service_supervisor.stop.return_value = ServiceRecord("dashboard", "definition", "stopped", "unknown", datetime(2026, 1, 1, tzinfo=timezone.utc))
    manager.materialization.service.return_value = record
    server_app.app.state.manager = manager
    client = TestClient(server_app.app)

    response = client.post("/services/register", json={"name": "dashboard", "source_digest": "digest", "entrypoint": "demo:Dashboard"})
    assert response.status_code == 200 and response.json()["status"] == "registered"
    definition = manager.register_service.call_args.args[0]
    assert definition.kind == "service" and definition.outputs is None
    assert client.post("/services/dashboard/start").json()["health"] == "healthy"
    assert client.post("/services/dashboard/stop").json()["status"] == "stopped"
    assert client.get("/services/dashboard").json()["status"] == "running"
