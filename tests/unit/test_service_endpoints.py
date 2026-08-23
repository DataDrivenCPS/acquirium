from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

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
    request = server_app.ServiceRegistration(name="dashboard", source_digest="digest", entrypoint="demo:Dashboard")
    response = server_app.register_service(request)
    assert response["status"] == "registered"
    definition = manager.register_service.call_args.args[0]
    assert definition.kind == "service" and definition.outputs is None
    assert server_app.start_service("dashboard")["health"] == "healthy"
    assert server_app.stop_service("dashboard")["status"] == "stopped"
    assert server_app.service_status("dashboard")["status"] == "running"
