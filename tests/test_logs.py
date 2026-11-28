from datetime import datetime, timezone

import pytest

from acquirium.internals.internals_namespaces import ACQUIRIUM_NS, S223, WATR

@pytest.fixture(scope="module")
def pump_model(own_acquirium):
    """Load the pump and its connection points on an isolated server."""
    own_acquirium.insert_graph_file("tests/test_model_nodata.ttl", source_id="plant")
    return own_acquirium


def test_logbook_filters_by_write_time_and_observation_period(pump_model):
    client = pump_model.client
    inlet = "urn:ex/Pump1-in"
    # Write notes out of order. The last note has no observation period.
    notes = [
        ("started", "2024-06-17T12:00:00Z", "2024-06-15T12:00:00Z", "2024-06-15T12:30:00Z"),
        ("checked", "2024-06-16T12:00:00Z", "2024-06-14T12:00:00Z", "2024-06-14T12:30:00Z"),
        ("ran", "2024-06-18T12:00:00Z", "2024-06-14T12:00:00Z", "2024-06-16T12:30:00Z"),
        ("stopped", "2024-06-20T12:00:00Z", "2024-06-15T12:20:00Z", "2024-06-15T12:25:00Z"),
        ("inspected", "2024-06-30T12:00:00Z", None, None),
    ]
    for message, logged, start, end in notes:
        assert client.insert_log(
            property_uri=inlet, log_time=logged, observation_start=start,
            observation_end=end, log_message=message,
        ) == {"ok": True}

    # Reads are ordered by write time. Observation filters use the observed interval.
    filters = [
        ({"log_time_start": "2024-06-17T12:00:00Z"}, ["started", "ran", "stopped", "inspected"]),
        ({"log_time_end": "2024-06-21T11:00:00Z"}, ["checked", "started", "ran", "stopped"]),
        ({"observation_start": "2024-06-14T12:45:00Z"}, ["started", "ran", "stopped"]),
        ({"observation_end": "2023-06-15T12:26:00Z"}, []),
        ({"observation_end": "2024-06-14T12:26:00Z"}, ["checked", "ran"]),
    ]
    for bounds, expected in filters:
        assert [log.message for log in client.query_logs(property_uri=inlet, **bounds)] == expected, bounds

    # Wait for the graph to reflect the logbook link and then its removal.
    query = f"""
        SELECT ?point ?log WHERE {{
            ?point a <{S223}InletConnectionPoint> ; <{ACQUIRIUM_NS}hasLog> ?log .
            ?log a <{ACQUIRIUM_NS}Logbook> .
        }}
    """
    assert client.sparql_query(query, wait_for_fresh=True)["rows"] == [[inlet, f"{inlet}_log"]]
    assert client.delete_logs(property_uri=inlet) == {"ok": True}
    assert client.query_logs(property_uri=inlet) == []
    assert client.sparql_query(query, wait_for_fresh=True)["rows"] == []


def test_query_logs_follow_the_current_node_or_requested_alias(pump_model):
    pump = pump_model.query().entity(WATR.Pump, alias="pump")
    outlet = pump.related(S223.OutletConnectionPoint, alias="outlet", via=[S223.hasConnectionPoint])
    # Write to the current node, then write one note to both nodes.
    pump.insert_log("Pump started")
    outlet.insert_log("Outlet connected")
    assert outlet.read_logs()["message"].to_list() == ["Outlet connected"]
    assert outlet.read_logs(alias="pump")["message"].to_list() == ["Pump started"]
    assert outlet.read_logs(alias="*").shape == (2, 5)

    outlet.insert_log(
        "Both operating", alias="*",
        observation_start="2024-07-02T10:00:00Z", observation_end="2024-07-02T10:30:00Z",
    )
    assert outlet.read_logs(alias="*")["message"].to_list() == [
        "Pump started", "Outlet connected", "Both operating", "Both operating",
    ]
    outlet.insert_log(
        "Outlet checked", alias="outlet",
        observation_start="2024-07-02T10:00:00Z", observation_end="2024-07-02T11:30:00Z",
    )
    assert outlet.read_logs(alias="pump")["message"].to_list() == ["Pump started", "Both operating"]
    assert outlet.read_logs(alias="*", observation_start="2024-07-02T10:45:00Z")["message"].to_list() == [
        "Outlet checked",
    ]
    with pytest.raises(KeyError):
        outlet.read_logs(alias="unknown")


def test_logbook_persists_and_replaces_entries_in_sql(experiment_ledger):
    from acquirium.Storage.ledger import DuckDBLedger
    from acquirium.Storage.logbook import Logbook
    from acquirium.internals.models import LogEntry, TimeIntervalModel

    backend = "duckdb" if isinstance(experiment_ledger, DuckDBLedger) else "postgres"
    logbook = Logbook(experiment_ledger, backend=backend)
    at = datetime(2026, 11, 1, tzinfo=timezone.utc)
    uri = "urn:property"
    entry = LogEntry(property_uri=uri, timestamp=at, period=TimeIntervalModel())
    logbook.insert(entry.model_copy(update={"message": "first"}))
    logbook.insert(entry.model_copy(update={"message": "corrected"}))

    reopened = Logbook(experiment_ledger, backend=backend)
    assert [row.message for row in reopened.query(uri, None, None)] == ["corrected"]
    reopened.delete(uri)
    assert logbook.query(uri, None, None) == []


def test_plant_logbook_round_trips_through_the_client(own_acquirium):
    acq = own_acquirium
    acq.delete_logs()

    assert acq.insert_log("Pump checked", log_time="2026-11-01T04:00:00-06:00") == {"ok": True}
    entries = acq.read_logs(log_time_start="2026-11-01T10:00:00Z")
    assert entries["message"].to_list() == ["Pump checked"]
    assert entries["log_time"][0].hour == 10

    acq.delete_logs()
    assert acq.read_logs().is_empty()
