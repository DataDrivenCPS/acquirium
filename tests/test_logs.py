import pytest
from conftest import ACQUIRIUM_TEST_SERVER_HOST, ACQUIRIUM_TEST_SERVER_PORT
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import *
from acquirium.Client.query import Q
import shutil
import polars as pl
import time
from datetime import datetime
from zoneinfo import ZoneInfo

@pytest.fixture
def acquirium_client_nodata():
    acq = Acquirium(
        server_url=ACQUIRIUM_TEST_SERVER_HOST,
        server_port=ACQUIRIUM_TEST_SERVER_PORT,
        use_ssl=False,
    )
    acq.insert_graph("tests/test_model_nodata.ttl", source_id="plant")
    time.sleep(1)
    result = acq.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True
    result = acq.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
    result = acq.client.delete_logs(point_uri="urn:ex/Pump1-in")
    assert result["ok"] is True

    return acq

##### Log tests backend #####
def test_log_1(acquirium_client_nodata):
    '''
    No period specified
    '''
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-in")
    assert result["ok"] is True


    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1",
        log_time="2024-06-10T12:00:00Z",
        log_message="Pump started successfully."
    )
    # Q the logs
    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1",
        log_time_start="2024-06-10T11:00:00Z"
    )
    assert len(logs) == 1
    assert logs[0].message == "Pump started successfully."

    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1",
        log_time_end="2024-06-11T11:00:00Z"
    )
    assert len(logs) == 1
    assert logs[0].message == "Pump started successfully."

    query = f'''
    SELECT ?pt ?logpt WHERE 
    {{
        ?pt a <{WATR}Pump> .
        ?pt <{ACQUIRIUM_NS}hasLog> ?logpt .
        ?logpt a <{ACQUIRIUM_NS}Logbook> .
    }}
    '''
    result = acquirium_client_nodata.client.sparql_query(query)
    assert len(result['rows']) == 1
    assert result['rows'][0][0] == "urn:ex/Pump1"
    assert result['rows'][0][1] == "urn:ex/Pump1_log"

    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True

##### Log tests backend #####
def test_log_2(acquirium_client_nodata):
    '''
    Period specified, multiple logs
    '''
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-in")
    assert result["ok"] is True

    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1-in",
        log_time="2024-06-17T12:00:00Z",
        observation_start="2024-06-15T12:00:00Z",
        observation_end="2024-06-15T12:30:00Z",
        log_message="Pump started successfully. - log 1"
    )
    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1-in",
        log_time="2024-06-16T12:00:00Z",
        observation_start="2024-06-14T12:00:00Z",
        observation_end="2024-06-14T12:30:00Z",
        log_message="Pump started successfully. - log 2"
    )
    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1-in",
        log_time="2024-06-18T12:00:00Z",
        observation_start="2024-06-14T12:00:00Z",
        observation_end="2024-06-16T12:30:00Z",
        log_message="Pump started successfully. - log 3"
    )
    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1-in",
        log_time="2024-06-20T12:00:00Z",
        observation_start="2024-06-15T12:20:00Z",
        observation_end="2024-06-15T12:25:00Z",
        log_message="Pump started successfully. - log 4"
    )
    acquirium_client_nodata.client.insert_log(
        point_uri="urn:ex/Pump1-in",
        log_time="2024-06-30T12:00:00Z",
        log_message="Pump started successfully. - log 5"
    )
    
    # Q the logs
    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1-in",
        log_time_start="2024-06-10T11:00:00Z"
    )
    assert len(logs) == 5
    assert logs[0].message == "Pump started successfully. - log 2"
    assert logs[4].message == "Pump started successfully. - log 5"

    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1-in",
        log_time_end="2024-06-21T11:00:00Z"
    )
    assert len(logs) == 4
    assert logs[0].message == "Pump started successfully. - log 2"

    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1-in",
        observation_start="2024-06-14T12:45:00Z"
    )
    assert len(logs) == 3

    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1-in",
        observation_end="2023-06-15T12:26:00Z"
    )
    assert len(logs) == 0

    logs = acquirium_client_nodata.client.query_logs(
        point_uri="urn:ex/Pump1-in",
        observation_end="2024-06-14T12:26:00Z"
    )
    assert len(logs) == 2

    query = f'''
    SELECT ?pt ?logpt WHERE 
    {{  
        ?pt a <{S223}InletConnectionPoint> .
        ?pt <{ACQUIRIUM_NS}hasLog> ?logpt .
        ?logpt a <{ACQUIRIUM_NS}Logbook> .
    }}
    '''
    result = acquirium_client_nodata.client.sparql_query(query)
    assert len(result['rows']) == 1
    assert result['rows'][0][0] == "urn:ex/Pump1-in"
    assert result['rows'][0][1] == "urn:ex/Pump1-in_log"

    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-in")
    assert result["ok"] is True

##### Log tests api #####
def test_log_3(acquirium_client_nodata):
    '''
    No period specified
    '''
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-in")
    assert result["ok"] is True
    q = acquirium_client_nodata.find_entity(_class = WATR.Pump, alias= "pumps")

    q.insert_log(
        message="Pump started successfully."
    )

    # Q the logs
    logs = q.read_logs(log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 1
    assert logs.shape == (1, 5)
    assert logs[0, "message"] == "Pump started successfully."

    q2 = q.find_related(_class = S223.OutletConnectionPoint, alias = "out_cp")

    q2.insert_log(
        message="Outlet connected successfully."
    )
    logs = q2.read_logs(log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 1
    assert logs.shape == (1, 5)
    assert logs[0, "message"] == "Outlet connected successfully."

    logs = q2.read_logs(alias = "*", log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 2
    assert logs.shape == (2, 5)

    logs = q2.read_logs(alias = "pumps",log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 1
    assert logs.shape == (1, 5)

    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True


def test_log_4(acquirium_client_nodata):
    '''
    Multiple entities, observation period specified
    '''

    q = acquirium_client_nodata.find_entity(_class = WATR.Pump, alias= "pumps")
    q.insert_log(
        message="Pump started successfully. - t2"
    )

    q2 = q.find_related(_class = S223.OutletConnectionPoint, alias = "out_cp")
    q2.insert_log(
        message="Outlet connected successfully. - t2"
    )

    logs = q2.read_logs(alias = "*", log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 2
    assert logs.shape == (2, 5)

    q2.insert_log(
        alias="*",
        observation_start="2024-07-02T10:00:00Z",
        observation_end="2024-07-02T10:30:00Z",
        message="Pump and Outlet are happy"
    )

    logs = q2.read_logs(alias = "pump", log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 0

    logs = q2.read_logs(alias = "pumps", log_time_start="2026-01-01T01:00:00Z")
    assert len(logs) == 2
    assert logs.shape == (2, 5)
    assert logs[0, "message"] == "Pump started successfully. - t2"
    assert logs[1, "message"] == "Pump and Outlet are happy"

    q2.insert_log(
        alias="out_cp",
        observation_start="2024-07-02T10:00:00Z",
        observation_end="2024-07-02T11:30:00Z",
        message="Outlet is super happy"
    )

    logs = q2.read_logs(alias = "*", observation_start="2024-07-02T10:45:00Z")
    assert len(logs) == 1
    assert logs.shape == (1, 5)
    assert logs[0, "message"] == "Outlet is super happy"

    logs = q2.read_logs(alias = "out_cp", observation_start="2024-07-02T10:15:00Z")
    assert len(logs) == 2
    assert logs.shape == (2, 5)
    assert logs[1, "message"] == "Outlet is super happy"

    q2.insert_log(
        alias="out_cp",
        observation_start="2024-07-02T09:00:00Z",
        observation_end="2024-07-02T11:30:00Z",
        message="Outlet is super happy"
    )

    logs = q2.read_logs(alias = "*", observation_end="2024-07-02T09:45:00Z")
    assert len(logs) == 1
    assert logs.shape == (1, 5)
    assert logs[0, "message"] == "Outlet is super happy"

    logs = q2.read_logs(alias = "*", observation_end="2024-07-02T10:15:00Z")
    assert len(logs) == 4
    assert logs.shape == (4, 5)
    assert logs[2, "message"] == "Outlet is super happy"
    assert logs[0, "message"] == "Pump and Outlet are happy"



    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1-out")
    assert result["ok"] is True
    result = acquirium_client_nodata.client.delete_logs(point_uri="urn:ex/Pump1")
    assert result["ok"] is True
