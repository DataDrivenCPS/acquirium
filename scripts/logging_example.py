"""
Example: Acquirium Logging (Logbook) end to end usage

This script shows:
1) Creating an Acquirium client
2) Loading a small graph (no timeseries required)
3) Writing logs through the Query API (single alias and alias="*")
4) Reading logs with time filters and observation period filters
5) Clearing logs at the end for repeatable runs

Adjust the URIs and model path to match your deployment.
"""

import time
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import WATR, S223


# Helper: pretty banners
def banner(msg: str) -> None:
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n")


banner("1) Create Acquirium session")
acq = Acquirium(
    server_url="localhost",
    server_port=8000,
    use_ssl=False,
    lexicon_path="ontologies/lexicon.json",
)

banner("2) Add a graph (no timeseries needed)")
acq.insert_graph("tests/test_model_nodata.ttl")
time.sleep(1)

# Clean slate (repeatable demo)
banner("3) Clear existing logs for demo points")
acq.client.delete_logs(point_uri="urn:ex/Pump1")
acq.client.delete_logs(point_uri="urn:ex/Pump1-in")
acq.client.delete_logs(point_uri="urn:ex/Pump1-out")

banner("4) Create a query and write logs")
q = acq.find_entity(_class=WATR.Pump, alias="pumps")

# Insert a log on the current query pointer (alias="pumps")
q.insert_log(
    message="Pump started successfully."
)

# Expand to another entity and write a log there too
q2 = q.find_related(_class=S223.OutletConnectionPoint, alias="out_cp")
q2.insert_log(
    message="Outlet connected successfully."
)

# Insert a log to *all* nodes in the current query graph
q2.insert_log(
    alias="*",
    observation_start="2024-07-02T10:00:00Z",
    observation_end="2024-07-02T10:30:00Z",
    message="Pump and Outlet are happy"
)

banner("5) Read logs (different filters)")
logs_all = q2.read_logs(alias="*", log_time_start="2026-01-01T01:00:00Z")
print("Logs (alias='*', log_time_start=2026-01-01T01:00:00Z):")
print(logs_all)

logs_pumps = q2.read_logs(alias="pumps", log_time_start="2026-01-01T01:00:00Z")
print("\nLogs (alias='pumps'):")
print(logs_pumps)

logs_obs_window = q2.read_logs(alias="*", observation_start="2024-07-02T10:15:00Z")
print("\nLogs (observation_start=2024-07-02T10:15:00Z):")
print(logs_obs_window)

banner("6) Cleanup (optional)")
acq.client.delete_logs(point_uri="urn:ex/Pump1")
acq.client.delete_logs(point_uri="urn:ex/Pump1-in")
acq.client.delete_logs(point_uri="urn:ex/Pump1-out")

print("\nDone.")
