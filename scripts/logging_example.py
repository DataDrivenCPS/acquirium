# =============================================================================
# WARNING: UNTESTED. This script was ported to the explore query interface
# (acq.query().entity() / related() / measurement() / where()) mechanically
# when the legacy find_* builder was removed, and has not been run since.
# Expect it to need fixes before it works end to end.
# =============================================================================
"""
Example: Acquirium Logging (Logbook) end to end usage

This script shows:
1) Creating an Acquirium client
2) Loading a small graph (no timeseries required)
3) Writing logs through the query API (single alias and alias="*")
4) Reading logs with time filters and observation period filters
5) Clearing logs at the end for repeatable runs

Adjust the URIs and model path to match your deployment.
"""

import time
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import WATR, S223


def banner(msg: str) -> None:
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n")


banner("1) Create Acquirium session")
acq = Acquirium(server_url="localhost", server_port=8000, use_ssl=False)

banner("2) Add a graph (no timeseries needed)")
acq.insert_graph_file("tests/test_model_nodata.ttl", source_id="plant")
time.sleep(1)

# Clean slate (repeatable demo)
banner("3) Clear existing logs for demo points")
for point_uri in ("urn:ex/Pump1", "urn:ex/Pump1-in", "urn:ex/Pump1-out"):
    acq.client.delete_logs(point_uri=point_uri)

banner("4) Create a query and write logs")
q = acq.query().entity(WATR.Pump, alias="pumps")

# a note on every entity the current node matches (alias="pumps")
q.insert_log("Pump started successfully.")

# expand to another entity and write a note there too
q2 = q.related(S223.OutletConnectionPoint, alias="out_cp", via=[S223.hasConnectionPoint])
q2.insert_log("Outlet connected successfully.")

# a note on every node in the query
q2.insert_log(
    "Pump and Outlet are happy",
    alias="*",
    observation_start="2024-07-02T10:00:00Z",
    observation_end="2024-07-02T10:30:00Z",
)

banner("5) Read logs (different filters)")
print("Logs (alias='*', log_time_start=2026-01-01T01:00:00Z):")
print(q2.read_logs(alias="*", log_time_start="2026-01-01T01:00:00Z"))

print("\nLogs (alias='pumps'):")
print(q2.read_logs(alias="pumps", log_time_start="2026-01-01T01:00:00Z"))

print("\nLogs (observation_start=2024-07-02T10:15:00Z):")
print(q2.read_logs(alias="*", observation_start="2024-07-02T10:15:00Z"))

banner("6) Cleanup (optional)")
for point_uri in ("urn:ex/Pump1", "urn:ex/Pump1-in", "urn:ex/Pump1-out"):
    acq.client.delete_logs(point_uri=point_uri)

print("\nDone.")
