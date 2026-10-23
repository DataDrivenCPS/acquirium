# =============================================================================
# WARNING: UNTESTED. This script was ported to the explore query interface
# (acq.query().entity() / related() / measurement() / where()) mechanically
# when the legacy find_* builder was removed, and has not been run since.
# Expect it to need fixes before it works end to end.
# =============================================================================
"""
Example: Acquirium + Query API end to end usage

This script shows:
1) Creating an Acquirium client
2) Building a query with entity nodes
3) Expanding to measurement points with measurement() and find_all_data()
4) Filtering measurement points with where()
5) Inspecting the compiled SPARQL
6) Executing the metadata query and fetching a timeseries frame

For a fuller introduction see the WaterTAP notebooks under /notebooks/watertap/
and /deployments/WATERTAP/readme.md. Adjust the ontology URIs and filter
values to match your deployment.
"""

from acquirium import Acquirium
from acquirium.internals.internals_namespaces import S223


def banner(msg: str) -> None:
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n")


banner("1) Create Acquirium session")
acq = Acquirium(server_url="localhost", server_port=8000, use_ssl=False)

banner("2) Start a new query and add an entity node")
# an ontology class ...
q1 = acq.query().entity(S223.InletConnectionPoint, alias="pump-in")
print(q1.metadata().head())
# ... or free text, resolved against the ontology by the server
q2 = acq.query().entity("inlet connection pt", alias="pump-in")
print(q2.to_sparql())
print(q2.metadata().head())        # should match q1

banner("3) Add a related entity node")
# an entity reachable within one step from "pump-in"; frm= is optional while
# "pump-in" is the current node
q1 = q1.related("urn:nawi-water-ontology#Pump", alias="pump", frm="pump-in", max_depth=1)
print(q1.to_sparql())

banner("4) Every measurement point in the plant")
q5 = acq.find_all_data()           # same as acq.query().measurement()
print(q5.to_sparql())
print(q5.metadata().head())
print(q5.dataframe(limit=5).head())

banner("5) Measurement points of one entity")
q6 = acq.query().entity("pump", alias="pump").measurement()
print(q6.to_sparql())
print(q6.metadata().head())
print(q6.dataframe(limit=5).head())

banner("6) Filter measurement points")
q7 = q5.where(unit="kilogram per second")
print(q7.to_sparql())
print(q7.metadata().head())
print(q7.dataframe(limit=5).head())
