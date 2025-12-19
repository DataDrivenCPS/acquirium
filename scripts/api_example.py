"""
Example: Acquirium + Query API end to end usage

This script shows:
1) Creating an Acquirium client
2) Building a query graph with entity nodes
3) Expanding to data nodes using find_data and find_all_data
4) Applying generic filters via convenience functions (filter_by_unit, etc.)
5) Inspecting query graph and compiled SPARQL
6) Executing metadata query + fetching timeseries dataframe

Adjust the ontology URIs and filter values to match your deployment.
"""

from datetime import datetime, timedelta

from acquirium import Acquirium

from acquirium.Client.query import Query
from acquirium.Internals.internals_namespaces import (
    HAS_UNIT,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    OF_SUBSTANCE,
    S223,
    WATR,
    UNIT
)


# Helper: pretty banners
def banner(msg: str) -> None:
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n")


## Initial setup: create Acquirium instance
banner("1) Create Acquirium session")
acq = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )

## Add a graph to acquirium
## This will automatically connect the data sources to database and ingest timeseries data (if file, ingest all, if stream, start listening)
banner("2) Add a graph to acquirium")
acq.insert_graph("deployments/WATERTAP/models/watertap-simple-pipe-model-with-ext-refs.ttl")

banner("3) Start a new query and add an entity node")

#create a query object
q1 = acq.query() 
#find an entity node using an ontology class
q1 : Query  = q1.find_entity(_class=S223.InletConnectionPoint, alias="pump-in")
q1.metadata_head()

#alternatively, use a natural language string to match ontology classes
#Acquirium.find_entity is a convenience function for simplifying query object creation
q2 : Query = acq.find_entity(_class="inlet connection pt", alias="pump-in")
q2.show_query_graph()
q2.metadata_head() # should be same as q1

banner("4) Add a related entity node")
# add a related entity node reachable within 1 hop from "pump-in"
# _from is optional here since we have only one node so far and it is the current pointer
q1 : Query = q1.find_related(_class="urn:nawi-water-ontology#Pump", alias="pump", _from="pump-in", hops=1)
q1.show_query_graph()

# alternatively, you can join two query objects
q3 : Query = acq.find_entity(_class="pump", alias="pump")
q4 : Query = q2.relate_to(q3, _from="pump-in", hops=1)
q4.show_query_graph() # should be same as q1


banner("5) Find all data nodes")
q5 :Query = acq.find_all_data()
q5.show_query_graph()
q5.metadata_head()
q5.data_head()

banner("6) Find data nodes of specific entity")
q6 : Query = q3.find_data()
q6.show_query_graph()
q6.metadata_head()
q6.data_head()

banner("7) Apply filters to data nodes")
q7 : Query = q5.filter_by_unit(unit = "kilogram per second")
q7.show_query_graph()
q7.metadata_head()
q7.data_head()



