"""4 - Inserting a Plant Model

The code of the tutorial page "4 - Inserting a Plant Model".
"""

from datetime import datetime, timezone

import acquirium as aq

acq = aq.init()

# Inserting a model from text
#
# The pump P1 feeds the mixer M1, and the mixer feeds the reverse osmosis stage RO.

model = """
@prefix owl:    <http://www.w3.org/2002/07/owl#> .
@prefix qudt:   <http://qudt.org/schema/qudt/> .
@prefix qudtqk: <http://qudt.org/vocab/quantitykind/> .
@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
@prefix s223:   <http://data.ashrae.org/standard223#> .
@prefix unit:   <http://qudt.org/vocab/unit/> .
@prefix watr:   <https://watermetadata.org/ontology/watr#> .
@prefix wbs:    <urn:swro/> .

wbs: a owl:Ontology ;
    owl:imports <urn:nawi-water-ontology> .

wbs:P1 a watr:Pump ;
    rdfs:label "High-pressure Pump P1" ;
    s223:hasConnectionPoint wbs:P1-out ;
    s223:connectedTo wbs:M1 .

wbs:M1 a s223:Junction ;
    rdfs:label "Feed Mixer M1" ;
    s223:connectedTo wbs:RO .

wbs:RO a watr:ReverseOsmosisMembrane ;
    rdfs:label "Reverse Osmosis Stage" .

wbs:P1-out a s223:OutletConnectionPoint ;
    s223:hasMedium watr:Water-Seawater ;
    s223:hasProperty wbs:P1-out-pressure-pa .

wbs:P1-out-pressure-pa a s223:QuantifiableObservableProperty ;
    qudt:hasQuantityKind qudtqk:Pressure ;
    qudt:hasUnit unit:PA .
"""

acq.insert_graph(model, source_id="plant")

print(acq.query().entity("Equipment").include("label"))

# Connecting streams to the plant model

acq.register_streams([{
    "source_id": "scada",
    "stream_name": "PT-101",
    "property_uri": "urn:swro/P1-out-pressure-pa",
}])

acq.insert_timeseries("scada", "PT-101", [
    (datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc), 5.1e6),
    (datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc), 5.2e6),
])

# The stream gets its unit and quantity kind from the model:

print(acq.query().measurement().where(quantity_kind="pressure").include("unit", "quantity_kind"))

# The pressure upstream of the mixer:

q = acq.query().entity("Junction").measurement(direction="upstream", quantity_kind="pressure")

print(q.dataframe())

# Stop Acquirium.

aq.shutdown()
