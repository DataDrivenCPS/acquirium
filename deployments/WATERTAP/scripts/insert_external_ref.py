import rdflib
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_NS,
    BRICK_REF,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    MQTT_BROKER,
    MQTT_REFERENCE,
    MQTT_TOPIC,
    S223,
    TIME_KEY,
    VALUE_KEY,
)

wbs = rdflib.Namespace("urn:ex/")
watr = rdflib.Namespace("urn:nawi-water-ontology#")
s223 = rdflib.Namespace("http://data.ashrae.org/standard223#")

model = rdflib.Graph().parse("deployments/WATERTAP/models/watertap-simple-pipe-model.ttl", format="turtle")
model.bind("acq", ACQUIRIUM_NS)
model.bind("ref", BRICK_REF)

model.add((wbs.Pump1, s223.hasProperty, wbs.PumpWork))
model.add((wbs.PumpWork, rdflib.RDF.type, s223.QuantifiableObservableProperty))
model.add((wbs.PumpWork, rdflib.RDFS.comment, rdflib.Literal("SOFT SENSOR")))


def add_mqtt_ref(point: rdflib.URIRef, ref_node: rdflib.URIRef, topic: str) -> None:
    model.add((point, HAS_EXTERNAL_REFERENCE, ref_node))
    model.add((ref_node, rdflib.RDF.type, MQTT_REFERENCE))
    model.add((ref_node, DATA_SOURCE, rdflib.Literal("SCADA")))
    model.add((ref_node, MQTT_BROKER, rdflib.Literal("mosquitto:1883")))
    model.add((ref_node, MQTT_TOPIC, rdflib.Literal(topic)))
    model.add((ref_node, TIME_KEY, rdflib.Literal("Timestamp")))
    model.add((ref_node, VALUE_KEY, rdflib.Literal("Value")))


add_mqtt_ref(wbs.PumpWork, wbs.pump_work_mqtt_ref, "pump_work")
add_mqtt_ref(wbs["Pump1-in-flow-mass-seawater"], wbs.pump_inlet_flow_mass_seawater_mqtt_ref, "saltwater_flow_mass_rate")
add_mqtt_ref(wbs["Pump1-in-flow-mass-water"], wbs.pump_inlet_flow_mass_water_mqtt_ref, "water_flow_mass_rate")
add_mqtt_ref(wbs["Pump1-in-pressure"], wbs.pump_inlet_pressure_mqtt_ref, "pump_inlet_pressure")
add_mqtt_ref(wbs["Pump1-in-temperature"], wbs.pump_inlet_temperature_mqtt_ref, "pump_inlet_temperature")

model.serialize("deployments/WATERTAP/models/watertap-simple-pipe-model-with-ext-refs.ttl", format="turtle")
