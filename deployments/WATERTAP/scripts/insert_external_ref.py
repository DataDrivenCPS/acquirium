import rdflib
from acquirium.internals.internals_namespaces import *

wbs = rdflib.Namespace("urn:ex/")
watr = rdflib.Namespace("urn:nawi-water-ontology#")
s223 = rdflib.Namespace("http://data.ashrae.org/standard223#")
ref_ns = ACQUIRIUM_NS

model = rdflib.Graph().parse("deployments/WATERTAP/models/watertap-simple-pipe-model.ttl", format="turtle")

model.add((wbs.Pump1, s223.hasProperty, wbs.PumpWork))
model.add((wbs.PumpWork, rdflib.RDF.type, s223.QuantifiableObservableProperty ))
model.add((wbs.PumpWork, rdflib.RDFS.comment, rdflib.Literal("SOFT SENSOR")))

blnk = wbs.pump_work_mqtt_ref
model.add((wbs.PumpWork, ref_ns.hasExternalReference, blnk))
model.add((blnk, rdflib.RDF.type, ref_ns.MQTTReference))
model.add((blnk, ref_ns.Broker, rdflib.Literal("mosquitto")))
model.add((blnk, ref_ns.Port, rdflib.Literal("1883")))
model.add((blnk, ref_ns.Topic, rdflib.Literal("pump_work")))
model.add((blnk, ref_ns.value_key, rdflib.Literal("Value")))
model.add((blnk, ref_ns.time_key, rdflib.Literal("Timestamp")))


blnk = wbs.pump_inlet_flow_mass_seawater_mqtt_ref
model.add((wbs['Pump1-in-flow-mass-seawater'], ref_ns.hasExternalReference, blnk))
model.add((blnk, rdflib.RDF.type, ref_ns.MQTTReference))
model.add((blnk, ref_ns.Broker, rdflib.Literal("mosquitto")))
model.add((blnk, ref_ns.Port, rdflib.Literal("1883")))
model.add((blnk, ref_ns.Topic, rdflib.Literal("saltwater_flow_mass_rate")))
model.add((blnk, ref_ns.value_key, rdflib.Literal("Value")))
model.add((blnk, ref_ns.time_key, rdflib.Literal("Timestamp")))

blnk = wbs.pump_inlet_flow_mass_water_mqtt_ref
model.add((wbs['Pump1-in-flow-mass-water'], ref_ns.hasExternalReference, blnk))
model.add((blnk, rdflib.RDF.type, ref_ns.MQTTReference))
model.add((blnk, ref_ns.Broker, rdflib.Literal("mosquitto")))
model.add((blnk, ref_ns.Port, rdflib.Literal("1883")))
model.add((blnk, ref_ns.Topic, rdflib.Literal("water_flow_mass_rate")))
model.add((blnk, ref_ns.value_key, rdflib.Literal("Value")))
model.add((blnk, ref_ns.time_key, rdflib.Literal("Timestamp")))

blnk = wbs.pump_inlet_pressure_mqtt_ref
model.add((wbs['Pump1-in-pressure'], ref_ns.hasExternalReference, blnk))
model.add((blnk, rdflib.RDF.type, ref_ns.MQTTReference))
model.add((blnk, ref_ns.Broker, rdflib.Literal("mosquitto")))
model.add((blnk, ref_ns.Port, rdflib.Literal("1883")))
model.add((blnk, ref_ns.Topic, rdflib.Literal("pump_inlet_pressure")))
model.add((blnk, ref_ns.value_key, rdflib.Literal("Value")))
model.add((blnk, ref_ns.time_key, rdflib.Literal("Timestamp")))

blnk = wbs.pump_inlet_temperature_mqtt_ref
model.add((wbs['Pump1-in-temperature'], ref_ns.hasExternalReference, blnk))
model.add((blnk, rdflib.RDF.type, ref_ns.MQTTReference))
model.add((blnk, ref_ns.Broker, rdflib.Literal("mosquitto")))
model.add((blnk, ref_ns.Port, rdflib.Literal("1883")))
model.add((blnk, ref_ns.Topic, rdflib.Literal("pump_inlet_temperature")))
model.add((blnk, ref_ns.value_key, rdflib.Literal("Value")))
model.add((blnk, ref_ns.time_key, rdflib.Literal("Timestamp")))

model.serialize("deployments/WATERTAP/models/watertap-simple-pipe-model-with-ext-refs.ttl", format="turtle")