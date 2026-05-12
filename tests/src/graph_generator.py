"""
Generate a simple Turtle file for Acquirium testing.

Assumptions:
- You already have Namespace objects available via your internals import:
  ACQUIRIUM_NS, S223, QUDT
- Model namespace must be "urn:ex/"

Output:
- Writes test_model.ttl in the current directory
"""

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF

# Pull in your namespaces (adjust import path if needed)
from acquirium.internals.internals_namespaces import *


def build_test_graph_stream() -> Graph:
    num_equipments = 3
    num_points = 4
    ex = Namespace("urn:ex/")

    g = Graph()
    g.bind("ex", ex)
    g.bind("acq", ACQUIRIUM_NS)
    g.bind("ref", BRICK_REF)
    g.bind("s223", S223)
    g.bind("qudt", QUDT)

    # Equipment classes A B C D E under ACQUIRIUM_NS
    equip_classes = [
        ACQUIRIUM_NS.A,
        ACQUIRIUM_NS.C,
    ]

    # Create 10 equipments: ex:eq_1 .. ex:eq_10
    for i in range(1, num_equipments + 1):
        eq = ex[f"eq_{i+10}"]
        g.add((eq, RDF.type, equip_classes[(i - 1) % len(equip_classes)]))

    g.add((ex["eq_11"],ACQUIRIUM_NS.x,ex["eq_12"]))
    g.add((ex["eq_12"],ACQUIRIUM_NS.x,ex["eq_13"]))
    g.add((ex["eq_13"],ACQUIRIUM_NS.z,ex["eq_12"]))
    g.add((ex["eq_12"],ACQUIRIUM_NS.z,ex["eq_11"]))
    g.add((ex["eq_11"],ACQUIRIUM_NS.y,ex["eq_13"]))
    g.add((ex["eq_13"],ACQUIRIUM_NS.w,ex["eq_11"]))

    g.add((ex["eq_12"],ACQUIRIUM_NS.hasProperty,ex["point_11"]))
    g.add((ex["eq_12"],ACQUIRIUM_NS.hasProperty,ex["point_12"]))
    g.add((ex["eq_13"],ACQUIRIUM_NS.hasProperty,ex["point_13"]))
    g.add((ex["eq_13"],ACQUIRIUM_NS.hasProperty,ex["point_14"]))

    # Create 10 data nodes: ex:point_1 .. ex:point_10
    for i in range(1, num_points + 1):
        i=i + 10
        point = ex[f"point_{i}"]
        ref = ex[f"point_{i}_mqtt_ref"]

        # Data node
        g.add((point, RDF.type, ACQUIRIUM_NS.QuantifiableObservableProperty))
        g.add((point, HAS_EXTERNAL_REFERENCE, ref))

        # Required metadata triples, objects under ACQUIRIUM_NS
        g.add((point, HAS_MEDIUM, ACQUIRIUM_NS[f"Medium{i//3}"]))
        g.add((point, OF_SUBSTANCE, ACQUIRIUM_NS[f"Substance{i//3}"]))
        g.add((point, HAS_QUANTITY_KIND, ACQUIRIUM_NS[f"QuantityKind{i//2}"]))
        g.add((point, HAS_UNIT, ACQUIRIUM_NS[f"Unit{i}"]))

        # MQTT reference node — port encoded in the broker literal per ref-schema
        g.add((ref, RDF.type, MQTT_REFERENCE))
        g.add((ref, DATA_SOURCE, Literal("SCADA")))
        g.add((ref, MQTT_BROKER, Literal("mosquitto:1883")))
        g.add((ref, MQTT_TOPIC, Literal(f"topic{i-10}")))
        g.add((ref, TIME_KEY, Literal("Timestamp")))
        g.add((ref, VALUE_KEY, Literal("Value")))

    return g

if __name__ == "__main__":
    g2 = build_test_graph_stream()
    g2.serialize(destination="tests/test_model_stream.ttl", format="turtle")
    g2.serialize(destination="tests/test_model.ttl", format="turtle")