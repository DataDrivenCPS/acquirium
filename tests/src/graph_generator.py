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
from acquirium.Internals.internals_namespaces import *


def build_test_graph_csv() -> Graph:
    num_equipments: int = 10
    num_points: int = 10
    csv_path: str = "tests/sample_data.csv"
    ex = Namespace("urn:ex/")

    g = Graph()
    g.bind("ex", ex)
    g.bind("acq", ACQUIRIUM_NS)
    g.bind("s223", S223)
    g.bind("qudt", QUDT)

    # Equipment classes A B C D E under ACQUIRIUM_NS
    equip_classes = [
        ACQUIRIUM_NS.A,
        ACQUIRIUM_NS.B,
        ACQUIRIUM_NS.C,
        ACQUIRIUM_NS.D,
        ACQUIRIUM_NS.E,
    ]

    # Create 10 equipments: ex:eq_1 .. ex:eq_10
    for i in range(1, num_equipments + 1):
        eq = ex[f"eq_{i}"]
        g.add((eq, RDF.type, equip_classes[(i - 1) % len(equip_classes)]))

    g.add((ex["eq_1"],ACQUIRIUM_NS.hasProperty,ex["point_9"]))
    g.add((ex["eq_1"],ACQUIRIUM_NS.x,ex["eq_2"]))
    g.add((ex["eq_1"],ACQUIRIUM_NS.x,ex["eq_6"]))
    
    g.add((ex["eq_2"],ACQUIRIUM_NS.x,ex["eq_3"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.x,ex["eq_7"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.w,ex["eq_6"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.z,ex["eq_1"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.hasProperty,ex["point_1"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.hasProperty,ex["point_2"]))
    g.add((ex["eq_2"],ACQUIRIUM_NS.hasProperty,ex["point_3"]))

    g.add((ex["eq_3"],ACQUIRIUM_NS.x,ex["eq_4"]))
    g.add((ex["eq_3"],ACQUIRIUM_NS.x,ex["eq_9"]))
    g.add((ex["eq_3"],ACQUIRIUM_NS.w,ex["eq_7"]))
    g.add((ex["eq_3"],ACQUIRIUM_NS.z,ex["eq_2"]))

    g.add((ex["eq_4"],ACQUIRIUM_NS.z,ex["eq_3"]))
    g.add((ex["eq_4"],ACQUIRIUM_NS.w,ex["eq_8"]))
    
    g.add((ex["eq_5"],ACQUIRIUM_NS.x,ex["eq_6"]))
    g.add((ex["eq_5"],ACQUIRIUM_NS.hasProperty,ex["point_4"]))

    g.add((ex["eq_6"],ACQUIRIUM_NS.x,ex["eq_7"]))
    g.add((ex["eq_6"],ACQUIRIUM_NS.y,ex["eq_2"]))
    g.add((ex["eq_6"],ACQUIRIUM_NS.w,ex["eq_5"]))
    g.add((ex["eq_6"],ACQUIRIUM_NS.hasProperty,ex["point_5"]))
    g.add((ex["eq_6"],ACQUIRIUM_NS.hasProperty,ex["point_6"]))

    g.add((ex["eq_7"],ACQUIRIUM_NS.x,ex["eq_8"]))
    g.add((ex["eq_7"],ACQUIRIUM_NS.y,ex["eq_3"]))
    g.add((ex["eq_7"],ACQUIRIUM_NS.z,ex["eq_6"]))
    g.add((ex["eq_7"],ACQUIRIUM_NS.x,ex["eq_10"]))
    g.add((ex["eq_7"],ACQUIRIUM_NS.hasProperty,ex["point_7"]))

    g.add((ex["eq_8"],ACQUIRIUM_NS.y,ex["eq_3"]))
    g.add((ex["eq_8"],ACQUIRIUM_NS.z,ex["eq_7"]))

    g.add((ex["eq_9"],ACQUIRIUM_NS.hasProperty,ex["point_4"]))

    g.add((ex["eq_10"],ACQUIRIUM_NS.hasProperty,ex["point_8"]))


   # Create 10 data nodes: ex:point_1 .. ex:point_10
    for i in range(1, num_points + 1):
        point = ex[f"point_{i}"]
        ref = ex[f"point_{i}_csv_ref"]

        # Data node
        g.add((point, RDF.type, ACQUIRIUM_NS.QuantifiableObservableProperty))
        g.add((point, HAS_EXTERNAL_REFERENCE, ref))

        # Required metadata triples, objects under ACQUIRIUM_NS
        g.add((point, HAS_MEDIUM, ACQUIRIUM_NS[f"Medium{1+i//3}"]))
        g.add((point, OF_SUBSTANCE, ACQUIRIUM_NS[f"Substance{1+i//3}"]))
        if i <=8:
            g.add((point, HAS_QUANTITY_KIND, ACQUIRIUM_NS[f"QuantityKind{i//3}"]))
        else:
            g.add((point, HAS_ENUMERATION_KIND, ACQUIRIUM_NS[f"EnumerationKind{1}"]))
        g.add((point, HAS_UNIT, ACQUIRIUM_NS[f"Unit{i//4}"]))

        # CSV reference node
        g.add((ref, RDF.type, ACQUIRIUM_NS.CSVReference))
        g.add((ref, DATA_SOURCE, Literal("LAB")))
        g.add((ref, REF_PATH, Literal(csv_path)))
        g.add((ref, REF_TIME_COL, Literal(0)))
        # Value column: 1..10 (assumes time is col 0)
        g.add((ref, REF_VALUE_COL, Literal(i)))

    return g

def build_test_graph_stream() -> Graph:
    num_equipments = 3
    num_points = 4
    ex = Namespace("urn:ex/")

    g = Graph()
    g.bind("ex", ex)
    g.bind("acq", ACQUIRIUM_NS)
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

        # CSV reference node
        g.add((ref, RDF.type, ACQUIRIUM_NS.MQTTReference))
        g.add((ref, DATA_SOURCE, Literal("SCADA")))
        g.add((ref, BROKER, Literal("mosquitto")))
        g.add((ref, PORT, Literal("1883")))
        g.add((ref, TIME_KEY, Literal("Timestamp")))
        g.add((ref, VALUE_KEY, Literal("Value")))
        g.add((ref, TOPIC, Literal(f"topic{i-10}")))

    return g

if __name__ == "__main__":
    g1 = build_test_graph_csv()
    g1.serialize(destination="tests/test_model_csv.ttl", format="turtle")
    g2 = build_test_graph_stream()
    g2.serialize(destination="tests/test_model_stream.ttl", format="turtle")

    # Combine both graphs
    g_combined = g1 + g2
    g_combined.serialize(destination="tests/test_model.ttl", format="turtle")