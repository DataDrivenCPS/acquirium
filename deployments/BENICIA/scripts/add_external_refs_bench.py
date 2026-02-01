import argparse
from pathlib import Path

import rdflib
from rdflib import Literal
from rdflib.namespace import RDF, XSD

from acquirium.internals.internals_namespaces import ACQUIRIUM_NS, QUDT_UNIT, S223


PROPERTY_TYPES = {
    S223.QuantifiableObservableProperty,
    S223.QuantifiableActuatableProperty,
}

UNIT_MAP = {
    "MGD": QUDT_UNIT["GAL_US-PER-DAY"],
    "ug/L": QUDT_UNIT["MicroGM-PER-L"],
    "mg/L": QUDT_UNIT["MilliGM-PER-L"],
    "pH": QUDT_UNIT.PH,
    "CFU/100mL": QUDT_UNIT["CFU-PER-100ML"],
    "fraction": QUDT_UNIT.ONE,
    "percent": QUDT_UNIT.PERCENT,
}


def local_name(uri: rdflib.term.Identifier) -> str:
    return str(uri).split("/")[-1]


def get_properties(graph: rdflib.Graph) -> list[rdflib.term.Identifier]:
    props = []
    for subj, _, obj in graph.triples((None, RDF.type, None)):
        if obj in PROPERTY_TYPES:
            props.append(subj)
    return sorted(props, key=local_name)


def normalize_value(unit_label: str, value: float) -> tuple[float, dict[str, Literal]]:
    extras: dict[str, Literal] = {}
    if unit_label == "MGD":
        extras["original_value"] = Literal(value, datatype=XSD.decimal)
        extras["original_unit"] = Literal(unit_label)
        return value * 1_000_000.0, extras
    return value, extras


def add_external_references(
    graph: rdflib.Graph,
    properties: list[rdflib.term.Identifier],
    broker: str,
    port: int,
    topic_prefix: str,
    count_refs: int,
) -> None:
    wbs = rdflib.Namespace("urn:ex/")

    i = 0
    for prop in properties:
        if i >= count_refs:
            break
        i += 1
        name = local_name(prop)
        mqtt_ref = wbs[f"{name}_mqtt_ref"]

        graph.add((prop, ACQUIRIUM_NS.hasExternalReference, mqtt_ref))

        graph.add((mqtt_ref, RDF.type, ACQUIRIUM_NS.MQTTReference))
        graph.add((mqtt_ref, ACQUIRIUM_NS.Broker, Literal(broker)))
        graph.add((mqtt_ref, ACQUIRIUM_NS.Port, Literal(port)))
        graph.add((mqtt_ref, ACQUIRIUM_NS.Topic, Literal(f"{topic_prefix}/{name}")))
        graph.add((mqtt_ref, ACQUIRIUM_NS.time_key, Literal("Timestamp")))
        graph.add((mqtt_ref, ACQUIRIUM_NS.value_key, Literal("Value")))



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add parquet/mqtt references and thresholds to the Benicia model.",
    )
    parser.add_argument(
        "--model",
        default="deployments/BENICIA/benicia-model-100.ttl",
        help="Path to the base Benicia model.",
    )
    parser.add_argument(
        "--output",
        default="deployments/BENICIA/benicia-model-with-refs-1.ttl",
        help="Path to write the updated model.",
    )
    parser.add_argument("--broker", default="mosquitto", help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port.")
    parser.add_argument(
        "--topic-prefix",
        default="benicia",
        help="Prefix for MQTT topics.",
    )
    parser.add_argument(
        "--count-refs",
        type=int,
        default=1,
        help="Number of total references to create",
    )

    args = parser.parse_args()

    graph = rdflib.Graph().parse(args.model, format="turtle")
    properties = get_properties(graph)

    add_external_references(
        graph=graph,
        properties=properties,
        broker=args.broker,
        port=args.port,
        topic_prefix=args.topic_prefix,
        count_refs=args.count_refs,
    )

    graph.bind("acq", ACQUIRIUM_NS)
    graph.bind("unit", QUDT_UNIT)
    graph.bind("s223", S223)
    graph.bind("xsd", XSD)

    graph.serialize(destination=args.output, format="turtle")
    print(f"Wrote updated model to {args.output}")


if __name__ == "__main__":
    main()
