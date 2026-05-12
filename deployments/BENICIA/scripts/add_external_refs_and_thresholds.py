import argparse
from pathlib import Path

import rdflib
from rdflib import Literal
from rdflib.namespace import RDF, XSD

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_NS,
    BRICK_REF,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    MQTT_BROKER,
    MQTT_REFERENCE,
    MQTT_TOPIC,
    QUDT_UNIT,
    S223,
    TIME_KEY,
    VALUE_KEY,
)

FILE_REFERENCE = BRICK_REF.FileReference
FILE_LOCATION = BRICK_REF.fileLocation
TIME_COLUMN_ID = BRICK_REF.timeColumnID
VALUE_COLUMN_ID = BRICK_REF.valueColumnID


PROPERTY_TYPES = {
    S223.QuantifiableObservableProperty,
    S223.QuantifiableActuatableProperty,
}

THRESHOLD_DATA = {
    "urn:ex/Influent_Pump-in-flow-rate": {
        "side": "influent",
        "monitoring_location": "INF-001",
        "limits": [
            {
                "kind": "discharge_prohibition",
                "statistic": "average_dry_weather",
                "operator": "<=",
                "value": 4.5,
                "unit": "MGD",
                "notes": "Average dry weather influent flow must not exceed 4.5 MGD.",
            }
        ],
    },
    "urn:ex/Influent_Pump-in-cyanide": {
        "side": "influent",
        "monitoring_location": "INF-001",
        "limits": [
            {
                "kind": "action_threshold",
                "statistic": "sample_result",
                "operator": ">",
                "value": 6.6,
                "unit": "ug/L",
                "notes": "If influent cyanide exceeds 6.6 ug/L, a follow up sample is required within 5 days; if follow up also exceeds 6.6 ug/L, this indicates a significant cyanide discharge.",
            }
        ],
    },
    "urn:ex/Influent_Pump-in-biochemical-oxygen-demand": {
        "side": "influent",
        "monitoring_location": "INF-001",
        "limits": [],
        "notes": "No direct influent concentration limit. Used for monthly percent removal compliance (see compliance_rules).",
    },
    "urn:ex/Influent_Pump-in-tss-concentration": {
        "side": "influent",
        "monitoring_location": "INF-001",
        "limits": [],
        "notes": "No direct influent concentration limit. Used for monthly percent removal compliance (see compliance_rules).",
    },
    "urn:ex/Effluent_Pump-out-flow-rate": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [],
        "notes": "Flow is monitored for reporting. The permit limit captured here is the influent average dry weather prohibition (4.5 MGD) rather than an explicit effluent flow limit.",
    },
    "urn:ex/Effluent_Pump-out-biochemical-oxygen-demand": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 30,
                "unit": "mg/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "average_weekly",
                "operator": "<=",
                "value": 45,
                "unit": "mg/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-tss-concentration": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 30,
                "unit": "mg/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "average_weekly",
                "operator": "<=",
                "value": 45,
                "unit": "mg/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-ph": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "instantaneous_minimum",
                "operator": ">=",
                "value": 6.0,
                "unit": "pH",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "instantaneous_maximum",
                "operator": "<=",
                "value": 9.0,
                "unit": "pH",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-cl2-mgL": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "one_hour_average",
                "operator": "<=",
                "value": 0.38,
                "unit": "mg/L",
                "notes": "Compliance is evaluated as a one hour average.",
            },
            {
                "kind": "process_target",
                "statistic": "target",
                "operator": "=",
                "value": 0.0,
                "unit": "mg/L",
                "notes": "Operational target residual is 0.0 mg/L (process control plan target).",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-nh4-mgL": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 64,
                "unit": "mg/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "maximum_daily",
                "operator": "<=",
                "value": 110,
                "unit": "mg/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-copper": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 64,
                "unit": "ug/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "maximum_daily",
                "operator": "<=",
                "value": 119,
                "unit": "ug/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-cyanide": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 17,
                "unit": "ug/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "maximum_daily",
                "operator": "<=",
                "value": 43,
                "unit": "ug/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-teq-dioxin": {
        "side": "effluent",
        "monitoring_location": "EFF-001",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "average_monthly",
                "operator": "<=",
                "value": 1.4e-8,
                "unit": "ug/L",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "maximum_daily",
                "operator": "<=",
                "value": 2.8e-8,
                "unit": "ug/L",
            },
        ],
    },
    "urn:ex/Effluent_Pump-out-bacteria-enterococcus": {
        "side": "effluent",
        "monitoring_location": "EFF-001 (permit text); EFF-001D (MRP sampling location)",
        "limits": [
            {
                "kind": "effluent_limitation",
                "statistic": "six_week_rolling_geometric_mean",
                "operator": "<=",
                "value": 210,
                "unit": "CFU/100mL",
                "notes": "Calculated weekly using all results from the past six weeks.",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "monthly_exceedance_frequency",
                "operator": "<=",
                "value": 0.10,
                "unit": "fraction",
                "notes": "No more than 10 percent of samples in a calendar month may exceed 1000 CFU/100mL.",
            },
            {
                "kind": "effluent_limitation",
                "statistic": "monthly_exceedance_threshold",
                "operator": ">",
                "value": 1000,
                "unit": "CFU/100mL",
            },
        ],
    },
}

COMPLIANCE_RULES = [
    {
        "kind": "percent_removal",
        "applies_to": [
            "urn:ex/Influent_Pump-in-biochemical-oxygen-demand",
            "urn:ex/Influent_Pump-in-tss-concentration",
            "urn:ex/Effluent_Pump-out-biochemical-oxygen-demand",
            "urn:ex/Effluent_Pump-out-tss-concentration",
        ],
        "statistic": "average_monthly",
        "operator": ">=",
        "value": 85,
        "unit": "percent",
        "equivalent_form": "Effluent monthly mean concentration must be <= 15% of influent monthly mean concentration (for matched sampling period).",
    }
]

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
    parquet_dir: Path,
    broker: str,
    port: int,
    topic_prefix: str,
    time_col: str,
    value_col: str,
) -> None:
    wbs = rdflib.Namespace("urn:ex/")
    broker_literal = f"{broker}:{port}" if port else broker

    for prop in properties:
        name = local_name(prop)
        file_ref = wbs[f"{name}_file_ref"]
        mqtt_ref = wbs[f"{name}_mqtt_ref"]

        graph.add((prop, HAS_EXTERNAL_REFERENCE, file_ref))
        graph.add((prop, HAS_EXTERNAL_REFERENCE, mqtt_ref))

        file_path = str(parquet_dir / f"{name}.parquet")

        graph.add((file_ref, RDF.type, FILE_REFERENCE))
        graph.add((file_ref, DATA_SOURCE, Literal("Lab")))
        graph.add((file_ref, FILE_LOCATION, Literal(file_path)))
        graph.add((file_ref, TIME_COLUMN_ID, Literal(time_col)))
        graph.add((file_ref, VALUE_COLUMN_ID, Literal(value_col)))

        graph.add((mqtt_ref, RDF.type, MQTT_REFERENCE))
        graph.add((mqtt_ref, DATA_SOURCE, Literal("SCADA")))
        graph.add((mqtt_ref, MQTT_BROKER, Literal(broker_literal)))
        graph.add((mqtt_ref, MQTT_TOPIC, Literal(f"{topic_prefix}/{name}")))
        graph.add((mqtt_ref, TIME_KEY, Literal("Timestamp")))
        graph.add((mqtt_ref, VALUE_KEY, Literal("Value")))


def add_thresholds(graph: rdflib.Graph) -> None:
    wbs = rdflib.Namespace("urn:ex/")

    for prop_uri, payload in THRESHOLD_DATA.items():
        prop = rdflib.URIRef(prop_uri)
        name = local_name(prop)
        threshold_node = wbs[f"{name}_threshold"]

        graph.add((prop, ACQUIRIUM_NS.hasThreshold, threshold_node))
        graph.add((threshold_node, RDF.type, ACQUIRIUM_NS.Threshold))
        graph.add((threshold_node, ACQUIRIUM_NS.side, Literal(payload["side"])))
        graph.add(
            (
                threshold_node,
                ACQUIRIUM_NS.monitoring_location,
                Literal(payload["monitoring_location"]),
            )
        )

        if "notes" in payload:
            graph.add((threshold_node, ACQUIRIUM_NS.notes, Literal(payload["notes"])))

        for idx, limit in enumerate(payload.get("limits", []), start=1):
            limit_node = wbs[f"{name}_limit_{idx}"]
            graph.add((threshold_node, ACQUIRIUM_NS.hasLimit, limit_node))
            graph.add((limit_node, RDF.type, ACQUIRIUM_NS.Limit))
            graph.add((limit_node, ACQUIRIUM_NS.kind, Literal(limit["kind"])))
            graph.add((limit_node, ACQUIRIUM_NS.statistic, Literal(limit["statistic"])))
            graph.add((limit_node, ACQUIRIUM_NS.operator, Literal(limit["operator"])))

            unit_label = limit["unit"]
            unit_uri = UNIT_MAP.get(unit_label)
            if unit_uri is None:
                raise ValueError(f"No unit mapping for {unit_label} in {prop_uri}")

            value, extras = normalize_value(unit_label, limit["value"])
            graph.add((limit_node, ACQUIRIUM_NS.value, Literal(value)))
            graph.add((limit_node, ACQUIRIUM_NS.unit, unit_uri))

            for key, literal in extras.items():
                graph.add((limit_node, ACQUIRIUM_NS[key], literal))

            if "notes" in limit:
                graph.add((limit_node, ACQUIRIUM_NS.notes, Literal(limit["notes"])))

    for idx, rule in enumerate(COMPLIANCE_RULES, start=1):
        rule_node = wbs[f"compliance_rule_{idx}"]
        graph.add((rule_node, RDF.type, ACQUIRIUM_NS.ComplianceRule))
        graph.add((rule_node, ACQUIRIUM_NS.kind, Literal(rule["kind"])))
        graph.add((rule_node, ACQUIRIUM_NS.statistic, Literal(rule["statistic"])))
        graph.add((rule_node, ACQUIRIUM_NS.operator, Literal(rule["operator"])))
        graph.add((rule_node, ACQUIRIUM_NS.value, Literal(rule["value"])))

        unit_label = rule["unit"]
        unit_uri = UNIT_MAP.get(unit_label)
        if unit_uri is None:
            raise ValueError(f"No unit mapping for {unit_label} in compliance rules")
        graph.add((rule_node, ACQUIRIUM_NS.unit, unit_uri))

        if "equivalent_form" in rule:
            graph.add(
                (rule_node, ACQUIRIUM_NS.equivalent_form, Literal(rule["equivalent_form"]))
            )

        for prop_uri in rule.get("applies_to", []):
            graph.add((rule_node, ACQUIRIUM_NS.appliesTo, rdflib.URIRef(prop_uri)))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add parquet/mqtt references and thresholds to the Benicia model.",
    )
    parser.add_argument(
        "--model",
        default="deployments/BENICIA/benicia-model.ttl",
        help="Path to the base Benicia model.",
    )
    parser.add_argument(
        "--output",
        default="deployments/BENICIA/benicia-model-with-refs-thresholds.ttl",
        help="Path to write the updated model.",
    )
    parser.add_argument(
        "--parquet-dir",
        default="data/BENICIA/parquet",
        help="Directory for parquet file references.",
    )
    parser.add_argument("--broker", default="mosquitto", help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port.")
    parser.add_argument(
        "--topic-prefix",
        default="benicia",
        help="Prefix for MQTT topics.",
    )
    parser.add_argument(
        "--time-column",
        default="0",
        help="Time column name (or numeric index as a string) for ref:FileReference.",
    )
    parser.add_argument(
        "--value-column",
        default="1",
        help="Value column name (or numeric index as a string) for ref:FileReference.",
    )
    args = parser.parse_args()

    graph = rdflib.Graph().parse(args.model, format="turtle")
    properties = get_properties(graph)

    add_external_references(
        graph=graph,
        properties=properties,
        parquet_dir=Path(args.parquet_dir),
        broker=args.broker,
        port=args.port,
        topic_prefix=args.topic_prefix,
        time_col=args.time_column,
        value_col=args.value_column,
    )
    add_thresholds(graph)

    graph.bind("acq", ACQUIRIUM_NS)
    graph.bind("ref", BRICK_REF)
    graph.bind("unit", QUDT_UNIT)
    graph.bind("s223", S223)
    graph.bind("xsd", XSD)

    graph.serialize(destination=args.output, format="turtle")
    print(f"Wrote updated model to {args.output}")


if __name__ == "__main__":
    main()
