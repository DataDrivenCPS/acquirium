from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from rdflib import RDF, RDFS, Graph, Literal, URIRef

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_NS,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    ALARM,
    APP,
    APP_PARAMS,
    APP_QUERY,
    DATA_SOURCE,
    ENV_SPEC,
    EVENT_STREAM,
    HAS_EXTERNAL_REFERENCE,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    HAS_VERSION,
    OUTPUT_KIND,
    PRODUCES,
    REPORT,
    RUN_INTERVAL,
    RUN_MODE,
    SOFT_SENSOR,
    STORAGE_BACKEND,
    STREAM,
    TASK,
    THRESHOLD,
    TIMESERIES_STREAM,
    VIRTUAL_POINT,
)

if TYPE_CHECKING:
    from acquirium.internals.models import AppSpec


def _safe_fragment(value: str) -> str:
    return quote(value, safe="")


def app_uri_for(name: str) -> str:
    return str(ACQUIRIUM_NS[f"app/{_safe_fragment(name)}"])


def app_source_id(name: str) -> str:
    """Return the reserved, stable graph and stream owner for one app."""
    return f"app:{name}"

def app_type_uri(app_type: str) -> URIRef:
    norm = (app_type or "").strip().lower()
    if norm in {"soft_sensor", "softsensor"}:
        return SOFT_SENSOR
    if norm == "threshold":
        return THRESHOLD
    if norm == "alarm":
        return ALARM
    if norm == "report":
        return REPORT
    if norm == "task":
        return TASK
    if "://" in app_type or app_type.startswith("urn:"):
        return URIRef(app_type)
    return URIRef(str(ACQUIRIUM_NS[app_type]))

def add_literal_or_uri(graph: Graph, subj: URIRef, pred: URIRef, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str) and ("://" in value or value.startswith("urn:")):
        graph.add((subj, pred, URIRef(value)))
    else:
        graph.add((subj, pred, Literal(value)))


def app_deregister_update(name: str) -> str:
    """SPARQL UPDATE stripping one app/task's registration triples.

    Inverse of :func:`app_spec_graph`, driven only by the app URI: removes
    the app node, the virtual points it produces, and those points'
    external references — including triples a build phase may have added on
    the points, not just what registration wrote. Applied to the app's own
    graph by whoever owns it (the runner, the task host, or an actor-less
    server path).
    """
    app_uri = app_uri_for(name)
    return f"""
    DELETE {{
      ?app ?ap ?ao .
      ?point ?pp ?po .
      ?ref ?rp ?ro .
    }} WHERE {{
      VALUES ?app {{ <{app_uri}> }}
      {{ ?app ?ap ?ao . }}
      UNION {{ ?app <{PRODUCES}> ?point . ?point ?pp ?po . }}
      UNION {{
        ?app <{PRODUCES}> ?point .
        ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
        ?ref ?rp ?ro .
      }}
    }}
    """


def app_spec_graph(spec: "AppSpec") -> Graph:
    """Build the registration graph for one app/task spec.

    Inverted by ``restore_app_specs``; every field written here must be read
    back there (one round-trip test covers the pair). A free function — not
    actor state — because the app runner, the task host, and actor-less
    registration paths all write the same shape.
    """
    from acquirium.internals.models import compute_ref_uri

    app_uri = URIRef(app_uri_for(spec.name))
    source_id = app_source_id(spec.name)
    graph = Graph()

    graph.add((app_uri, RDF.type, APP))
    if spec.kind == "task":
        graph.add((app_uri, RDF.type, TASK))
    graph.add((app_uri, RDFS.label, Literal(spec.name)))
    if spec.app_type:
        graph.add((app_uri, RDF.type, app_type_uri(spec.app_type)))

    if spec.version:
        graph.add((app_uri, HAS_VERSION, Literal(spec.version)))
    if spec.queries:
        graph.add((app_uri, APP_QUERY, Literal(json.dumps(spec.queries, sort_keys=True, ensure_ascii=True))))
    if spec.params:
        graph.add((app_uri, APP_PARAMS, Literal(json.dumps(spec.params, sort_keys=True, ensure_ascii=True))))
    graph.add((app_uri, RUN_MODE, Literal(spec.run_mode)))
    if spec.interval is not None:
        graph.add((app_uri, RUN_INTERVAL, Literal(float(spec.interval))))
    if spec.env is not None:
        graph.add((app_uri, ENV_SPEC, Literal(
            json.dumps(spec.env.model_dump(), sort_keys=True, ensure_ascii=True)
        )))

    for out in spec.outputs:
        point_uri = URIRef(out.point_uri)
        ref_uri = compute_ref_uri(source_id, out.point_uri)

        graph.add((app_uri, PRODUCES, point_uri))
        graph.add((point_uri, RDF.type, VIRTUAL_POINT))
        graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
        graph.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
        graph.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(out.point_uri)))
        graph.add((ref_uri, RDF.type, STREAM))
        graph.add((ref_uri, OUTPUT_KIND, Literal(out.kind)))
        if out.kind in {"event", "trigger"}:
            graph.add((ref_uri, RDF.type, EVENT_STREAM))
            graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("text")))
        else:
            graph.add((ref_uri, RDF.type, TIMESERIES_STREAM))
            graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")))

        graph.add((ref_uri, STORAGE_BACKEND, Literal(out.storage_backend or "timescale")))

        add_literal_or_uri(graph, point_uri, HAS_QUANTITY_KIND, out.quantity_kind)
        add_literal_or_uri(graph, point_uri, HAS_UNIT, out.unit)
        add_literal_or_uri(graph, point_uri, DATA_SOURCE, out.data_source)
    return graph
