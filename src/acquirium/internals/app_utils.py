from __future__ import annotations

from urllib.parse import quote
from rdflib import URIRef, Graph, Literal
from typing import Any
from acquirium.internals.internals_namespaces import (ACQUIRIUM_NS, 
                                                      SOFT_SENSOR, 
                                                      THRESHOLD, 
                                                      ALARM, 
                                                      REPORT)


def _safe_fragment(value: str) -> str:
    return quote(value, safe="")


def app_uri_for(name: str) -> str:
    return str(ACQUIRIUM_NS[f"app/{_safe_fragment(name)}"])

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
