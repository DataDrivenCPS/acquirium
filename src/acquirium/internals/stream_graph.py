"""Turning stream registrations into RDF.

The external reference carries a stream's semantics — unit, quantity kind,
medium, substance, origin tag. A point, when one is supplied, gets only its
type, its label and the link to the reference; Acquirium never asserts
semantics onto a point, because points belong to the user's model and a
later ``insert_graph(replace=True)`` on that source would drop anything we
wrote there. Where a point *does* carry its own semantics, the two are
reconciled at registration (see ``Manager.register_streams``) and the point
wins at read time.

This lives under ``internals`` rather than in the client because the server
builds these triples now: it is the only side that can see an existing point
to reconcile against.
"""
from __future__ import annotations

from typing import Any

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    OF_SUBSTANCE,
    STORED_AT,
    UNIT_MISMATCH_ALLOWED,
    VIRTUAL_POINT,
)
from acquirium.internals.models import compute_ref_uri
from acquirium.Storage.values import normalize_value_kind

#: Semantic fields written onto the reference, and the predicate each uses.
#: Also the set that reconciliation compares against a linked point.
SEMANTIC_PREDICATES: dict[str, URIRef] = {
    "unit": HAS_UNIT,
    "quantity_kind": HAS_QUANTITY_KIND,
    "medium": HAS_MEDIUM,
    "substance": OF_SUBSTANCE,
}

#: Semantic field -> the resolver ``kind`` its free text is resolved as. The
#: field name is the semantic role, so callers never supply a kind. ``medium``
#: resolves as a substance because that is what ``s223:hasMedium`` objects
#: are; it used to resolve as a class on one of two call paths.
FIELD_KINDS: dict[str, str] = {
    "unit": "unit",
    "quantity_kind": "quantity_kind",
    "medium": "substance",
    "substance": "substance",
}


def _add(g: Graph, subj: URIRef, pred: URIRef, value: Any) -> None:
    """Add one triple, treating URI-shaped strings as IRIs and the rest as literals."""
    if value is None:
        return
    if isinstance(value, URIRef):
        g.add((subj, pred, value))
    elif "://" in str(value) or str(value).startswith("urn:"):
        g.add((subj, pred, URIRef(str(value))))
    else:
        g.add((subj, pred, Literal(value)))


def build_stream_triples(g: Graph, stream: dict, resolved: dict[str, Any]) -> None:
    """Write one stream's reference, point and metadata triples into ``g``.

    ``resolved`` maps each semantic field to its resolved URI (or ``None``
    when the text did not resolve, in which case the raw value is kept as a
    literal so nothing is lost silently).
    """
    source_id = stream.get("source_id")
    ref_name = stream.get("ref_name")
    point_uri_raw = stream.get("point_uri")
    label = stream.get("label")

    ref_uri = None
    if ref_name is not None and source_id is not None:
        ref_uri = compute_ref_uri(source_id, ref_name)
        g.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
        g.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)))
        # Only assert a value kind the caller supplied; a default would
        # contradict a later data-derived kind on the same reference node.
        if stream.get("value_kind") is not None:
            g.add((
                ref_uri,
                ACQUIRIUM_VALUE_KIND,
                Literal(normalize_value_kind(stream["value_kind"])),
            ))
        g.add((ref_uri, STORED_AT, ACQUIRIUM_DB_URI))
        g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))

        # The semantics live here, whether or not a point exists.
        for field, predicate in SEMANTIC_PREDICATES.items():
            raw = stream.get(field)
            if raw is None:
                continue
            _add(g, ref_uri, predicate, resolved.get(field) or raw)
        _add(g, ref_uri, DATA_SOURCE, stream.get("data_source"))
        if stream.get("allow_unit_mismatch"):
            g.add((ref_uri, UNIT_MISMATCH_ALLOWED, Literal(True)))

    if point_uri_raw is not None:
        point = URIRef(str(point_uri_raw))
        g.add((point, RDF.type, VIRTUAL_POINT))
        if label is not None:
            g.add((point, RDFS.label, Literal(label)))
        if ref_uri is not None:
            g.add((point, HAS_EXTERNAL_REFERENCE, ref_uri))
    elif ref_uri is not None and label is not None:
        # No point to hang it on, so the reference carries its own label.
        g.add((ref_uri, RDFS.label, Literal(label)))

    target = ref_uri if ref_uri is not None else (
        URIRef(str(point_uri_raw)) if point_uri_raw is not None else None
    )
    if target is not None:
        for pred, value in (stream.get("properties") or {}).items():
            _add(g, target, pred, value)
