"""Pure graph construction for stream registration."""
from typing import Any, Callable
import warnings
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import compute_ref_uri
from acquirium.Storage.values import normalize_value_kind
from acquirium.Client.explore.attributes import raw_predicate
from acquirium.Client.metadata import is_uri

# Known point-metadata fields → the resolver ``kind`` each is resolved as.
# The field name is the semantic role, so callers supply no ``kind``.
POINT_FIELD_KINDS: dict[str, str] = {
    "unit": "unit",
    "quantity_kind": "quantity_kind",
    "medium": "substance",
    "substance": "substance",
}

# The same fields → the predicate each is written under on the point. Extend
# both maps together when adding a semantic field; ``data_source`` is a plain
# literal and stays outside (it is not resolved).
POINT_FIELD_PREDICATES: dict[str, URIRef] = {
    "unit": HAS_UNIT,
    "quantity_kind": HAS_QUANTITY_KIND,
    "medium": HAS_MEDIUM,
    "substance": OF_SUBSTANCE,
}


def _add_triple(g: Graph, subj: URIRef, pred: URIRef, value: "str | URIRef | None") -> None:
    if value is None:
        return
    if isinstance(value, URIRef):
        g.add((subj, pred, value))
    elif "://" in str(value) or str(value).startswith("urn:"):
        g.add((subj, pred, URIRef(str(value))))
    else:
        g.add((subj, pred, Literal(value)))


def _coerce_resolved(
    resolved: dict[str, "str | None"], name: str, value: Any,
) -> "str | URIRef | None":
    """Map a raw field value to its resolved URIRef.

    Passes ``None``/``URIRef`` through; warns and returns ``None`` when
    plain text did not resolve, so the field is skipped as if it had not
    been supplied.
    """
    if value is None:
        return None
    if isinstance(value, URIRef):
        return value
    uri = resolved.get(name)
    if uri is None:
        warnings.warn(
            f"Could not resolve {name!r} value {value!r} to a QUDT URI; "
            "skipping",
            stacklevel=3,
        )
        return None
    return URIRef(uri)


def _build_stream_triples(
    g: Graph, stream: dict, resolved: dict[str, "str | None"],
    existing: dict[str, "str | None"],
    units_compatible: "Callable[[Any, Any], bool]",
) -> None:
    """Write one stream's reference/point/metadata triples into ``g``.

    ``resolved`` is the precomputed ``field -> URI-or-None`` map for this
    stream's semantic fields (typically from ``resolve_point_metadata``);
    pass ``{}`` when there is nothing to resolve.

    ``existing`` is the metadata the point already carries in the server
    graph (from ``AcquiriumClient._point_metadata``); pass ``{}`` for a
    point that does not exist yet. A provided field the point lacks is
    added; a conflicting value raises ``ValueError``. Units are the one
    exception: a provided unit that differs from the point's is accepted
    when ``units_compatible`` says the two are convertible, and is written
    on the external reference as the unit the raw rows are stored in.
    """
    point_uri_raw = stream.get("point_uri")
    label = stream.get("label")
    source_id = stream.get("source_id")
    ref_name = stream.get("ref_name")
    ref_uri = None
    if ref_name is not None and source_id is not None:
        ref_uri = compute_ref_uri(source_id, ref_name)
        g.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
        g.add((ref_uri, ACQUIRIUM_REF_NAME,  Literal(ref_name)))
        # Only assert a value kind the caller supplied; a default would
        # contradict a later data-derived kind on the same reference node.
        if stream.get("value_kind") is not None:
            g.add((
                ref_uri,
                ACQUIRIUM_VALUE_KIND,
                Literal(normalize_value_kind(stream["value_kind"])),
            ))
        g.add((ref_uri, STORED_AT,           ACQUIRIUM_DB_URI))
        g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))

    if point_uri_raw is not None:
        subj = URIRef(str(point_uri_raw))
    else:
        if ref_uri is None:
            raise ValueError(
                "a stream registration needs a point_uri or a "
                "(source_id, ref_name) pair to mint one"
            )
        subj = URIRef(f"{ref_uri}__point")
        if label is None:
            label = ref_name

    g.add((subj, RDF.type, VIRTUAL_POINT))
    if label is not None:
        g.add((subj, RDFS.label, Literal(label)))

    for field, pred in POINT_FIELD_PREDICATES.items():
        raw = stream.get(field)
        if isinstance(raw, str) and not is_uri(raw):
            # the free text as written, kept even when it did not resolve
            g.add((subj, URIRef(raw_predicate(field)), Literal(raw)))
        provided = _coerce_resolved(resolved, field, raw)
        if provided is None:
            continue
        current = existing.get(field)
        if current is None:
            _add_triple(g, subj, pred, provided)
        elif str(current) == str(provided):
            if field == "unit" and ref_uri is not None:
                _add_triple(g, ref_uri, HAS_UNIT, provided)
        elif field != "unit":
            raise ValueError(
                f"{field} mismatch for point <{subj}>: the point has "
                f"<{current}>, the stream provided <{provided}>"
            )
        elif not units_compatible(provided, current):
            raise ValueError(
                f"unit mismatch for point <{subj}>: the point has <{current}>, "
                f"the stream provided <{provided}> and they are not convertible"
            )
        elif ref_uri is None:
            raise ValueError(
                f"stream for point <{subj}> stores data in <{provided}> but has "
                "no (source_id, ref_name) reference to record the storage unit on"
            )
        else:
            # Convertible: the provided unit is what the raw rows are stored
            # in; data access defaults to the point's unit and converts.
            _add_triple(g, ref_uri, HAS_UNIT, provided)

    data_source = stream.get("data_source")
    if data_source is not None:
        current = existing.get("data_source")
        if current is None:
            _add_triple(g, subj, DATA_SOURCE, data_source)
        elif str(current) != str(data_source):
            raise ValueError(
                f"data_source mismatch for point <{subj}>: the point has "
                f"{current!r}, the stream provided {data_source!r}"
            )

    if ref_uri is not None:
        g.add((subj, HAS_EXTERNAL_REFERENCE, ref_uri))

    target = ref_uri if ref_uri is not None else subj
    for pred, value in (stream.get("properties") or {}).items():
        _add_triple(g, target, pred, value)
    return subj
