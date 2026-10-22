"""Lossless metadata documents and their queryable attribute projections.

Documents use private predicates, independent of the attribute path syntax.
Their typed JSON encoding preserves arbitrary object keys, empty containers,
null positions, and the dates and URIs accepted by the metadata writer.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
import json
from urllib.parse import quote, unquote

from rdflib import Literal, URIRef

DOCUMENT_NS = "urn:acquirium:metadata:document:"
DOCUMENT_TYPE = URIRef("urn:acquirium:metadata:typed-json-v1")


def _encode(value):
    if isinstance(value, URIRef):
        return ["uri", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("metadata object keys must be strings")
        return ["object", {key: _encode(child) for key, child in value.items()}]
    if isinstance(value, (list, tuple)):
        return ["array", [_encode(child) for child in value]]
    if value is None or isinstance(value, (str, bool, int, float)):
        return ["value", value]
    raise TypeError(f"unsupported metadata value {value!r}")


def _decode(item):
    kind, value = item
    if kind == "object":
        return {key: _decode(child) for key, child in value.items()}
    if kind == "array":
        return [_decode(child) for child in value]
    if kind == "date":
        return date.fromisoformat(value)
    if kind == "datetime":
        return datetime.fromisoformat(value)
    if kind == "uri":
        return value
    if kind == "value":
        return value
    raise ValueError(f"unknown metadata document type {kind!r}")


def dumps(value) -> str:
    return json.dumps(_encode(value), sort_keys=True, allow_nan=False, ensure_ascii=False)


def loads(value: str):
    return _decode(json.loads(value))


def document_predicate(key: str) -> URIRef:
    return URIRef(DOCUMENT_NS + quote(key, safe=""))


def document_triples(subject, values):
    # Validate the complete map before returning any triples.
    dumps(values)
    return [(URIRef(subject), document_predicate(key), Literal(dumps(value), datatype=DOCUMENT_TYPE))
            for key, value in values.items()]


def read_documents(client, subjects, *, include_dependencies=False):
    """Read complete documents in batches through the shared SPARQL seam."""
    subjects = sorted(set(map(str, subjects)))
    # Real backends read asserted source documents, independently of inference.
    # Query-only clients can use the same representation through SPARQL.
    if callable(getattr(type(client), "metadata_documents", None)):
        return client.metadata_documents(subjects)
    rows = []
    for offset in range(0, len(subjects), 500):
        selected = subjects[offset:offset + 500]
        query = "SELECT DISTINCT ?s ?p ?value ?g WHERE { VALUES ?s { " + " ".join(URIRef(s).n3() for s in selected) + " } GRAPH ?g { "
        query += f'?s ?p ?value . FILTER(STRSTARTS(STR(?p), "{DOCUMENT_NS}")) }} }}'
        rows.extend(client.sparql_query(query, include_dependencies=include_dependencies)["rows"])
    return decode_document_rows(subjects, rows)


def decode_document_rows(subjects, rows):
    from acquirium.Storage.graph_registry import source_graph_uri, METADATA_SOURCE_ID

    metadata_graph = str(source_graph_uri(METADATA_SOURCE_ID))
    result = {subject: {} for subject in subjects}
    priorities = {}
    for subject, predicate, value, graph in sorted(rows, key=lambda row: str(row[3]) == metadata_graph, reverse=True):
        subject = str(subject)
        key = unquote(str(predicate)[len(DOCUMENT_NS):])
        decoded = loads(str(value))
        priority = int(str(graph) == metadata_graph)
        previous = result[subject]
        old_priority = priorities.get((subject, key), -1)
        if priority < old_priority:
            continue
        if priority == old_priority and key in previous and previous[key] != decoded:
            raise ValueError(f"conflicting metadata documents for {subject!r}, key {key!r}")
        previous[key] = decoded
        priorities[subject, key] = priority
    return result


def matching_documents(client, subjects, values):
    """Select authoritative documents by exact saved field values.

    Query projections can contain assertions from several source owners.
    Complete document selection uses the same source precedence as reads and
    compares typed JSON, preserving list order and null positions.
    """
    expected = {key: dumps(value) for key, value in values.items()}
    documents = read_documents(client, subjects)
    return {subject for subject, document in documents.items()
            if all(dumps(document.get(key)) == value for key, value in expected.items())}


def projected_values(values):
    """Keep valid attribute paths; the complete document retains every key."""
    from acquirium.Client.explore.attributes import KEY_PATTERN

    if isinstance(values, Mapping):
        return {key: projected_values(value) for key, value in values.items() if KEY_PATTERN.fullmatch(key)}
    if isinstance(values, (list, tuple)):
        return [projected_values(value) for value in values]
    return values


def batch_update(operations):
    """Coalesce resolved node replacements and field patches into one update.

    The SQL outbox keeps delivery order. This produces at most three SPARQL
    operations for an arbitrarily large batch, rather than one per event.
    """
    nodes, replacements, predicates = {}, set(), set()
    for operation in operations:
        subject = operation["subject"]
        if operation["operation"] in {"replace", "delete"}:
            replacements.add(subject)
            nodes[subject] = {}
            predicates = {pair for pair in predicates if pair[0] != subject}
        else:
            for predicate in operation["predicates"]:
                nodes.setdefault(subject, {}).pop(predicate, None)
                if subject not in replacements:
                    predicates.add((subject, predicate))
        for _, predicate, value in operation.get("triples", []):
            nodes.setdefault(subject, {}).setdefault(predicate, set()).add(value)
    updates = []
    if replacements:
        values = " ".join(URIRef(subject).n3() for subject in sorted(replacements))
        updates.append(f"DELETE {{ ?s ?p ?o }} WHERE {{ VALUES ?s {{ {values} }} ?s ?p ?o }}")
    if predicates:
        values = " ".join(f"({URIRef(subject).n3()} {URIRef(predicate).n3()})" for subject, predicate in sorted(predicates))
        updates.append(f"DELETE {{ ?s ?p ?o }} WHERE {{ VALUES (?s ?p) {{ {values} }} ?s ?p ?o }}")
    triples = [f"{URIRef(subject).n3()} {URIRef(predicate).n3()} {value} ."
               for subject, fields in nodes.items() for predicate, values in fields.items() for value in sorted(values)]
    if triples:
        updates.append("INSERT DATA { " + "\n".join(triples) + " }")
    return ";\n".join(updates)
