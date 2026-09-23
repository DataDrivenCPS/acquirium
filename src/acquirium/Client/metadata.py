"""Turn an ``insert_metadata`` value map into triples and one SPARQL update.

A value map is ``{key: value}`` on one node. Keys fall into three kinds:

- a built-in attribute of the explore registry (``unit``, ``medium``,
  ``label``, ``type``, ...): written with that attribute's own predicate,
  text resolved to a URI first;
- anything else: a user attribute, ``urn:acquirium:attr#<leaf path>``, one
  predicate per leaf of the (possibly nested) value.

A relation name (``entity``, ``measurement``, ``upstream``, ``downstream``)
is refused: edges of the plant model are not written here.

Nested dicts and lists flatten to dotted leaf paths, ``product_info.year``,
``tags.0``; order and duplicates of a list survive as indices. Scalars are
typed literals; a string that looks like a URI is a URI. ``None`` on a
top-level key removes it.

The update replaces every touched key: delete what the node carries under
the key (the leaf itself and everything under ``key.``), then insert the
new leaves. Keys not mentioned stay. The update is scoped to the graph it
runs in, the reserved metadata source graph, so it never retracts a triple
from the plant model or a driver's graph.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from rdflib import Literal, URIRef
from rdflib.namespace import RDF, RDFS

from acquirium.Client.explore.attributes import (
    ATTR_NS,
    REGISTRY,
    Attr,
    check_key,
)
from acquirium.Client.explore.relations import RELATIONS
from acquirium.internals.internals_namespaces import (
    HAS_MEDIUM,
    OF_MEDIUM,
    WATR,
)

# Built-ins whose write predicate depends on the node: s223 gives a
# Property ``ofMedium`` and a connection or connection point ``hasMedium``.
WRITE_PREDICATES: Dict[str, Dict[str, str]] = {
    "medium": {"data": str(OF_MEDIUM), "entity": str(HAS_MEDIUM)},
}
# Built-ins with no single triple to write: ``cp_type`` is the class of a
# connection point hanging off the entity, which would need a new node.
NOT_WRITABLE = frozenset({"cp_type"})


def is_uri(value: Any) -> bool:
    return isinstance(value, URIRef) or (
        isinstance(value, str) and ("://" in value or value.startswith("urn:"))
    )


def to_term(value: Any) -> "Literal | URIRef":
    """The RDF term for one scalar: typed literal, or URI for URI-like text.

    The query compiler renders the same Python values with the same
    ``n3()`` forms, so an equality filter matches what was written.
    """
    if isinstance(value, URIRef):
        return value
    if isinstance(value, bool) or isinstance(value, (int, float, datetime, date)):
        return Literal(value)
    if isinstance(value, str):
        return URIRef(value) if is_uri(value) else Literal(value)
    raise TypeError(
        f"unsupported metadata value {value!r}: use str, int, float, bool, date, "
        "datetime, a URI, or a dict/list of those"
    )


def flatten(values: Mapping[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """``{key: value}`` -> ``({leaf path: scalar}, [keys to remove])``.

    Keys are validated with :func:`~explore.attributes.check_key`; list
    indices are 0-based. A ``None`` at top level removes the key; a nested
    ``None`` is skipped (the key is still replaced as a whole).
    """
    leaves: Dict[str, Any] = {}
    removed: List[str] = []

    def walk(prefix: str, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, Mapping):
            for k, v in value.items():
                walk(f"{prefix}.{check_key(k)}", v)
        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                walk(f"{prefix}.{i}", v)
        else:
            leaves[prefix] = value

    for key, value in values.items():
        check_key(key)
        if value is None:
            removed.append(key)
            continue
        walk(key, value)
    return leaves, removed


@dataclass
class Write:
    """The triples and replacements one node's value map produces."""

    subject: URIRef
    # predicates replaced on the subject (built-ins)
    replace_forward: List[str] = field(default_factory=list)
    # user attribute keys replaced (leaf and everything under ``key.``)
    replace_keys: List[str] = field(default_factory=list)
    triples: List[tuple] = field(default_factory=list)

    @property
    def signature(self) -> tuple:
        return (tuple(self.replace_forward), tuple(self.replace_keys))


def _builtin_predicate(attr: Attr, role: str) -> str:
    by_role = WRITE_PREDICATES.get(attr.name)
    if by_role is not None:
        return by_role[role]
    return attr.predicates[0]


def plan_write(
    subject: str,
    values: Mapping[str, Any],
    *,
    role: str,
    resolve: Callable[[str, str], Optional[str]],
) -> Write:
    """Build the :class:`Write` for one node.

    ``role`` is ``"data"`` or ``"entity"`` (which predicate a built-in
    takes, and whether it applies). ``resolve(text, kind)`` turns text on a
    URI-valued built-in into a URI, or ``None``.
    """
    w = Write(subject=URIRef(subject))
    leaves, removed = flatten(values)

    for key, value in values.items():
        attr = REGISTRY.get(key)
        if attr is None and (key in RELATIONS or key == "measurement"):
            raise ValueError(
                f"{key!r} names a relation of the plant model; insert_metadata writes "
                "attributes only")
        if attr is not None:
            if key in NOT_WRITABLE:
                raise ValueError(f"attribute {key!r} cannot be written: it describes another node")
            if role not in attr.roles:
                raise ValueError(f"attribute {key!r} does not apply to a {role} node")
            pred = _builtin_predicate(attr, role)
            w.replace_forward.append(pred)
            if value is None:
                continue
            items = value if isinstance(value, (list, tuple)) else [value]
            for v in items:
                if v is None:
                    continue
                if isinstance(v, (Mapping, list, tuple)):
                    raise ValueError(f"attribute {key!r} takes scalar values, got {v!r}")
                if attr.literal:
                    term = Literal(v) if not isinstance(v, URIRef) else v
                elif is_uri(v):
                    term = URIRef(str(v))
                else:
                    uri = resolve(str(v), attr.kind)
                    if uri is None:
                        raise ValueError(f"Could not resolve {v!r} as {attr.kind} for attribute {key!r}")
                    term = URIRef(uri)
                w.triples.append((w.subject, URIRef(pred), term))
        else:
            w.replace_keys.append(key)
    for path, scalar in leaves.items():
        if path.split(".", 1)[0] in REGISTRY:
            continue
        w.triples.append((w.subject, URIRef(ATTR_NS + path), to_term(scalar)))
    return w


def update_text(writes: Iterable[Write]) -> str:
    """One SPARQL update for the writes: the deletes, then one INSERT DATA.

    Nodes with the same set of replaced keys share a DELETE. Meant to run
    inside the metadata graph (the store scopes an update to its graph), so
    no GRAPH clause.
    """
    groups: Dict[tuple, List[Write]] = {}
    for w in writes:
        groups.setdefault(w.signature, []).append(w)
    ops: List[str] = []
    for (forward, keys), members in groups.items():
        values = " ".join(w.subject.n3() for w in members)
        if forward or keys:
            terms = [f"?p = <{p}>" for p in forward]
            terms += [f"?p = <{ATTR_NS}{k}>" for k in keys]
            terms += [f'STRSTARTS(STR(?p), "{ATTR_NS}{k}.")' for k in keys]
            ops.append(
                f"DELETE {{ ?s ?p ?o }}\nWHERE {{\n  VALUES ?s {{ {values} }}\n"
                f"  ?s ?p ?o .\n  FILTER({' || '.join(terms)})\n}}"
            )
    triples = [t for w in writes for t in w.triples]
    if triples:
        body = "\n".join(f"  {s.n3()} {p.n3()} {o.n3()} ." for s, p, o in triples)
        ops.append(f"INSERT DATA {{\n{body}\n}}")
    return " ;\n".join(ops)


# Fields a stream registration already takes at top level; a ``metadata``
# key naming one of them is merged into the stream (see register_streams).
STREAM_FIELDS = frozenset({"unit", "quantity_kind", "medium", "substance", "data_source", "label"})
