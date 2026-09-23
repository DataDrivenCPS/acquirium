"""Declarative attribute registry for the explore query layer.

Each :class:`Attr` maps a user-facing attribute name (``medium``,
``quantity_kind``, ...) to the RDF predicate(s) that express it, the text
resolver kind used to turn free text into URIs, and the node roles it
applies to. The registry is the single source of truth consumed by the
explore compiler (``compile.py``) and the ``where()`` resolution layer.

Built-in attributes live in :data:`REGISTRY`. User attributes written by
``insert_metadata`` are discovered from the graph: every predicate under
``urn:acquirium:attr#`` is one, named by its local part (the flattened leaf
path, ``product_info.year``). A list index segment (``tags.0``) is also
offered collapsed (``tags``: every index at once), which is how a bare
comparison on a list means "some element". :class:`Registry` is the view a
``Query`` reads: the built-ins plus what the connected server holds, cached
per graph version.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

from rdflib.namespace import RDF, RDFS

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_ATTR_NS,
    CONNECTION_POINT,
    DATA_SOURCE,
    HAS_ENUMERATION_KIND,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    OF_MEDIUM,
    OF_SUBSTANCE,
    PRODUCED_BY,
    WATR,
)

ENTITY = frozenset({"entity"})
DATA = frozenset({"data"})
BOTH = ENTITY | DATA


@dataclass(frozen=True)
class Not:
    """Negation marker: match only nodes where the attribute value is absent.

    Example::

        q.where(medium=Not("brine"))   # FILTER NOT EXISTS in SPARQL
    """

    value: Any


@dataclass(frozen=True)
class Attr:
    """One queryable attribute.

    - ``predicates``: predicate URI alternatives (OR-union in SPARQL).
    - ``kind``: resolver kind passed to ``client.resolve`` when
      the user supplies free text instead of a URI.
    - ``roles``: which node roles the attribute applies to ("entity"/"data").
    - ``via_subclass``: the value is a class matched through the object's
      ``rdf:type``/``rdfs:subClassOf*`` (compiled with the anchored
      sub-SELECT fence; see ``Query.to_sparql``).
    - ``literal``: the value is a plain literal (never resolved to a URI).
    """

    name: str
    predicates: tuple[str, ...]
    kind: str
    roles: frozenset[str]
    via_subclass: bool = False
    literal: bool = False
    doc: str = ""  # one-liner for generated docstrings / facet displays


REGISTRY: dict[str, Attr] = {
    a.name: a
    for a in (
        # rdf:type of the node itself, subclass-closed.
        Attr("type", (str(RDF.type),), "class", BOTH, via_subclass=True,
             doc='class of the node, subclass-closed ("tank" matches all tank kinds)'),
        # watr:hasProcess object, resolved within the process taxonomy (its
        # own extraction kind, so equipment classes never outrank processes).
        Attr("process", (str(WATR.hasProcess),), "process", ENTITY, via_subclass=True,
             doc='treatment process the entity performs ("ozonation", "reverse osmosis")'),
        # Class of a connection point hanging off the entity
        # (s223:hasConnectionPoint -> ?cp a <class>).
        Attr("cp_type", (str(CONNECTION_POINT),), "class", ENTITY, via_subclass=True,
             doc='class of one of the entity\'s connection points ("outlet connection point")'),
        # Properties carry s223:ofMedium; connection points carry
        # s223:hasMedium. One attribute, OR-union of both predicates.
        Attr("medium", (str(OF_MEDIUM), str(HAS_MEDIUM)), "class", BOTH,
             doc='carried medium: ofMedium|hasMedium ("fluid water", "air", "brine")'),
        Attr("substance", (str(OF_SUBSTANCE),), "substance", DATA,
             doc='measured substance/constituent ("chlorine", "organics", "ammonia")'),
        Attr("quantity_kind", (str(HAS_QUANTITY_KIND),), "quantity_kind", DATA,
             doc='QUDT quantity kind ("volume flow rate", "turbidity", "acidity")'),
        Attr("unit", (str(HAS_UNIT),), "unit", DATA,
             doc='QUDT unit ("mg/l", "PSI", "NTU")'),
        Attr("enumeration_kind", (str(HAS_ENUMERATION_KIND),), "class", DATA,
             doc='enumeration kind of a state/enum property ("on off", "run status")'),
        # Origin tag literal on a data node (e.g. "Lab", "SCADA").
        Attr("data_source", (str(DATA_SOURCE),), "any", DATA, literal=True,
             doc='origin tag literal, matched verbatim ("Lab", "SCADA")'),
        # Name of the app that derived this measurement, recorded by the
        # materializer. Absent on measurements that came from a driver.
        Attr("app", (str(PRODUCED_BY),), "any", DATA, literal=True,
             doc='app that derived the measurement ("normalize-temperatures")'),
        # rdfs:label literal on a data node (stream label / CSV column name).
        Attr("label", (str(RDFS.label),), "any", BOTH, literal=True,
             doc='stream or entity label, matched verbatim ("Influent flow")'),
    )
}

# Attributes include("all") leaves out, per node role. type and cp_type
# project one row per asserted type / connection point; a measurement's
# label is already a metadata() column.
NOT_IN_ALL = {
    "entity": frozenset({"type", "cp_type"}),
    "data": frozenset({"type", "app", "label"}),
}


ATTR_NS = str(ACQUIRIUM_ATTR_NS)

# One segment of a user attribute path: a key of the value map, or a list
# index. Keys are identifier-like so the predicate stays a readable IRI and
# the dot stays the separator; an all-digit segment is a list index.
KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def check_key(key: object) -> str:
    """Validate one metadata key; returns it. Raises ``ValueError`` otherwise."""
    if not isinstance(key, str) or not KEY_PATTERN.match(key):
        raise ValueError(
            f"invalid metadata key {key!r}: letters, digits, underscore and hyphen, "
            "starting with a letter or underscore"
        )
    return key


def valid_path(path: str) -> bool:
    """A dotted path whose segments are all valid keys or list indices."""
    return bool(path) and all(
        seg.isdigit() or KEY_PATTERN.match(seg) for seg in path.split(".")
    )

# Data graphs only: the ``attr:`` namespace never appears in an ontology, and
# the union with dependencies is orders of magnitude larger to scan.
DISCOVERY_SPARQL = (
    "SELECT DISTINCT ?p\nWHERE {\n  ?s ?p ?o .\n"
    f"  FILTER(STRSTARTS(STR(?p), \"{ATTR_NS}\"))\n}}"
)

# (server_key, graph_version) -> {name: Attr}
_DISCOVERED_CACHE: Dict[tuple, Dict[str, Attr]] = {}


def clear_registry_cache() -> None:
    _DISCOVERED_CACHE.clear()


def _server_key(client) -> str:
    return str(getattr(client, "base_url", id(client)))


def _collapse_indices(path: str) -> Optional[str]:
    """``a.0.b`` -> ``a.b``; ``None`` when the path has no index segment."""
    parts = path.split(".")
    kept = [seg for seg in parts if not seg.isdigit()]
    return ".".join(kept) if len(kept) != len(parts) and kept else None


def _index_key(predicate: str) -> tuple:
    """Sort key placing ``tags.2`` after ``tags.10`` numerically, not lexically."""
    return tuple(
        (0, int(seg)) if seg.isdigit() else (1, seg)
        for seg in predicate[len(ATTR_NS):].split(".")
    )


def user_attributes(predicates: "list[str] | tuple[str, ...]") -> Dict[str, Attr]:
    """Build the discovered-attribute map from the ``attr:`` predicates in use.

    Every predicate yields its exact leaf path; paths with list indices also
    yield the collapsed name, whose predicates are all indices found.
    """
    groups: Dict[str, list] = {}
    for pred in predicates:
        pred = str(pred)
        if not pred.startswith(ATTR_NS):
            continue
        path = pred[len(ATTR_NS):]
        if not valid_path(path):
            continue
        groups.setdefault(path, []).append(pred)
        collapsed = _collapse_indices(path)
        if collapsed is not None:
            groups.setdefault(collapsed, []).append(pred)
    return {
        name: Attr(name, tuple(sorted(set(preds), key=_index_key)), "any", BOTH,
                   literal=True, doc="user attribute")
        for name, preds in groups.items()
        if name not in REGISTRY
    }


class Registry(Mapping):
    """Built-in attributes plus those discovered from the connected server.

    Built-ins answer without a server round trip; anything else triggers one
    discovery query, cached per ``(server, graph_version)`` until the graph
    changes. With no client only the built-ins exist.
    """

    def __init__(self, client=None):
        self.client = client

    def _discovered(self) -> Dict[str, Attr]:
        if self.client is None:
            return {}
        key = (_server_key(self.client), self.client.graph_version())
        found = _DISCOVERED_CACHE.get(key)
        if found is None:
            res = self.client.sparql_query(DISCOVERY_SPARQL, include_dependencies=False)
            cols = res.get("columns", [])
            pi = cols.index("p") if "p" in cols else 0
            found = user_attributes(
                [str(r[pi]) for r in res.get("rows", []) if r and r[pi] is not None]
            )
            _DISCOVERED_CACHE[key] = found
        return found

    def __getitem__(self, name: str) -> Attr:
        attr = REGISTRY.get(name)
        if attr is not None:
            return attr
        return self._discovered()[name]

    def __contains__(self, name: object) -> bool:
        return name in REGISTRY or name in self._discovered()

    def __iter__(self) -> Iterator[str]:
        yield from REGISTRY
        yield from self._discovered()

    def __len__(self) -> int:
        return len(REGISTRY) + len(self._discovered())

    def for_role(self, role: str) -> "list[Attr]":
        return [a for a in self.values() if role in a.roles]


def normalize_value(v: Any) -> tuple[list[Any], bool]:
    """Normalize a user-supplied attribute value for the compiler.

    Unwraps a :class:`Not` marker, coerces scalars to single-element lists,
    and drops ``None`` entries. Returns ``(values, negated)``.
    """
    negated = isinstance(v, Not)
    if negated:
        v = v.value
    if isinstance(v, (list, tuple, set, frozenset)):
        values = [x for x in v if x is not None]
    else:
        values = [v] if v is not None else []
    return values, negated


def attributes_doc(indent: int = 8) -> str:
    """The registry rendered as a docstring block (single source of truth)."""
    pad = " " * indent
    role = {frozenset({"entity"}): "entity", frozenset({"data"}): "measurement",
            BOTH: "both"}
    width = max(len(n) for n in REGISTRY) + 2
    lines = [f"{pad}Attributes (usable on):"]
    for a in REGISTRY.values():
        lines.append(f"{pad}  {a.name:<{width}}{role[a.roles]:<12}{a.doc}")
    return "\n".join(lines)
