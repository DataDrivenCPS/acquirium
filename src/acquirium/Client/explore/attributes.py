"""Declarative attribute registry for the explore query layer.

Each :class:`Attr` maps a user-facing attribute name (``medium``,
``quantity_kind``, ...) to the RDF predicate(s) that express it, the text
resolver kind used to turn free text into URIs, and the node roles it
applies to. The registry is the single source of truth consumed by the
explore compiler (``compile.py``) and the ``where()`` resolution layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rdflib.namespace import RDF

from acquirium.internals.internals_namespaces import (
    CONNECTION_POINT,
    DATA_SOURCE,
    HAS_ENUMERATION_KIND,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    OF_MEDIUM,
    OF_SUBSTANCE,
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
    - ``via_ref``: on a measurement node, also match the value through
      ``ref:hasExternalReference``. Stream semantics are written on the
      external reference, while a user's model may put them on the point —
      matching both means a query finds either without the caller having to
      know which. The point still wins when both are present and disagree
      (see ``DataObject._resolve_effective_units``).
    """

    name: str
    predicates: tuple[str, ...]
    kind: str
    roles: frozenset[str]
    via_subclass: bool = False
    literal: bool = False
    via_ref: bool = False
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
        Attr("medium", (str(OF_MEDIUM), str(HAS_MEDIUM)), "substance", BOTH,
             via_ref=True,
             doc='carried medium: ofMedium|hasMedium ("fluid water", "air", "brine")'),
        Attr("substance", (str(OF_SUBSTANCE),), "substance", DATA, via_ref=True,
             doc='measured substance/constituent ("chlorine", "organics", "ammonia")'),
        Attr("quantity_kind", (str(HAS_QUANTITY_KIND),), "quantity_kind", DATA,
             via_ref=True,
             doc='QUDT quantity kind ("volume flow rate", "turbidity", "acidity")'),
        Attr("unit", (str(HAS_UNIT),), "unit", DATA, via_ref=True,
             doc='QUDT unit ("mg/l", "PSI", "NTU")'),
        Attr("enumeration_kind", (str(HAS_ENUMERATION_KIND),), "class", DATA,
             doc='enumeration kind of a state/enum property ("on off", "run status")'),
        # Origin tag literal on a reference node (e.g. "Lab", "SCADA").
        Attr("data_source", (str(DATA_SOURCE),), "any", DATA, literal=True,
             via_ref=True,
             doc='origin tag literal, matched verbatim ("Lab", "SCADA")'),
    )
}


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


#: Display name for each node role, in the order roles are listed.
_ROLE_LABELS: dict[str, str] = {
    "entity": "entity",
    "data": "measurement",
    "stream": "stream",
}


def _roles_label(roles: frozenset[str]) -> str:
    """Render a role set for display.

    Computed from the set rather than looked up by exact frozenset, so adding
    a role or a new role combination cannot raise at import time.
    """
    if roles == ENTITY | DATA:
        return "both"
    names = [_ROLE_LABELS.get(r, r) for r in _ROLE_LABELS if r in roles]
    names += sorted(r for r in roles if r not in _ROLE_LABELS)
    return "/".join(names) if names else "-"


def attributes_doc(indent: int = 8) -> str:
    """The registry rendered as a docstring block (single source of truth)."""
    pad = " " * indent
    width = max(len(n) for n in REGISTRY) + 2
    lines = [f"{pad}Attributes (usable on):"]
    for a in REGISTRY.values():
        lines.append(f"{pad}  {a.name:<{width}}{_roles_label(a.roles):<12}{a.doc}")
    return "\n".join(lines)
