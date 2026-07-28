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
    """

    name: str
    predicates: tuple[str, ...]
    kind: str
    roles: frozenset[str]
    via_subclass: bool = False
    literal: bool = False


REGISTRY: dict[str, Attr] = {
    a.name: a
    for a in (
        # rdf:type of the node itself, subclass-closed.
        Attr("type", (str(RDF.type),), "class", BOTH, via_subclass=True),
        # watr:hasProcess object, matched by its class (there is no
        # dedicated "process" resolver kind; processes are classes).
        Attr("process", (str(WATR.hasProcess),), "class", ENTITY, via_subclass=True),
        # Class of a connection point hanging off the entity
        # (s223:hasConnectionPoint -> ?cp a <class>).
        Attr("cp_type", (str(CONNECTION_POINT),), "class", ENTITY, via_subclass=True),
        # Properties carry s223:ofMedium; connection points carry
        # s223:hasMedium. One attribute, OR-union of both predicates.
        Attr("medium", (str(OF_MEDIUM), str(HAS_MEDIUM)), "class", BOTH),
        Attr("substance", (str(OF_SUBSTANCE),), "substance", DATA),
        Attr("quantity_kind", (str(HAS_QUANTITY_KIND),), "quantity_kind", DATA),
        Attr("unit", (str(HAS_UNIT),), "unit", DATA),
        Attr("enumeration_kind", (str(HAS_ENUMERATION_KIND),), "class", DATA),
        # Origin tag literal on a reference node (e.g. "Lab", "SCADA").
        Attr("data_source", (str(DATA_SOURCE),), "any", DATA, literal=True),
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
