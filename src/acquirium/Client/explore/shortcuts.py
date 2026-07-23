"""Named traversal shortcuts and the hidden-predicate list.

A :class:`Shortcut` names *one meaningful step* through the RDF plumbing —
"the next equipment", "a downstream property" — as a set of alternative
:class:`Step` chains (the step matches if any chain does). Definitions are
structured data, not SPARQL: each :class:`Step` is a predicate (URI,
namespace constant, or free text resolved through the server) plus an
optional class constraint on the node it lands on::

    register_shortcut(Shortcut("dosing", (
        (Step("feeds chemical to"),),                       # free text, resolved on use
        (Step(S223.connectedTo, node="chemical feeder"),),  # constant + class check
    )))

Queries compose shortcuts in ``via`` with ``/`` and repeat them with ``*``
(shortcuts are not always repeatable — an entity→property shortcut cannot
follow itself, so repetition is explicit)::

    related(via="next_equipment", ...)                        # exactly one step
    related(via="next_equipment*/downstream_property", ...)   # 0..max_depth equipment
                                                              # steps, then one property step

Compilation and traversal do **not** consume ``Shortcut`` objects: the query
core lowers a ``via`` expression into a plain-tuple *step program* (see
:func:`compile_program` docs in ``core.py``), so the shortcut layer can
evolve without touching the compiler or the BFS machinery.

Separately, :func:`hide` maintains predicates that generic (``via="any"``)
traversal must never follow — scaffolding edges like ``s223:cnx``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

from rdflib import URIRef

from acquirium.internals.internals_namespaces import S223


@dataclass(frozen=True)
class Step:
    """One hop of a shortcut chain.

    - ``predicate``: URI, namespace constant, or free text (server-resolved
      when the shortcut is used); prefix with ``"^"`` to invert.
    - ``node``: optional class (URI or free text) the hop must land on,
      matched through ``rdf:type``/``rdfs:subClassOf*``.
    """

    predicate: Union[str, URIRef]
    node: Union[str, URIRef, None] = None


@dataclass(frozen=True)
class Shortcut:
    """A named step: alternatives of Step chains; the step matches any chain."""

    name: str
    alternatives: tuple  # tuple[tuple[Step, ...], ...]
    description: str = ""


NEXT_EQUIPMENT = Shortcut(
    "next_equipment",
    ((Step(str(S223.connectedTo)),),),
    "The next connected entity downstream (s223:connectedTo only).",
)

DOWNSTREAM_PROPERTY = Shortcut(
    "downstream_property",
    (
        # via own outlet connection point
        (Step(str(S223.hasConnectionPoint), node=str(S223.OutletConnectionPoint)),
         Step(str(S223.hasProperty))),
        # on the outgoing connection itself
        (Step(f"^{S223.connectsFrom}"), Step(str(S223.hasProperty))),
        # via the inlet connection point of the next entity
        (Step(f"^{S223.connectsFrom}"), Step(str(S223.connectsAt), node=str(S223.InletConnectionPoint)),
         Step(str(S223.hasProperty))),
        # directly on the next connected entity
        (Step(str(S223.connectedTo)), Step(str(S223.hasProperty))),
    ),
    "Properties observable immediately downstream of an entity.",
)

DOWNSTREAM_EQUIPMENT = Shortcut(
    "downstream_equipment",
    (
        (Step(str(S223.connectedTo)),),
        (Step(f"^{S223.connectedFrom}"),),
        (Step(str(S223.connectedThrough)), Step(str(S223.connectsTo))),
        (Step(f"^{S223.connectsFrom}"), Step(f"^{S223.connectedThrough}")),
    ),
    "The next entity downstream, via any s223 connection pattern (one step).",
)

UPSTREAM_EQUIPMENT = Shortcut(
    "upstream_equipment",
    (
        (Step(f"^{S223.connectedTo}"),),
        (Step(str(S223.connectedFrom)),),
        (Step(str(S223.connectedThrough)), Step(str(S223.connectsFrom))),
        (Step(f"^{S223.connectsTo}"), Step(f"^{S223.connectedThrough}")),
    ),
    "The next entity upstream, via any s223 connection pattern (one step).",
)

UPSTREAM_PROPERTY = Shortcut(
    "upstream_property",
    (
        (Step(str(S223.hasConnectionPoint), node=str(S223.InletConnectionPoint)),
         Step(str(S223.hasProperty))),
        (Step(f"^{S223.connectsTo}"), Step(str(S223.hasProperty))),
        (Step(f"^{S223.connectsTo}"), Step(str(S223.connectsAt), node=str(S223.OutletConnectionPoint)),
         Step(str(S223.hasProperty))),
        (Step(f"^{S223.connectedTo}"), Step(str(S223.hasProperty))),
    ),
    "Properties observable immediately upstream of an entity.",
)

SHORTCUTS: dict[str, Shortcut] = {
    s.name: s
    for s in (NEXT_EQUIPMENT, DOWNSTREAM_EQUIPMENT, UPSTREAM_EQUIPMENT,
              DOWNSTREAM_PROPERTY, UPSTREAM_PROPERTY)
}

_RESERVED = {"any", "all"}


def register_shortcut(shortcut: Shortcut) -> None:
    """Register (or replace) a shortcut for use in ``via`` expressions."""
    if shortcut.name in _RESERVED:
        raise ValueError(f"shortcut name {shortcut.name!r} is reserved")
    if "/" in shortcut.name or shortcut.name.endswith("*"):
        raise ValueError(f"shortcut name {shortcut.name!r} may not contain '/' or end with '*'")
    if not shortcut.alternatives or any(not chain for chain in shortcut.alternatives):
        raise ValueError("shortcut must define at least one non-empty Step chain")
    SHORTCUTS[shortcut.name] = shortcut


def get_shortcut(name: str) -> Optional[Shortcut]:
    return SHORTCUTS.get(name)


# ---------------- hidden predicates ----------------

_HIDDEN: set[str] = set()


def hide(*predicates) -> None:
    """Never follow these predicates in generic (``via="any"``) traversal.

    Accepts full URIs or rdflib URIRefs (e.g. ``hide(S223.cnx)``).
    """
    for p in predicates:
        s = str(p)
        if not (s.startswith("urn:") or s.startswith("http://") or s.startswith("https://")):
            raise ValueError(
                f"hide: {p!r} is not a URI; pass a full URI or a namespace constant like S223.cnx"
            )
        _HIDDEN.add(s)


def unhide(*predicates) -> None:
    """Remove predicates from the hidden set (all of them when called bare)."""
    if not predicates:
        _HIDDEN.clear()
        return
    for p in predicates:
        _HIDDEN.discard(str(p))


def hidden_predicates() -> frozenset[str]:
    return frozenset(_HIDDEN)
