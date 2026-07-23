"""Named traversal shortcuts and the hidden-predicate list.

A :class:`Shortcut` names a bundle of SPARQL path shapes: **one step of the
shortcut** matches the UNION of its patterns. This lets queries say
``related(via="next_equipment")`` instead of spelling out how equipment is
wired at the RDF level, and makes ``max_depth`` count meaningful steps.

Each pattern is either

- a bare SPARQL property path, e.g. ``"<...connectedTo>"`` or
  ``"^<...connectsFrom>/<...hasProperty>"`` — compiled as
  ``?s (path) ?t .``; or
- a graph-pattern template mentioning ``?s`` and ``?t``, for shapes a
  property path cannot express (like a type check on an intermediate node)::

      "?s <...hasConnectionPoint> ?m . ?m a <...OutletConnectionPoint> . "
      "?m <...hasProperty> ?t ."

  Internal variables (``?m`` above) are renamed per use, so patterns never
  collide with each other or the rest of the query.

Separately, :func:`hide` maintains a set of predicates that generic
(``via="any"``) traversal must never follow — scaffolding edges like
``s223:cnx`` that would otherwise leak into "any predicate" hops.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from acquirium.internals.internals_namespaces import S223

_CONNECTED_TO = f"<{S223.connectedTo}>"
_CONNECTS_FROM = f"<{S223.connectsFrom}>"
_CONNECTS_TO = f"<{S223.connectsTo}>"
_CONNECTS_AT = f"<{S223.connectsAt}>"
_HAS_CP = f"<{S223.hasConnectionPoint}>"
_HAS_PROPERTY = f"<{S223.hasProperty}>"
_OUTLET_CP = f"<{S223.OutletConnectionPoint}>"
_INLET_CP = f"<{S223.InletConnectionPoint}>"


@dataclass(frozen=True)
class Shortcut:
    """A named bundle of path shapes; one step = the union of the patterns."""

    name: str
    patterns: tuple[str, ...]
    description: str = ""


NEXT_EQUIPMENT = Shortcut(
    "next_equipment",
    (f"{_CONNECTED_TO}",),
    "The next connected entity downstream (s223:connectedTo only).",
)

DOWNSTREAM_PROPERTY = Shortcut(
    "downstream_property",
    (
        # via own outlet connection point
        f"?s {_HAS_CP} ?m . ?m a {_OUTLET_CP} . ?m {_HAS_PROPERTY} ?t .",
        # on the outgoing connection itself
        f"^{_CONNECTS_FROM}/{_HAS_PROPERTY}",
        # via the inlet connection point of the next entity
        f"?s ^{_CONNECTS_FROM}/{_CONNECTS_AT} ?m . ?m a {_INLET_CP} . ?m {_HAS_PROPERTY} ?t .",
        # directly on the next connected entity
        f"{_CONNECTED_TO}/{_HAS_PROPERTY}",
    ),
    "Properties observable immediately downstream of an entity.",
)

UPSTREAM_PROPERTY = Shortcut(
    "upstream_property",
    (
        f"?s {_HAS_CP} ?m . ?m a {_INLET_CP} . ?m {_HAS_PROPERTY} ?t .",
        f"^{_CONNECTS_TO}/{_HAS_PROPERTY}",
        f"?s ^{_CONNECTS_TO}/{_CONNECTS_AT} ?m . ?m a {_OUTLET_CP} . ?m {_HAS_PROPERTY} ?t .",
        f"^{_CONNECTED_TO}/{_HAS_PROPERTY}",
    ),
    "Properties observable immediately upstream of an entity.",
)

SHORTCUTS: dict[str, Shortcut] = {
    s.name: s for s in (NEXT_EQUIPMENT, DOWNSTREAM_PROPERTY, UPSTREAM_PROPERTY)
}

_RESERVED = {"any", "all"}


def register_shortcut(shortcut: Shortcut) -> None:
    """Register (or replace) a shortcut for use as ``via=<name>``."""
    if shortcut.name in _RESERVED:
        raise ValueError(f"shortcut name {shortcut.name!r} is reserved")
    if not shortcut.patterns:
        raise ValueError("shortcut must define at least one pattern")
    SHORTCUTS[shortcut.name] = shortcut


def get_shortcut(name: str) -> Shortcut:
    s = SHORTCUTS.get(name)
    if s is None:
        raise ValueError(f"unknown shortcut {name!r}; known: {sorted(SHORTCUTS)}")
    return s


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


# ---------------- pattern instantiation ----------------

_VAR = re.compile(r"\?([A-Za-z_]\w*)")


def instantiate_pattern(pattern: str, src_var: str, tgt_var: str, uid: str) -> str:
    """Render one shortcut pattern as WHERE-clause text between two variables.

    Bare property paths become ``src (path) tgt .``; templates get ``?s``/``?t``
    substituted and every other variable suffixed with ``uid``.
    """
    if "?s" not in pattern and "?t" not in pattern:
        return f"{src_var} ({pattern}) {tgt_var} ."

    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name == "s":
            return src_var
        if name == "t":
            return tgt_var
        return f"?{name}_{uid}"

    return _VAR.sub(repl, pattern).strip()
