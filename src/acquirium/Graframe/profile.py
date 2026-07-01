"""Discovery profiles — shape what a Graframe *shows* you.

An ontology exposes far more predicates than any one task cares about. A
:class:`Profile` curates the discovery surface (facets + named virtual edges)
without restricting what you *can* query: hidden predicates just don't clutter
the facets, and named paths become first-class, traversable facet rows.

A profile carries four things:

* ``allow`` / ``deny`` — predicate visibility. Entries are exact predicates
  (CURIE/URI) or namespace globs (a prefix ending in ``:`` like ``"s223:"``,
  optionally ``"s223:*"``, or a full namespace URI ending in ``#``/``/``).
* ``allow_types`` / ``deny_types`` — same, for object rdf:types in
  ``pred-obj-type`` facets (used to drop schema/shape objects).
* ``edges`` — named virtual edges: ``name -> path``. The path is a
  :class:`~acquirium.Graframe.algebra.Path`, a list of predicates
  (alternation), or a SPARQL property-path string (``"s223:connectedTo+"``).

**Visibility rule:** a predicate shows iff ``(allow empty OR matches allow)
AND (matches no deny)`` — ``allow`` sets the universe, ``deny`` carves out
exceptions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Profile:
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()
    allow_types: tuple[str, ...] = ()
    deny_types: tuple[str, ...] = ()
    edges: Mapping[str, Any] = field(default_factory=dict)

    def with_(
        self,
        *,
        allow: Sequence[str] = (),
        deny: Sequence[str] = (),
        allow_types: Sequence[str] = (),
        deny_types: Sequence[str] = (),
        edges: Mapping[str, Any] | None = None,
    ) -> "Profile":
        """Return a new Profile layering these additions on top of this one.

        Lists concatenate; ``edges`` merge (later definitions win). Handy for a
        house style plus per-domain specializations::

            base = Profile.base()
            water = base.with_(allow=["s223:", "nawi:", "qudt:"], edges={...})
        """
        return Profile(
            allow=self.allow + tuple(allow),
            deny=self.deny + tuple(deny),
            allow_types=self.allow_types + tuple(allow_types),
            deny_types=self.deny_types + tuple(deny_types),
            edges={**dict(self.edges), **dict(edges or {})},
        )

    @classmethod
    def base(cls) -> "Profile":
        """A ready-made profile that hides common schema/ontology noise.

        Drops the structural vocabularies (``rdf``/``rdfs``/``owl``/``sh`` and
        a few documentation namespaces) from predicate facets, and drops
        class/shape/ontology resources from object-type facets — the clutter
        the ontology closure injects. Compose domain predicates on top with
        :meth:`with_`.
        """
        return cls(
            deny=(
                "rdf:", "rdfs:", "owl:", "sh:", "skos:",
                "dcterms:", "dc:", "prov:", "vann:",
            ),
            deny_types=(
                "sh:NodeShape", "sh:Shape", "sh:PropertyShape",
                "rdfs:Class", "owl:Class", "owl:Ontology",
                "s223:Class", "nawi:Class",
            ),
        )

    # -- SPARQL filter construction ------------------------------------
    def predicate_filter(self, var: str, nsmap: Mapping[str, str], expand) -> str | None:
        """A ``FILTER(...)`` line restricting predicate var to the visible set, or None."""
        return _build_filter(var, self.allow, self.deny, nsmap, expand)

    def type_filter(self, var: str, nsmap: Mapping[str, str], expand) -> str | None:
        """A ``FILTER(...)`` line restricting object-type var to the visible set, or None."""
        return _build_filter(var, self.allow_types, self.deny_types, nsmap, expand)


def _build_filter(var, allow, deny, nsmap, expand) -> str | None:
    clauses: list[str] = []
    a = _group(var, allow, nsmap, expand)
    if a:
        clauses.append(f"({a})")  # parenthesize: && binds tighter than || in SPARQL
    d = _group(var, deny, nsmap, expand)
    if d:
        clauses.append(f"!({d})")
    if not clauses:
        return None
    return "FILTER(" + " && ".join(clauses) + ")"


def _group(var, entries, nsmap, expand) -> str | None:
    exacts: list[str] = []
    namespaces: list[str] = []
    for e in entries:
        kind, val = _resolve_entry(e, nsmap, expand)
        if val is None:
            continue
        if kind == "exact":
            exacts.append(val)
        elif kind == "ns":
            namespaces.append(val)
    parts: list[str] = []
    if exacts:
        parts.append(f"?{var} IN (" + ", ".join(f"<{u}>" for u in exacts) + ")")
    for ns in namespaces:
        parts.append(f'STRSTARTS(STR(?{var}), "{ns}")')
    if not parts:
        return None
    return " || ".join(parts)


def _resolve_entry(entry: str, nsmap: Mapping[str, str], expand) -> tuple[str, str | None]:
    s = entry
    if s.endswith("*"):
        s = s[:-1]
    if s.endswith("#") or s.endswith("/"):
        return ("ns", s)
    if s.endswith(":"):
        pfx = s[:-1]
        ns = nsmap.get(pfx)
        if ns is None:
            try:
                full = expand(pfx + ":x")
                ns = full[:-1] if full.endswith("x") else full
            except Exception:
                ns = None
        if ns is None:
            logger.debug("profile: unknown namespace prefix %r (ignored)", pfx)
            return ("skip", None)
        return ("ns", ns)
    try:
        return ("exact", expand(s))
    except Exception:
        logger.debug("profile: could not resolve predicate %r (ignored)", entry)
        return ("skip", None)
