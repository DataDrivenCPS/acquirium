"""Hidden predicates: edges generic (``via="any"``) traversal never follows.

Attribute predicates are hidden by default: the explore layer treats them as
node *attributes* (query them with where()/include()/options()), not plant
edges. Traversing them walks into attribute-value hub nodes (e.g.
s223:Fluid-Water) or the ontology TBox (rdf:type/subClassOf into QUDT),
which is both semantically wrong and the main any-traversal fan-out
explosion. Explicitly named predicates (``via=`` values, direction patterns)
are never filtered — naming a predicate overrides hiding. Edges onto a
measurement node do not walk predicates at all: they follow the ``entity``
relation of ``explore.relations`` (hasProperty, actuatedByProperty).
"""
from __future__ import annotations

from rdflib.namespace import RDFS

from acquirium.Client.explore.attributes import ATTR_NS, REGISTRY
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, S223

# s223:cnx is not an attribute but a redundant scaffolding shorthand for the
# connection topology — hidden by default for the same fan-out reason.
DEFAULT_HIDDEN: frozenset[str] = frozenset(
    {str(p) for attr in REGISTRY.values() for p in attr.predicates}
    | {str(RDFS.subClassOf), str(S223.hasProperty), str(HAS_EXTERNAL_REFERENCE),
       str(S223.cnx)}
)

# Namespaces hidden as a whole: user metadata predicates are attributes too,
# but they are discovered at runtime, so the wildcard filter names the prefix.
# A prefix is any string ending in "#" or "/"; hide()/unhide() accept them.
DEFAULT_HIDDEN_PREFIXES: frozenset[str] = frozenset({ATTR_NS})

_USER_HIDDEN: set[str] = set()
_USER_UNHIDDEN: set[str] = set()
_USER_HIDDEN_PREFIXES: set[str] = set()
_USER_UNHIDDEN_PREFIXES: set[str] = set()


def _is_prefix(s: str) -> bool:
    return s.endswith("#") or s.endswith("/")


def hide(*predicates) -> None:
    """Never follow these predicates in generic (``via="any"``) traversal.

    Accepts full URIs or rdflib URIRefs (e.g. ``hide(S223.cnx)``). Attribute
    predicates (see ``DEFAULT_HIDDEN``) are hidden out of the box, and so is
    the whole ``attr:`` namespace. A string ending in ``#`` or ``/`` hides
    every predicate under that namespace.
    """
    for p in predicates:
        s = str(p)
        if not (s.startswith("urn:") or s.startswith("http://") or s.startswith("https://")):
            raise ValueError(
                f"hide: {p!r} is not a URI; pass a full URI or a namespace constant like S223.cnx"
            )
        if _is_prefix(s):
            _USER_HIDDEN_PREFIXES.add(s)
            _USER_UNHIDDEN_PREFIXES.discard(s)
            continue
        _USER_HIDDEN.add(s)
        _USER_UNHIDDEN.discard(s)


def unhide(*predicates) -> None:
    """Lift specific predicates (defaults included); bare call resets to defaults."""
    if not predicates:
        _USER_HIDDEN.clear()
        _USER_UNHIDDEN.clear()
        _USER_HIDDEN_PREFIXES.clear()
        _USER_UNHIDDEN_PREFIXES.clear()
        return
    for p in predicates:
        s = str(p)
        if _is_prefix(s):
            _USER_HIDDEN_PREFIXES.discard(s)
            _USER_UNHIDDEN_PREFIXES.add(s)
            continue
        _USER_HIDDEN.discard(s)
        _USER_UNHIDDEN.add(s)


def hidden_predicates() -> frozenset[str]:
    return frozenset((DEFAULT_HIDDEN | _USER_HIDDEN) - _USER_UNHIDDEN)


def hidden_prefixes() -> frozenset[str]:
    return frozenset((DEFAULT_HIDDEN_PREFIXES | _USER_HIDDEN_PREFIXES) - _USER_UNHIDDEN_PREFIXES)


def hidden_filter(pvar: str) -> "str | None":
    """The SPARQL FILTER excluding hidden predicates and namespaces from
    ``pvar``, or None when nothing is hidden."""
    hidden = sorted(hidden_predicates())
    terms = [f"{pvar} NOT IN (" + ", ".join(f"<{h}>" for h in hidden) + ")"] if hidden else []
    terms += [f'!STRSTARTS(STR({pvar}), "{prefix}")' for prefix in sorted(hidden_prefixes())]
    return "FILTER(" + " && ".join(terms) + ")" if terms else None
