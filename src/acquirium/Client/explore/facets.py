"""Multi-attribute facet summaries for the explore layer.

``Query.facets()`` gives the notebook-first overview of one node: for every
registry attribute applicable to the node's role, the distinct values and
counts. Each attribute falls back through three scopes until one yields
values:

- **matched** — aggregated over the current query pattern (``Query.options``);
- **model** — model-wide usage of the attribute, ignoring the pattern;
- **vocabulary** — taxonomy enumeration from the loaded ontologies, for
  attributes with a bounded vocabulary (``medium``/``substance``/``type``),
  so an empty model still shows what *could* be used.

The vocabulary WHERE fragments are copied from the server's embedding
extraction (``Server/manager.py::_extract_concepts_for_embedding``) — the
client deliberately does not import server code.

Model and vocabulary results are pattern-independent, so they are cached
per ``(server, attribute, graph_version)`` and survive across queries until
the graph changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import polars as pl
from rdflib import RDFS

from acquirium.internals.internals_namespaces import (
    HAS_ENUMERATION_KIND,
    HAS_MEDIUM,
    OF_SUBSTANCE,
    OWL_CLASS,
    S223,
    WATR,
)
from acquirium.Client.explore.attributes import Attr
from acquirium.Client.explore.compile import attr_pred_path

# Vocabulary fragments lifted from Server/manager.py (_extract_concepts_for
# _embedding); ?uri is the projected variable.
_CLASS_VOCAB_WHERE = f"""
  {{ ?uri a <{RDFS.Class}> . }}
  UNION {{ ?uri a <{OWL_CLASS}> . }}
  UNION {{ ?x <{RDFS.subClassOf}> ?uri . }}
  UNION {{ ?x a ?uri . }}
  UNION {{ ?uri a <{WATR.Class}> . }}
  UNION {{ ?uri <{RDFS.subClassOf}> ?x . }}
  UNION {{ ?x <{HAS_ENUMERATION_KIND}> ?uri . }}
  UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
  UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
"""

_SUBSTANCE_VOCAB_WHERE = f"""
  {{ ?uri (<{RDFS.subClassOf}>)* <{S223['EnumerationKind-Substance']}> . }}
  UNION {{ ?uri (<{RDFS.subClassOf}>)* <{WATR['Medium-Constituent']}> . }}
  UNION {{ ?x <{S223.ofMedium}> ?uri . }}
  UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
  UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
"""

# attr name -> vocabulary fragment (attrs without one stop at model scope)
VOCAB_FRAGMENTS: Dict[str, str] = {
    "type": _CLASS_VOCAB_WHERE,
    "medium": _SUBSTANCE_VOCAB_WHERE,
    "substance": _SUBSTANCE_VOCAB_WHERE,
}

_VOCAB_LIMIT = 200

# (server_key, scope, attr_name, graph_version) -> [(value, count), ...]
_FACET_CACHE: Dict[tuple, list] = {}


def clear_facet_cache() -> None:
    _FACET_CACHE.clear()


def _server_key(client) -> str:
    return str(getattr(client, "base_url", id(client)))


def _rows(res: dict, value_col: str, count_col: Optional[str]) -> list:
    cols = res.get("columns", [])
    vi = cols.index(value_col) if value_col in cols else 0
    ci = cols.index(count_col) if count_col and count_col in cols else None
    out = []
    for r in res.get("rows", []):
        if r[vi] is None:
            continue
        out.append((str(r[vi]), int(r[ci]) if ci is not None else 0))
    return out


def model_options(client, attr: Attr, version: int) -> list:
    """Model-wide (pattern-independent) value counts for one attribute."""
    key = (_server_key(client), "model", attr.name, version)
    if key not in _FACET_CACHE:
        # Model-wide, so no single node role applies; use the most permissive
        # one so values reachable only through a reference are still counted.
        pred_path = attr_pred_path(attr, "data")
        sparql = (
            "SELECT ?opt (COUNT(DISTINCT ?x) AS ?count)\n"
            f"WHERE {{\n  ?x ({pred_path}) ?opt .\n}}\n"
            "GROUP BY ?opt\nORDER BY DESC(?count)"
        )
        res = client.sparql_query(sparql, include_dependencies=True)
        _FACET_CACHE[key] = _rows(res, "opt", "count")
    return _FACET_CACHE[key]


def vocab_options(client, attr: Attr, version: int) -> list:
    """Vocabulary-level values (count 0) for attrs with a bounded taxonomy."""
    fragment = VOCAB_FRAGMENTS.get(attr.name)
    if fragment is None:
        return []
    key = (_server_key(client), "vocabulary", attr.name, version)
    if key not in _FACET_CACHE:
        sparql = (
            "SELECT DISTINCT ?uri\n"
            f"WHERE {{\n{fragment}\n  FILTER(isIRI(?uri))\n}}\n"
            f"LIMIT {_VOCAB_LIMIT}"
        )
        res = client.sparql_query(sparql, include_dependencies=True)
        _FACET_CACHE[key] = _rows(res, "uri", None)
    return _FACET_CACHE[key]


@dataclass
class FacetSummary:
    """Per-attribute value counts for one node, with the scope each came from."""

    node_alias: str
    frames: Dict[str, pl.DataFrame] = field(default_factory=dict)
    scopes: Dict[str, str] = field(default_factory=dict)

    def __getitem__(self, attr_name: str) -> pl.DataFrame:
        return self.frames[attr_name]

    def __contains__(self, attr_name: str) -> bool:
        return attr_name in self.frames

    def attrs(self) -> list:
        return list(self.frames)

    def __repr__(self) -> str:
        lines = [f"FacetSummary({self.node_alias!r})"]
        for name, df in self.frames.items():
            scope = self.scopes.get(name, "matched")
            if df.height == 0:
                lines.append(f"  {name}: (no values)")
                continue
            shown = [
                f"{v} ({c})" if scope != "vocabulary" else str(v)
                for v, c in zip(df[name].head(5).to_list(), df["count"].head(5).to_list())
            ]
            more = f", … +{df.height - 5}" if df.height > 5 else ""
            lines.append(f"  {name} [{scope}]: " + ", ".join(shown) + more)
        return "\n".join(lines)
