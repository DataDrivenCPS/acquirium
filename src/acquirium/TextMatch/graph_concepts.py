"""Class / predicate / substance / process / role concept extraction.

Pure extractor, the graph-side counterpart of :class:`QUDTStore`: it holds the
SPARQL query of each kind and turns the query's rows into concept dicts (uri,
kind, label, surfaces, exact_surfaces, related) for the embedding index. This
module does no querying, parsing, fetching, or disk caching.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from rdflib import RDFS
from rdflib.namespace import SKOS

from acquirium.internals.internals_namespaces import (
    HAS_ENUMERATION_KIND,
    HAS_MEDIUM,
    OF_SUBSTANCE,
    OWL_CLASS,
    OWL_DATA_PROP,
    OWL_OBJ_PROP,
    RDF_PROP,
    S223,
    WATR,
)
from acquirium.TextMatch.embedding_matcher import _split_local_name

_LABEL_BLOCK = f"""
  OPTIONAL {{
    {{ ?uri <{RDFS.label}> ?label . }}
    UNION {{ ?uri <{SKOS.prefLabel}> ?label . }}
    UNION {{ ?uri <{SKOS.altLabel}> ?label . }}
    FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
  }}
"""

# A role ("backwash", "permeate", "primary") qualifies equipment; it is not
# equipment. The s223 role enumeration, which the NAWI roles extend, plus
# whatever the loaded model actually uses as a role. An enumeration member is
# both a subclass and an instance of its enumeration kind; either one counts.
# (?roleHolder, not ?x: the class query binds ?x, and this block is also used
# inside its FILTER NOT EXISTS.)
_ROLE_ROOT = S223["EnumerationKind-Role"]
_IS_ROLE = f"""
  {{ ?uri (<{RDFS.subClassOf}>)* <{_ROLE_ROOT}> . }}
  UNION {{ ?uri a <{_ROLE_ROOT}> . }}
  UNION {{ ?roleHolder <{S223.hasRole}> ?uri . }}
"""
_ROLE_WHERE = _IS_ROLE

_CLASS_WHERE = f"""
  {{ ?uri a <{RDFS.Class}> . }}
  UNION {{ ?uri a <{OWL_CLASS}> . }}
  UNION {{ ?x <{RDFS.subClassOf}> ?uri . }}
  UNION {{ ?x a ?uri . }}
  UNION {{ ?uri a <{WATR.Class}> . }}
  UNION {{ ?uri <{RDFS.subClassOf}> ?x . }}
  UNION {{ ?x <{HAS_ENUMERATION_KIND}> ?uri . }}
  UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
  UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
  FILTER NOT EXISTS {{ ?uri (<{RDFS.subClassOf}>)* <{WATR.Process}> . }}
  FILTER(!STRSTARTS(STR(?uri), "{WATR}Process"))
  FILTER NOT EXISTS {{ {_IS_ROLE} }}
"""

_PREDICATE_WHERE = f"""
  {{ ?uri a <{RDF_PROP}> . }}
  UNION {{ ?uri a <{OWL_OBJ_PROP}> . }}
  UNION {{ ?uri a <{OWL_DATA_PROP}> . }}
  UNION {{ ?s ?uri ?o . }}
"""

# Constrained medium/substance space: the s223 substance enumeration and the
# NAWI water medium taxonomy, plus whatever the loaded model actually uses as
# a medium/substance (self-grounding so it's correct regardless of the
# imported s223 closure).
_SUBSTANCE_WHERE = f"""
  {{ ?uri (<{RDFS.subClassOf}>)* <{S223['EnumerationKind-Substance']}> . }}
  UNION {{ ?uri (<{RDFS.subClassOf}>)* <{WATR['Medium-Constituent']}> . }}
  UNION {{ ?x <{S223.ofMedium}> ?uri . }}
  UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
  UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
"""

# Constrained process space: the NAWI process taxonomy plus whatever the
# loaded model actually uses as a process (self-grounding, like substances).
# Its own kind so process filters never rank equipment classes ("reverse
# osmosis" must hit Process-ReverseOsmosis, not ReverseOsmosisMembrane).
_PROCESS_WHERE = f"""
  {{ ?uri (<{RDFS.subClassOf}>)* <{WATR.Process}> . }}
  UNION {{ ?x <{WATR.hasProcess}> ?uri . }}
  UNION {{ ?uri a <{WATR.Class}> . FILTER(STRSTARTS(STR(?uri), "{WATR}Process")) }}
"""

_WHERE = {
    "class": _CLASS_WHERE,
    "predicate": _PREDICATE_WHERE,
    "substance": _SUBSTANCE_WHERE,
    "process": _PROCESS_WHERE,
    "role": _ROLE_WHERE,
}

_INITIALISM_MIN_WORDS = 3
_WORD_BREAK = re.compile(r"[\s\-/]+")


def _local_name(uri: str) -> str:
    return uri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


class GraphConcepts:
    """Extract class / predicate / substance / process / role concepts from SPARQL rows.

    Processes and roles are kinds of their own and are left out of ``class``,
    so "condenser" as a class is the equipment and as a role is the role.
    """

    KINDS: tuple[str, ...] = ("class", "predicate", "substance", "process", "role")

    @staticmethod
    def concept_query(kind: str) -> str:
        """SELECT ``?uri ?label`` for every concept of *kind*.

        One row per English or untagged label, plus an unbound-label row for
        concepts with none (their local name still yields a surface).
        """
        return f"""
            SELECT DISTINCT ?uri ?label WHERE {{
              {_WHERE[kind]}
              {_LABEL_BLOCK}
              FILTER(isIRI(?uri))
            }}
            """

    @staticmethod
    def extract_concepts(rows: Iterable[tuple[Any, ...]], kind: str) -> list[dict[str, Any]]:
        """Concept dicts from :meth:`concept_query` rows, sorted by URI.

        SPARQL row order is not stable across processes; labels are sorted and
        URIs are iterated in sorted order so the concept dicts hash identically
        run to run (otherwise the embedding cache misses every restart).
        """
        uri_labels: dict[str, set[str]] = {}
        for row in rows:
            uri = str(row[0]) if row[0] else None
            label = str(row[1]).strip('"') if row[1] else None
            if not uri:
                continue
            bucket = uri_labels.setdefault(uri, set())
            if label:
                bucket.add(label)

        concepts: list[dict[str, Any]] = []
        for uri in sorted(uri_labels):
            labels = sorted(uri_labels[uri])
            surfaces: list[str] = []
            for lbl in labels:
                lbl_lower = lbl.lower()
                if lbl_lower not in surfaces:
                    surfaces.append(lbl_lower)
            tokens = _split_local_name(uri)
            # "Role-Backwash" is asked for as "backwash". Within the role kind
            # the bare name is unambiguous; within class it was not, because
            # "condenser" is also a piece of equipment.
            if kind == "role" and len(tokens) > 1 and tokens[0] == "role":
                bare = " ".join(tokens[1:])
                if bare not in surfaces:
                    surfaces.append(bare)
            if tokens:
                joined = " ".join(tokens)
                if joined not in surfaces:
                    surfaces.append(joined)
            display_label = labels[0] if labels else (" ".join(tokens) if tokens else uri)
            concepts.append({
                "uri": uri,
                "kind": kind,
                "label": display_label,
                "surfaces": surfaces,
                "related": [],
            })
        return concepts

    @staticmethod
    def add_initialisms(concepts: list[dict[str, Any]]) -> None:
        """Add the initialism of a long label as an exact-only surface.

        Practitioners type "VFD", "SBR", "CSTR" for equipment whose ontology
        label is spelled out. A label of three or more words gives its initial
        letters as an ``exact_surfaces`` entry (looked up, never embedded),
        but only when one concept of that kind owns them and they are not
        already a surface of that kind. The same local name in the water and
        s223 ontologies counts as one owner. Predicates get none.

        Takes the concepts of every kind at once: uniqueness is per kind.
        """
        def _initialism(surface: str) -> str | None:
            words = [w for w in _WORD_BREAK.split(surface) if w]
            if len(words) < _INITIALISM_MIN_WORDS or not all(w[0].isalpha() for w in words):
                return None
            return "".join(w[0] for w in words)

        owners: dict[tuple[str, str], set[str]] = {}
        candidates: list[tuple[dict[str, Any], str]] = []
        taken: set[tuple[str, str]] = set()
        for c in concepts:
            if c["kind"] == "predicate":
                continue
            local = _local_name(c["uri"])
            # "Constituent-DissolvedOxygen" splits to "constituent dissolved
            # oxygen": the enumeration prefix is not part of the name.
            prefixed = " ".join(_split_local_name(c["uri"])) if "-" in local else None
            for s in c["surfaces"]:
                taken.add((c["kind"], s.lower()))
                if s == prefixed or "(" in s:
                    continue
                ini = _initialism(s)
                if ini:
                    owners.setdefault((c["kind"], ini), set()).add(local)
                    candidates.append((c, ini))
        for c, ini in candidates:
            if len(owners[(c["kind"], ini)]) == 1 and (c["kind"], ini) not in taken:
                exact = c.setdefault("exact_surfaces", [])
                if ini not in exact:
                    exact.append(ini)
