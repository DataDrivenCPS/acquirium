"""Behavioral tests for bundled-ontology loading and user-source overrides.

These exercise OxigraphGraphStore against a *real* OntoEnv (no Docker
services). They verify that:

1. All four bundled IRIs are registered with ontoenv after a cold start
   with no extra sources.
2. A user source declaring a brand-new IRI is loaded additively
   (bundled IRIs still present, new IRI is queryable as a named graph).
3. A user source declaring an IRI matching a bundled one **replaces**
   the bundled graph's contents. This relies on ``env.add(...,
   overwrite=True)``; ontoenv's default behavior is to keep the
   pre-existing graph and skip the new source, so this test would fail
   if the override flag is ever dropped.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rdflib import Graph, URIRef

from acquirium._ontologies import BUNDLED_IRIS, WATER_IRI
from acquirium.Storage.graph_store import OxigraphGraphStore


_OVERRIDE_MARKER = URIRef("urn:acquirium-test:override-marker")
_OVERRIDE_VALUE = URIRef("urn:acquirium-test:override-value")
_NEW_ONT_IRI = "urn:acquirium-test:brand-new-ontology"


def _write_ttl(path: Path, ontology_iri: str, *, marker_subject: URIRef | None = None) -> Path:
    """Write a minimal TTL file declaring *ontology_iri* as an owl:Ontology.

    Optionally adds one distinctive triple so tests can detect whether
    this file's content (vs. some other source declaring the same IRI)
    is what ended up in the store.
    """
    lines = [
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "",
        f"<{ontology_iri}> a owl:Ontology .",
    ]
    if marker_subject is not None:
        lines.append(
            f"<{marker_subject}> rdfs:seeAlso <{_OVERRIDE_VALUE}> ."
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _make_store(tmp_path: Path, *, extra_sources: list[str] | None = None) -> OxigraphGraphStore:
    return OxigraphGraphStore(
        store_path=tmp_path / "store",
        env_root=tmp_path / "env",
        extra_ontology_sources=extra_sources,
    )


def test_bundled_iris_load_by_default(tmp_path: Path) -> None:
    """No extra sources: ontoenv finds all four bundled ontology IRIs."""
    store = _make_store(tmp_path)
    try:
        registered = set(store.env.get_ontology_names())
        for iri in BUNDLED_IRIS:
            assert iri in registered, f"bundled IRI not registered: {iri}"
    finally:
        store.close()


def test_extra_source_with_new_iri_is_additive(tmp_path: Path) -> None:
    """A user source declaring a fresh IRI must not displace bundled graphs."""
    src = _write_ttl(tmp_path / "extra.ttl", _NEW_ONT_IRI)
    store = _make_store(tmp_path, extra_sources=[str(src)])
    try:
        registered = set(store.env.get_ontology_names())
        # All bundled IRIs still present.
        for iri in BUNDLED_IRIS:
            assert iri in registered, f"bundled IRI dropped: {iri}"
        # The new graph is queryable via ontoenv.
        assert _NEW_ONT_IRI in registered
    finally:
        store.close()


def test_extra_source_replaces_bundled_graph_on_iri_match(tmp_path: Path) -> None:
    """A user source declaring a bundled IRI must replace that bundled graph.

    Tests ``OxigraphGraphStore.__init__``'s use of
    ``env.add(..., overwrite=True)`` — with ontoenv's default
    ``overwrite=False``, the new source is silently skipped and the
    bundled graph stays. This test would catch that regression because
    the replacement file's distinctive marker triple would be absent.
    """
    target_iri = WATER_IRI
    src = _write_ttl(
        tmp_path / "water_override.ttl",
        target_iri,
        marker_subject=_OVERRIDE_MARKER,
    )
    store = _make_store(tmp_path, extra_sources=[str(src)])
    try:
        named: Graph = store.named_graph(target_iri)
        # The override's marker triple is present.
        assert (
            _OVERRIDE_MARKER,
            URIRef("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
            _OVERRIDE_VALUE,
        ) in named, "user-supplied source did not replace bundled graph contents"

        # And the original bundled water ontology's content is gone — we
        # pick an arbitrary class that the bundled water.ttl defines but
        # the override file does not declare.
        from rdflib.namespace import RDF, OWL

        bundled_classes = list(named.triples((None, RDF.type, OWL.Class)))
        assert not bundled_classes, (
            f"bundled water classes still present after override "
            f"({len(bundled_classes)} owl:Class triples)"
        )
    finally:
        store.close()
