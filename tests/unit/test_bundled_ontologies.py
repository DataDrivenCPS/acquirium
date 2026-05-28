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

from acquirium._ontologies import (
    BUNDLED_IRIS,
    QUDT_UNIT_IRI,
    WATER_IRI,
)
from acquirium.Server.config import OntologySource
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


def _make_store(
    tmp_path: Path, *, extra_sources: list[OntologySource] | None = None,
) -> OxigraphGraphStore:
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
    store = _make_store(tmp_path, extra_sources=[OntologySource(source=str(src))])
    try:
        registered = set(store.env.get_ontology_names())
        # All bundled IRIs still present.
        for iri in BUNDLED_IRIS:
            assert iri in registered, f"bundled IRI dropped: {iri}"
        # The new graph is queryable via ontoenv.
        assert _NEW_ONT_IRI in registered
    finally:
        store.close()


def test_rename_replaces_bundled_graph_at_canonical_iri(tmp_path: Path) -> None:
    """An ``OntologySource`` with ``rename_to=<bundled IRI>`` replaces that
    bundled graph at the canonical IRI, regardless of what the source
    file itself declares.

    Exercises the ``_add_user_source`` path: parse the source, rewrite
    its declared owl:Ontology IRI to the canonical key, and add with
    ``overwrite=True``.
    """
    # The override file declares a *different* IRI on purpose — the
    # rename machinery must remap it to WATER_IRI on load.
    src = _write_ttl(
        tmp_path / "water_override.ttl",
        ontology_iri="https://example.com/some-other-iri",
        marker_subject=_OVERRIDE_MARKER,
    )
    store = _make_store(
        tmp_path,
        extra_sources=[OntologySource(source=str(src), rename_to=WATER_IRI)],
    )
    try:
        named: Graph = store.named_graph(WATER_IRI)
        # The override's marker triple is present at the canonical IRI.
        assert (
            _OVERRIDE_MARKER,
            URIRef("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
            _OVERRIDE_VALUE,
        ) in named, "renamed user source did not land at canonical IRI"

        # And the original bundled water content is gone.
        from rdflib.namespace import RDF, OWL

        bundled_classes = list(named.triples((None, RDF.type, OWL.Class)))
        assert not bundled_classes, (
            f"bundled water classes still present after rename override "
            f"({len(bundled_classes)} owl:Class triples)"
        )

        # The source's originally-declared IRI must NOT also be registered:
        # the rename moves the graph, it doesn't duplicate it.
        registered = set(store.env.get_ontology_names())
        assert "https://example.com/some-other-iri" not in registered
    finally:
        store.close()


def test_qudt_iris_are_versionless_canonical(tmp_path: Path) -> None:
    """The bundled QUDT 3.2.1 files must register under the versionless
    canonical IRIs (not their declared version-specific IRIs)."""
    store = _make_store(tmp_path)
    try:
        registered = set(store.env.get_ontology_names())
        assert QUDT_UNIT_IRI in registered, registered
        # Version-specific IRI must NOT be registered separately.
        assert "http://qudt.org/3.2.1/vocab/unit" not in registered
    finally:
        store.close()
