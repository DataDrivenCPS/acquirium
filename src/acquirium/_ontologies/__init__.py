"""Bundled ontology files loaded by default at server startup.

The TTL files in this package directory ship inside the acquirium wheel
and are loaded explicitly into ontoenv at startup. Each file is paired
with a **canonical IRI** that acquirium guarantees to register it under;
the original ``owl:Ontology`` declaration is rewritten in-graph to the
canonical IRI when the bundled file's declared IRI differs (the QUDT
files declare version-specific IRIs but acquirium exposes them at
versionless URLs).

The bundled set and their canonical IRIs:

- ``water.ttl``        — ``WATER_IRI``       (NAWI Water)
- ``s223.ttl``         — ``S223_IRI``        (ASHRAE 223P)
- ``qudt_unit.ttl``    — ``QUDT_UNIT_IRI``   (QUDT units, versionless canonical)
- ``qudt_qk.ttl``      — ``QUDT_QK_IRI``     (QUDT quantity kinds, versionless canonical)
- ``ref-schema.ttl``   — ``REF_SCHEMA_IRI``  (Brick's external-reference vocabulary)

Users can override any of these (or add more) via the ``[ontologies]
sources`` list in ``acquirium.toml``::

    [ontologies]
    sources = [
        # Add as-is — registered under whatever IRI the file declares.
        "./local-extensions.ttl",
        # Replace a bundled graph: load the source and rewrite its
        # declared ontology IRI to the `as` value.
        { source = "https://qudt.org/3.3.0/vocab/unit",
          as = "https://qudt.org/vocab/unit" },
    ]
"""

from __future__ import annotations

import logging
from importlib.resources import files
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF

_logger = logging.getLogger("acquirium.ontologies")


# Canonical ontology IRIs. These are part of the package's public API:
# the embedding-index pipeline in Manager and the QUDT converter look
# up named graphs by these exact IRIs.
WATER_IRI = "urn:nawi-water-ontology"
S223_IRI = "http://data.ashrae.org/standard223/1.0/model/all"
QUDT_UNIT_IRI = "https://qudt.org/vocab/unit"
QUDT_QK_IRI = "https://qudt.org/vocab/quantitykind"
REF_SCHEMA_IRI = "https://brickschema.org/schema/Brick/ref"

# (filename, canonical IRI) pairs. The loader rewrites each file's
# declared owl:Ontology IRI to its canonical IRI before handing the
# graph to ontoenv. Bundled QUDT files declare version-specific IRIs
# (e.g. http://qudt.org/3.2.1/vocab/unit) which we collapse onto the
# versionless canonical URL so user overrides slot in cleanly.
BUNDLED_FILES: tuple[tuple[str, str], ...] = (
    ("water.ttl",      WATER_IRI),
    ("s223.ttl",       S223_IRI),
    ("qudt_unit.ttl",  QUDT_UNIT_IRI),
    ("qudt_qk.ttl",    QUDT_QK_IRI),
    ("ref-schema.ttl", REF_SCHEMA_IRI),
)

BUNDLED_IRIS: tuple[str, ...] = tuple(iri for _, iri in BUNDLED_FILES)


def bundled_dir() -> Path:
    """Return the on-disk path of the bundled ontologies directory.

    ``importlib.resources.files`` returns a real ``Path`` for any normal
    pip / uv / editable install — the directory ships unpacked inside
    site-packages. We don't try to support zip-imported installs.
    """
    return Path(str(files(__name__)))


def rename_ontology_iri(graph: Graph, frm: URIRef, to: URIRef) -> None:
    """Rewrite *frm* → *to* in subject and object positions, in place.

    Mirrors ontoenv's ``--rename`` semantics:

    - ``<frm> rdf:type owl:Ontology``  → ``<to> rdf:type owl:Ontology``
    - ``<frm> owl:imports <X>``        → ``<to> owl:imports <X>``
    - ``<frm> sh:prefixes <frm>``      → ``<to> sh:prefixes <to>``
    - ``<X>   sh:prefixes <frm>``      → ``<X>   sh:prefixes <to>``
    - ``<frm> owl:versionIRI <frm>``   → ``<to> owl:versionIRI <frm>``
      (the version-value object is intentionally preserved so the
      version provenance survives the rename).
    """
    if frm == to:
        return
    to_add: list = []
    to_remove: list = []
    for s, p, o in graph:
        new_s = to if s == frm else s
        new_o = to if (o == frm and p != OWL.versionIRI) else o
        if new_s is s and new_o is o:
            continue
        to_remove.append((s, p, o))
        to_add.append((new_s, p, new_o))
    for t in to_remove:
        graph.remove(t)
    for t in to_add:
        graph.add(t)


def load_bundled_graph(filename: str, canonical_iri: str) -> Graph:
    """Parse one bundled TTL and rewrite its declared ontology IRI to *canonical_iri*.

    The returned graph is ready to be handed to ``OntoEnv.add(graph, ...)``.
    """
    path = bundled_dir() / filename
    g = Graph()
    g.parse(path, format="turtle")
    declared = next(iter(g.subjects(RDF.type, OWL.Ontology)), None)
    if declared is None:
        _logger.warning(
            "bundled %s: no owl:Ontology declaration — leaving graph untouched", filename
        )
        return g
    target = URIRef(canonical_iri)
    if str(declared) != str(target):
        rename_ontology_iri(g, URIRef(str(declared)), target)
    return g
