"""Bundled ontology files loaded by default at server startup.

The TTL files in this package directory are shipped inside the acquirium
wheel and discovered by ontoenv on startup. Currently bundled (IRIs are
exported as module-level constants below):

- ``water.ttl``        — NAWI Water (``WATER_IRI``)
- ``s223.ttl``         — ASHRAE 223P (``S223_IRI``)
- ``qudt_unit.ttl``    — QUDT units 3.2.1 (``QUDT_UNIT_IRI``)
- ``qudt_qk.ttl``      — QUDT quantity kinds (``QUDT_QK_IRI``)
- ``ref-schema.ttl``   — Internal external-reference vocabulary (no
                         module-level constant; consumed implicitly via
                         the ``ref:`` namespace in inserted graphs)

Users can override any of these (or add more) via the ``[ontologies]``
table in ``acquirium.toml``::

    [ontologies]
    sources = [
        "https://example.com/my-water-ontology.ttl",
        "./local-extensions.ttl",
    ]

Each entry is registered with ontoenv. If a source declares an ontology
IRI matching one of the constants above, it replaces that bundled graph;
otherwise it is loaded additively.
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path


# Ontology IRIs declared by the bundled TTL files. These are part of the
# package's public API: the embedding-index pipeline in Manager and the
# QUDT converter look up named graphs by these exact IRIs.
WATER_IRI = "urn:nawi-water-ontology"
S223_IRI = "http://data.ashrae.org/standard223/1.0/model/all"
QUDT_UNIT_IRI = "http://qudt.org/3.2.1/vocab/unit"
QUDT_QK_IRI = "http://qudt.org/3.2.1/vocab/quantitykind"

# Convenience tuple for iteration / tests.
BUNDLED_IRIS: tuple[str, ...] = (WATER_IRI, S223_IRI, QUDT_UNIT_IRI, QUDT_QK_IRI)


def bundled_dir() -> Path:
    """Return the on-disk path of the bundled ontologies directory.

    Used as an ontoenv search directory at server startup. ontoenv reads
    each TTL inside, indexes the declared owl:Ontology IRI, and exposes
    the parsed graph via ``env.get_graph(iri)``. Downstream code (e.g.
    the QUDT converter) reaches the bundled ontologies through ontoenv
    by IRI, not by re-reading these files.

    ``importlib.resources.files`` returns a real ``Path`` for any normal
    pip / uv / editable install — the directory ships unpacked inside
    site-packages. We don't try to support zip-imported installs:
    ontoenv reads ontologies as filesystem paths, so a zip install
    would fail anyway.
    """
    return Path(str(files(__name__)))
