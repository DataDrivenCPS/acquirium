"""Naming rules for Acquirium's own named graphs.

Every graph Acquirium writes deployment data into lives under a URI it
controls (``urn:acquirium:...``), so ownership doesn't need to be tracked as
separate persisted state: a source's graph URI is a deterministic function of
its source_id, and "is this a deployment data graph" is answerable by
inspecting the URI alone. This deliberately does not look inside the RDF
dataset itself, so Acquirium's own bookkeeping never becomes input data for
SHACL inference or validation.
"""

from __future__ import annotations

from urllib.parse import quote

from rdflib import URIRef

# Preserve the existing main graph as the plant graph during migration.
PLANT_GRAPH_URI = "urn:acquirium#MainGraph"
# The plant model is a first-class deployment source. Public callers must use
# this ID instead of relying on an omitted source owner.
PLANT_SOURCE_ID = "plant"
ACQUIRIUM_GRAPH_URI = "urn:acquirium:graph:data:acquirium"
SOURCE_GRAPH_PREFIX = "urn:acquirium:graph:data:source:"


def source_graph_uri(source_id: str, *, plant_graph_uri: str = PLANT_GRAPH_URI) -> URIRef:
    """Return the data graph owned by ``source_id``.

    Deterministic and requires no lookup: the plant source maps to the
    reserved plant graph, and every other source_id maps to its own graph
    under ``SOURCE_GRAPH_PREFIX``.
    """
    if not source_id:
        raise ValueError("source_id must not be empty")
    if source_id == PLANT_SOURCE_ID:
        return URIRef(plant_graph_uri)
    return URIRef(f"{SOURCE_GRAPH_PREFIX}{quote(source_id, safe='')}")
