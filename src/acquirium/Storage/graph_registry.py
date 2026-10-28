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

# The plant source owns the canonical main graph.
PLANT_GRAPH_URI = "urn:acquirium#MainGraph"
# The plant model is a first-class deployment source. Public callers must use
# this ID instead of relying on an omitted source owner.
PLANT_SOURCE_ID = "plant"
ACQUIRIUM_GRAPH_URI = "urn:acquirium:graph:data:acquirium"
SOURCE_GRAPH_PREFIX = "urn:acquirium:graph:data:source:"
# The source that owns metadata written with ``insert_metadata``. An
# ordinary source graph by shape, so it needs no registration.
METADATA_SOURCE_ID = "metadata"


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


# Experiment graphs are rebuilt from the experiment ledger's SQL tables, so
# they are derived data like Acquirium's own graph and carry no history.
EXPERIMENTS_SOURCE_ID = "experiments"
EXPERIMENT_RUN_SOURCE_PREFIX = "experiment/"


def is_versioned_graph(graph_uri: str | URIRef, *, plant_graph_uri: str = PLANT_GRAPH_URI) -> bool:
    """Whether writes to ``graph_uri`` are recorded in the model history.

    The model is the plant graph and every source graph, the metadata graph
    included. Acquirium's own graph and the experiment graphs are derived
    from other state and are not part of it.
    """
    uri = str(graph_uri)
    if uri == plant_graph_uri:
        return True
    if not uri.startswith(SOURCE_GRAPH_PREFIX):
        return False
    return uri != str(source_graph_uri(EXPERIMENTS_SOURCE_ID)) and not uri.startswith(
        str(source_graph_uri(EXPERIMENT_RUN_SOURCE_PREFIX))
    )
