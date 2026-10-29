"""The graph nodes that describe a view's outputs.

Each output stream gets a point with an external reference and the
metadata its ``OutputSpec`` declares, the same shape a driver registers for a
raw stream, so another view's query can select it. These triples are the
publications planning adds to the model at each time, and what the runtime
publishes into Acquirium's own graph for the current plan.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME, ACQUIRIUM_SOURCE_ID, ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE, HAS_EXTERNAL_REFERENCE, HAS_MEDIUM, HAS_QUANTITY_KIND, HAS_UNIT,
    OF_SUBSTANCE, PRODUCED_BY, TIMESERIES_REFERENCE,
)
from acquirium.Materialization.models import OutputPort, StreamDescriptor


def _default_label(binding, port: str, spec) -> str:
    """Name a derived stream the most specific way its binding allows.

    The author's own ``label`` wins over this, and a ``named`` output already
    carries a human name. Otherwise the label reads as the thing measured
    followed by what produced it — ``Basin 1 inlet temperature
    (normalize-temperatures[celsius])`` — with any previous parenthetical
    dropped, so a chain of apps stays one hop deep instead of accumulating.
    An output over several streams has no single subject, so it is named for
    its app and port alone.
    """
    if spec.stream_name:
        return spec.stream_name
    tag = f"{binding.application_name}[{port}]"
    streams = [stream for values in binding.inputs.values() for stream in values]
    if len(streams) == 1 and streams[0].label:
        subject = re.sub(r"\s*\([^()]*\)$", "", streams[0].label)
        return f"{subject} ({tag})"
    return tag


class _Inputs:
    """The two fields ``_default_label`` reads from a binding."""
    def __init__(self, application_name: str, inputs: Mapping[str, Iterable[StreamDescriptor]]) -> None:
        self.application_name, self.inputs = application_name, inputs


def output_triples(application_name: str, inputs: Mapping[str, Iterable[StreamDescriptor]],
                   name: str, output: OutputPort) -> Graph:
    """The point, reference and metadata of one output stream."""
    graph = Graph()
    spec = output.spec
    ref, point = URIRef(output.ref_uri), URIRef(output.point_uri)
    graph.add((point, HAS_EXTERNAL_REFERENCE, ref))
    # Which app made this. On the point, where query attributes are matched,
    # so `measurement(app="…")` finds one app's work without the author
    # having to tag it by hand.
    graph.add((point, PRODUCED_BY, Literal(application_name)))
    graph.add((ref, RDF.type, TIMESERIES_REFERENCE))
    graph.add((ref, ACQUIRIUM_SOURCE_ID, Literal(f"derived:{application_name}")))
    graph.add((ref, ACQUIRIUM_REF_NAME, Literal(output.ref_name)))
    if spec.value_kind: graph.add((ref, ACQUIRIUM_VALUE_KIND, Literal(spec.value_kind)))
    if spec.unit: graph.add((point, HAS_UNIT, URIRef(spec.unit)))
    # A point the author supplied is theirs and already has a name; only a
    # point this runtime created gets a generated one, so a derived stream
    # never shows up as a bare URI.
    label = spec.label or (None if spec.point_uri else _default_label(_Inputs(application_name, inputs), name, spec))
    if label: graph.add((point, RDFS.label, Literal(label)))
    if spec.quantity_kind: graph.add((point, HAS_QUANTITY_KIND, URIRef(spec.quantity_kind)))
    if spec.medium: graph.add((point, HAS_MEDIUM, URIRef(spec.medium)))
    if spec.substance: graph.add((point, OF_SUBSTANCE, URIRef(spec.substance)))
    # On the point, not the reference: that is where driver registration
    # records it, and where the query layer looks — measurement(data_source=...)
    # filters the point.
    if spec.data_source: graph.add((point, DATA_SOURCE, Literal(spec.data_source)))
    for predicate, values in (spec.properties or {}).items():
        for value in values: graph.add((point, URIRef(predicate), URIRef(value)))
    return graph
