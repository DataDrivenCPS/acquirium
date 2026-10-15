"""Experiment adapters for PR #120's metadata writer and query builder.

Saved metadata reads use the shared graph object API. SQL snapshots remain
the recovery source for rebuilding the graph after an interrupted write.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS

from acquirium.Client.explore.attributes import ATTR_NS, REGISTRY, Attr, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import coerce, column_dtype
from acquirium.Client.metadata import flatten, plan_write

EXPERIMENT = Namespace("urn:acquirium:experiment:")
EXPERIMENT_SOURCE = "experiments"
RESERVED_METADATA_KEYS = frozenset({
    "study_id", "run_id", "variable_id", "annotation_id", "role", "kind",
    "status", "created_at", "started_at", "finished_at", "range_start", "range_end",
    "ref_uri", "label",
})


def study_uri(study_id: str) -> str:
    return str(EXPERIMENT[f"study/{study_id}"])


def run_uri(run_id: str) -> str:
    return str(EXPERIMENT[f"run/{run_id}"])


def variable_uri(variable_id: str) -> str:
    # Earlier releases stored bare UUIDs. Keep those SQL IDs and give their
    # graph nodes an absolute URI; new declarations already use stream URIs.
    if variable_id.startswith(("urn:", "http://", "https://")):
        return variable_id
    return str(EXPERIMENT[f"variable/{variable_id}"])


def annotation_uri(annotation_id: str) -> str:
    return str(EXPERIMENT[f"annotation/{annotation_id}"])


def metadata_values(values: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply upstream key validation and null/empty deletion semantics.

    This is only for descriptive metadata. Recorded JSON observations retain
    nulls and empty containers as experiment data.
    """
    values = {} if values is None else values
    flatten(values)
    conflicts = RESERVED_METADATA_KEYS.intersection(values)
    if conflicts:
        raise ValueError(f"reserved experiment metadata keys: {sorted(conflicts)}")

    def clean(value):
        if isinstance(value, Mapping):
            result = {}
            for key, child in value.items():
                child = clean(child)
                if child is not None:
                    result[key] = child
            return result or None
        if isinstance(value, (list, tuple)):
            # Keep index positions: upstream skips null leaves, but tags.2
            # must not become tags.1 after a restart/backfill.
            result = [clean(child) for child in value]
            return result if any(child is not None for child in result) else None
        return value

    return clean(values) or {}


def records_frame(items: list[dict[str, Any]]) -> pl.DataFrame:
    """Use upstream scalar typing without coercing saved objects into structs."""
    if not items:
        return pl.DataFrame()
    columns = {}
    for name in items[0]:
        values = [item.get(name) for item in items]
        dtype = column_dtype(values)
        columns[name] = pl.Series(name, coerce(values, dtype), dtype=dtype)
    return pl.DataFrame(columns)


def saved_metadata(values, snapshot):
    """Preserve shapes the shared table-based object reader cannot recover yet.

    This is a field-level compatibility path, not a fallback on graph errors.
    Remove it after upstream objects retain list shape, index gaps, and the
    distinction between a scalar and a list on different nodes.
    """
    def unsupported_shape(value, returned):
        if isinstance(value, dict):
            if not isinstance(returned, dict):
                return True
            return any(unsupported_shape(child, returned.get(key)) for key, child in value.items())
        if isinstance(value, list):
            return not isinstance(returned, list) or any(
                child is None or isinstance(child, (dict, list)) for child in value
            )
        return isinstance(returned, (dict, list))

    result = dict(values)
    for key, value in snapshot.items():
        attr = REGISTRY.get(key)
        if (key in {"id", "type"} or unsupported_shape(value, result.get(key))
                or isinstance(value, list) and attr is not None and not attr.literal):
            result[key] = value
    return result


class ExperimentMetadataGraph:
    """Publish ledger-owned nodes using the same writer as insert_metadata()."""

    def __init__(self, graph_store, resolve):
        self.graph_store = graph_store
        self.resolve = resolve
        self.client = _GraphQueryClient(graph_store)

    def validate(self, uri, values, *, variable=False):
        # Scalar declarations carry units without pretending to be live streams.
        return plan_write(uri, values, role="data" if variable else "entity", resolve=self.resolve)

    def graph(self, records):
        graph = Graph()
        for cls in (EXPERIMENT.Study, EXPERIMENT.Run, EXPERIMENT.Variable, EXPERIMENT.StreamAnnotation):
            graph.add((cls, RDF.type, RDFS.Class))
        for uri, cls, fields, metadata in records:
            subject = URIRef(uri)
            graph.add((subject, RDF.type, cls))
            values = {**metadata_values(metadata), **fields}
            write = self.validate(uri, values, variable=cls == EXPERIMENT.Variable)
            for triple in write.triples:
                graph.add(triple)
        return graph

    def replace(self, records):
        graph = self.graph(records)
        self.graph_store.insert_graph(
            graph, replace=True,
            graph_uri=self.graph_store.source_graph_uri(EXPERIMENT_SOURCE),
        )

    def upsert(self, record):
        graph = self.graph([record])
        subject = URIRef(record[0])
        triples = "\n".join(f"{s.n3()} {p.n3()} {o.n3()} ." for s, p, o in graph)
        update = f"DELETE WHERE {{ {subject.n3()} ?p ?o }}; INSERT DATA {{ {triples} }}"
        self.graph_store.sparql_update(
            update, graph_uri=self.graph_store.source_graph_uri(EXPERIMENT_SOURCE),
        )

    def read(self, cls, id_field, identifiers):
        """Read selected objects in batches through PR #120's object terminal."""
        identifiers = sorted(set(identifiers))
        result = {}
        for offset in range(0, len(identifiers), 500):
            query = ExperimentQuery(self.client).entity(cls, alias="object")
            query = query.where(**{id_field: identifiers[offset:offset + 500]})
            for row in query.include("all").metadata_objects(include_dependencies=False):
                item = row["object"]
                identifier = item[id_field]
                values = {
                    key: value for key, value in item.items()
                    if key != "id" and key not in RESERVED_METADATA_KEYS
                }
                # The experiment API keeps the caller's spelling. Native query
                # output exposes the resolved value and <key>.raw separately.
                for key in list(values):
                    if key.endswith(".raw"):
                        values[key[:-4]] = values.pop(key)
                result[identifier] = values
        return result


class _GraphQueryClient:
    """Run the shared query builder against the local graph, without HTTP."""

    def __init__(self, graph_store):
        self.graph_store = graph_store
        self.base_url = f"experiment-graph:{id(graph_store)}"

    def graph_version(self):
        return self.graph_store.graph_status()["source_version"]

    @staticmethod
    def compact_uri(uri):
        # Keep full URIs in saved metadata; native HTTP queries use CURIEs.
        return str(uri)

    def sparql_query(self, query, include_dependencies=False, **kwargs):
        kwargs.setdefault("wait_for_fresh", True)
        result = self.graph_store.sparql_query(query, include_dependencies=include_dependencies, **kwargs)
        if "datatypes" not in result:
            result["datatypes"] = [
                ["iri" if isinstance(value, URIRef) else
                 str(value.datatype) if isinstance(value, Literal) and value.datatype else None
                 for value in row]
                for row in result["rows"]
            ]
        result["rows"] = [[str(value) if value is not None else None for value in row] for row in result["rows"]]
        return result


class _ExperimentRegistry(Registry):
    """Extend the native registry with known ledger fields."""

    def _discovered(self):
        # Fields are valid even before this Study has any runs or variables.
        fields = {
            name: Attr(name, (ATTR_NS + name,), "any", frozenset({"entity", "data"}), literal=True)
            for name in RESERVED_METADATA_KEYS if name != "label"
        }
        return {**fields, **super()._discovered()}


class _ExperimentClient:
    """Experiment reads wait for their graph writes to become queryable."""

    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        return getattr(self.client, name)

    def sparql_query(self, query, include_dependencies=False, **kwargs):
        kwargs.setdefault("wait_for_fresh", True)
        return self.client.sparql_query(
            query, include_dependencies=include_dependencies, **kwargs,
        )


class ExperimentQuery(Query):
    """Native Query with experiment fields and fresh experiment reads."""

    @property
    def registry(self):
        if "_registry" not in self.cache:
            self.cache["_registry"] = _ExperimentRegistry(self.client)
        return self.cache["_registry"]

    def _with_graph(self, graph):
        return ExperimentQuery(self.client, graph)


def experiment_query(study, *, variable=False):
    study.client.sync_experiment_metadata()
    client = _ExperimentClient(study.client)
    query = ExperimentQuery(client)
    cls = EXPERIMENT.Variable if variable else EXPERIMENT.Run
    alias = "variable" if variable else "experiment"
    return query.entity(cls, alias=alias).where(study_id=study.study_id)


def matched_ids(query):
    result = query.execute(include_dependencies=False)
    index = result["columns"].index("v0")
    return {str(row[index]) for row in result["rows"]}
