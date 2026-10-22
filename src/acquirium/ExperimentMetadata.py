"""Experiment adapters for PR #120's metadata writer and query builder.

Metadata documents live in the graph. A transactional SQL outbox retries
resolved writes after an interruption without retaining metadata snapshots.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import RDF

from acquirium.Client.explore.attributes import ATTR_NS, REGISTRY, Attr, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import coerce, column_dtype
from acquirium.Client.metadata import flatten, plan_write, is_uri
from acquirium.Metadata import document_triples, read_documents

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


def node_uri(cls, identifier: str) -> str:
    return {EXPERIMENT.Study: study_uri, EXPERIMENT.Run: run_uri, EXPERIMENT.Variable: variable_uri,
            EXPERIMENT.StreamAnnotation: annotation_uri}[cls](identifier)


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

    return {key: value for key, value in values.items()
            if key == "metadata" or value is not None and value != [] and value != {}}


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


class ExperimentMetadataGraph:
    """Publish ledger-owned nodes using the same writer as insert_metadata()."""

    def __init__(self, graph_store, resolve, *, strict=True):
        self.strict = strict
        self.graph_store = graph_store
        self.resolve = resolve
        self.client = _GraphQueryClient(graph_store, resolve)

    def validate(self, uri, values, *, variable=False):
        # Scalar declarations carry units without pretending to be live streams.
        if not self.strict:
            def resolved(key, value):
                attr = REGISTRY.get(key)
                if attr is None or attr.literal:
                    return True
                items = value if isinstance(value, (list, tuple)) else [value]
                return all(item is None or is_uri(item) or self.resolve(str(item), attr.kind) is not None for item in items)
            values = {key: value for key, value in values.items() if resolved(key, value)}
        return plan_write(uri, values, role="data" if variable else "entity", resolve=self.resolve)

    def triples(self, record):
        uri, cls, fields, metadata = record
        values = {**metadata_values(metadata), **fields}
        write = self.validate(uri, values, variable=cls == EXPERIMENT.Variable)
        # Complete values are authoritative; attributes are a query projection.
        return list(set([(URIRef(uri), RDF.type, cls), *write.triples,
                         *document_triples(uri, metadata_values(metadata))]))

    def record_update(self, record):
        return {"operation": "replace", "subject": record[0],
                "triples": [[str(s), str(p), o.n3()] for s, p, o in self.triples(record)]}

    def fields_update(self, uri, fields):
        write = plan_write(uri, fields, role="entity", resolve=self.resolve)
        return {"operation": "patch", "subject": uri,
                "predicates": [*write.replace_forward, *(ATTR_NS + key for key in write.replace_keys)],
                "triples": [[str(s), str(p), o.n3()] for s, p, o in write.triples]}

    def read(self, cls, id_field, identifiers):
        subjects = {node_uri(cls, identifier): identifier for identifier in set(identifiers)}
        documents = read_documents(self.client, subjects)
        return {subjects[uri]: {key: value for key, value in values.items() if key not in RESERVED_METADATA_KEYS}
                for uri, values in documents.items() if id_field in values}


class _GraphQueryClient:
    """Run the shared query builder against the local graph, without HTTP."""

    def __init__(self, graph_store, resolve=None):
        self.graph_store = graph_store
        self._resolve = resolve
        self.base_url = f"experiment-graph:{id(graph_store)}"

    def resolve(self, text, kind=None, min_score=.4):
        return self._resolve(text, kind) if self._resolve is not None else None

    def graph_version(self):
        return self.graph_store.graph_status()["source_version"]

    def metadata_documents(self, subjects):
        return read_documents(self.graph_store, subjects)

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

    def metadata_documents(self, subjects):
        return read_documents(self.client, subjects)

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


def sync_study(study):
    """Bring the graph up to date before a query over one Study's nodes.

    A graph filter cannot match a record the graph does not hold, so a left-out
    record in this Study fails the query instead of silently going missing.
    """
    status = study.client.sync_experiment_metadata()
    failed = [item for item in status.get("unpublished", []) if item["study_id"] == study.study_id]
    if failed:
        details = "; ".join(f"{item['kind'].lower()} {item['id']!r}: {item['error']}" for item in failed)
        raise ValueError(f"cannot query study {study.study_id!r}: its metadata graph is missing {details}")


def experiment_query(study, *, variable=False):
    sync_study(study)
    client = _ExperimentClient(study.client)
    query = ExperimentQuery(client)
    cls = EXPERIMENT.Variable if variable else EXPERIMENT.Run
    alias = "variable" if variable else "experiment"
    return query.entity(cls, alias=alias).where(study_id=study.study_id)


def matched_ids(query):
    result = query.execute(include_dependencies=False)
    index = result["columns"].index("v0")
    return {str(row[index]) for row in result["rows"]}
