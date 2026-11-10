"""Experiment adapters for PR #120's metadata writer and query builder.

Metadata documents live in the graph. A transactional SQL outbox retries
resolved writes after an interruption without retaining metadata snapshots.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

import json
import logging

import polars as pl
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF

from acquirium.Client.explore.attributes import ATTR_NS, Attr, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import coerce, column_dtype
from acquirium.Client.metadata import flatten, plan_write, is_uri
from acquirium.Metadata import batch_update, document_triples, read_documents, DOCUMENT_NS
from acquirium.Storage.metadata_outbox import enqueue, execute

EXPERIMENT = Namespace("urn:acquirium:experiment:")
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
    # Variable IDs are already stream reference URIs.
    return variable_id


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

    def namespaces(self):
        """The namespace URIs the graph binds, the ones the server lists:
        a URI-looking user value under one is written as a URI, as in
        insert_metadata(). A graph without bindings writes such values as text."""
        manager = getattr(self.graph_store, "namespace_manager", None)
        return tuple(str(ns) for _, ns in manager().namespaces()) if callable(manager) else ()

    def validate(self, uri, values, *, variable=False):
        # Scalar declarations carry units without pretending to be live streams.
        projection = {key: value for key, value in {**values.get("_resolved_semantics", {}), **values}.items()
                      if key not in {"_resolved_semantics", "_declaration_format"}}
        return plan_write(uri, projection, role="data" if variable else "entity", resolve=self.resolve,
                          resolved=values.get("_resolved_semantics"), strict=self.strict,
                          namespaces=self.namespaces())

    def triples(self, record, *, prepared=None):
        uri, cls, fields, metadata = record
        values = {**metadata_values(metadata), **fields}
        if prepared is None:
            projection = self.validate(uri, values, variable=cls == EXPERIMENT.Variable).triples
        else:
            # Generated annotation IDs/receipt times are bound in SQL; semantic
            # terms were already resolved before entering that transaction.
            projection = [(URIRef(uri), p, o) for _,p,o in prepared.triples]
            projection += plan_write(uri, fields, role="data" if cls == EXPERIMENT.Variable else "entity",
                                     resolve=lambda *_: None, namespaces=self.namespaces()).triples
        # Complete values are authoritative; attributes are a query projection.
        return list(set([(URIRef(uri), RDF.type, cls), *(triple for triple in projection if not str(triple[1]).startswith(DOCUMENT_NS)),
                         *document_triples(uri, values)]))

    def record_update(self, record, *, prepared=None):
        return {"operation": "replace", "subject": record[0],
                "triples": [[str(s), str(p), o.n3()] for s, p, o in self.triples(record, prepared=prepared)]}

    def fields_update(self, uri, fields):
        write = plan_write(uri, fields, role="entity", resolve=self.resolve, namespaces=self.namespaces())
        return {"operation": "patch", "subject": uri,
                "predicates": [*write.replace_forward, *(ATTR_NS + key for key in write.replace_keys)],
                "triples": [[str(s), str(p), o.n3()] for s, p, o in write.triples]}


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
    study.client.sync_experiment_metadata()


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


def study_record(item):
    return (study_uri(item["template_id"]), EXPERIMENT.Study,
            {"study_id": item["template_id"], "label": item["name"], "created_at": item["created_at"]}, {})

def variable_record(template_id, item):
    fields = {key: item[key] for key in ("variable_id", "label", "role", "kind", "created_at")}
    fields["study_id"] = template_id
    return variable_uri(item["variable_id"]), EXPERIMENT.Variable, fields, item["metadata"]

def run_record(item):
    fields = {key: item.get(key) for key in ("run_id", "status", "started_at", "finished_at")}
    fields["study_id"] = item["template_id"]
    return run_uri(item["run_id"]), EXPERIMENT.Run, fields, item["metadata"]

def annotation_record(item):
    fields = {key: item.get(key) for key in ("annotation_id", "run_id", "ref_uri", "range_start", "range_end", "created_at")}
    fields["study_id"] = item["template_id"]
    return annotation_uri(item["annotation_id"]), EXPERIMENT.StreamAnnotation, fields, item["metadata"]


class ExperimentMetadata:
    """Own document hydration, serialized delivery, and durable acknowledgment.

    Mutations are resolved before enqueueing in the caller's SQL transaction.
    SQL writer locks are never held during graph I/O.
    ``on_publish`` runs under the delivery lock after synchronization; a
    callback that reconciles apps must share that lock with app deployment.
    """
    _ID_FIELDS = {
        EXPERIMENT.Study: "study_id", EXPERIMENT.Run: "run_id",
        EXPERIMENT.Variable: "variable_id", EXPERIMENT.StreamAnnotation: "annotation_id",
    }

    def __init__(self, store, graph_store, resolve, *, lock=None, on_publish=None):
        import threading
        required = ("metadata_documents", "metadata_triples", "metadata_update", "sparql_update", "clear_graphs", "flush_metadata", "source_graph_uri")
        missing = [name for name in required if not callable(getattr(graph_store, name, None))]
        if missing:
            raise TypeError(f"experiment metadata backend lacks capabilities: {', '.join(missing)}")
        self.store = store
        self.writer = ExperimentMetadataGraph(graph_store, resolve)
        self.lock = graph_store.__dict__.setdefault(
            "_experiment_delivery_lock", lock if lock is not None else threading.RLock()
        )
        self._on_publish = on_publish
        self._generation_checked = False

    def enqueue(self, conn, update):
        enqueue(conn, update)

    def _ensure_graph_generation(self):
        if self._generation_checked:
            return
        graph = self.writer.graph_store
        graph_uri = graph.source_graph_uri("experiments")
        subject = "urn:acquirium:experiments:store"
        with self.store._own_conn() as conn:
            generation, published = execute(conn, "SELECT generation, published_version FROM experiment_metadata_state").fetchone()
            pending = execute(conn, "SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0]
            records = sum(execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0]
                          for table in ("experiment_templates", "experiment_runs", "experiment_variables"))
        current = read_documents(self.writer.graph_store, [subject])[subject].get("generation")
        if current != generation:
            if records and (published > 0 or not pending):
                raise RuntimeError("experiment metadata graph is missing or belongs to another store; restore its graph backup")
            graph.clear_graphs([graph_uri])
            data = " ".join(f"{s.n3()} {p.n3()} {o.n3()} ." for s, p, o in document_triples(subject, {"generation": generation}))
            graph.sparql_update(f"INSERT DATA {{ {data} }}", graph_uri=graph_uri)
        self._generation_checked = True

    def _publish_pending(self):
        self._ensure_graph_generation()
        with self.store._own_conn() as conn:
            pending = execute(conn, "SELECT version, update_text FROM experiment_metadata_outbox ORDER BY version").fetchall()
        if not pending:
            return
        graph = self.writer.graph_store
        operations = [json.loads(row[1]) for row in pending]
        target = graph.source_graph_uri("experiments")
        buffered, current_target = [], target
        def deliver():
            if buffered:
                graph.metadata_update(batch_update(buffered, current_target), graph_uri=current_target)
                buffered.clear()
        for operation in operations:
            if "clear" in operation:
                deliver()
                graph.clear_graphs(operation["clear"])
            else:
                destination = operation.get("graph_uri", target)
                if destination != current_target:
                    deliver()
                    current_target = destination
                buffered.append(operation)
        deliver()
        graph.flush_metadata()
        # Graph commit precedes acknowledgment. Replaying after either process
        # interruption or an acknowledgment failure is idempotent.
        with self.store._lock, self.store._write_conn() as conn:
            versions = [row[0] for row in pending]
            marks = ",".join("?" for _ in versions)
            execute(conn, f"DELETE FROM experiment_metadata_outbox WHERE version IN ({marks})", versions)
            execute(conn, "UPDATE experiment_metadata_state SET published_version=greatest(published_version, ?)", [max(versions)])

    def sync(self):
        with self.lock:
            self._publish_pending()
            # Retry reconciliation even when an earlier delivery was acknowledged
            # before app compilation failed. No SQL transaction is open here.
            if self._on_publish is not None:
                self._on_publish()
            return {"ok": True, "unpublished": []}

    @contextmanager
    def edit(self):
        """Serialize explicit graph edits with delivery and declaration upgrades."""
        with self.lock:
            self.sync()
            yield

    def documents(self, subjects):
        """Hydrate a complete batch at the strict experiment freshness boundary."""
        with self.lock:
            self.sync()
            return read_documents(self.writer.graph_store, subjects)

    def publish(self):
        # General queries drain the same durable queue as experiment reads.
        try:
            self.sync()
        except Exception:
            logging.getLogger(__name__).exception("Experiment metadata synchronization deferred; retry on the next sync")

    def _hydrate(self, requests):
        """Load distinct (kind, ID) requests at one strict freshness boundary."""
        subjects = {key: node_uri(*key) for key in requests}
        if not subjects:
            return {}
        with self.lock:
            self.sync()
            documents = read_documents(self.writer.graph_store, subjects.values())
            missing = [identifier for (kind, identifier), uri in subjects.items()
                       if self._ID_FIELDS[kind] not in documents[uri]]
            if missing:
                raise RuntimeError(f"experiment metadata graph has no document for {sorted(missing)!r}")
            return {key: {k: v for k, v in documents[uri].items() if k not in RESERVED_METADATA_KEYS}
                    for key, uri in subjects.items()}

    def read(self, kind, identifiers):
        return {identifier: values for (_, identifier), values in
                self._hydrate((kind, identifier) for identifier in identifiers).items()}

    def read_metadata(self, items, cls, id_field):
        if not items:
            return items
        values = self.read(cls, [item[id_field] for item in items])
        return [{**item, "metadata": values[item[id_field]]} for item in items]

    def read_observation_metadata(self, items):
        values = self._hydrate(key for item in items for key in (
            (EXPERIMENT.Run, item["experiment_id"]), (EXPERIMENT.Variable, item["variable_id"])))
        return [{**item, "experiment_metadata": values[EXPERIMENT.Run, item["experiment_id"]],
                 "variable_metadata": values[EXPERIMENT.Variable, item["variable_id"]]} for item in items]


def stream_update(writer, source, name, study_id, run_id, variable_id, label, metadata, keys, key):
    """Resolve an owned stream's graph intent before entering the SQL writer."""
    from rdflib import Graph
    from acquirium.StreamMetadata import _build_stream_triples, POINT_FIELD_KINDS
    graph = Graph()
    canonical = metadata.get("_resolved_semantics", {})
    semantics = {field: metadata[field] for field in POINT_FIELD_KINDS if field in metadata}
    resolved = {field: canonical.get(field, str(value) if is_uri(value) else None) for field, value in semantics.items()}
    # Unresolved spellings remain in declaration documents; no resolver
    # is called during recording or replay.
    semantics = {field: value for field, value in semantics.items() if resolved[field] is not None}
    point = _build_stream_triples(graph, {"source_id": source, "ref_name": name, "label": label,
        "value_kind": "numeric", **semantics}, resolved, {}, lambda *_: False)
    observed = metadata.get("observed")
    from acquirium.Metadata import dumps
    for mapping in metadata.get("observed_by_key", []):
        if dumps(mapping["key"]) == dumps(list(key)):
            observed = mapping["observed"]
    identity = {"study_id": study_id, "run_id": run_id, "variable_id": variable_id,
                **dict(zip(keys, key)), "metadata": metadata.get("metadata", {})}
    if observed is not None:
        identity["observed"] = observed
    for field in ("source_path", "description"):
        if field in metadata:
            identity[field] = metadata[field]
    write = plan_write(str(point), identity, role="data", resolve=lambda *_: None,
                       namespaces=writer.namespaces())
    for triple in write.triples:
        graph.add(triple)
    # Insert is idempotent and preserves other assertions on the run-owned point.
    return {"operation": "patch", "subject": str(point), "predicates": [],
            "graph_uri": str(writer.graph_store.source_graph_uri(source)),
            "triples": [[str(s), str(p), o.n3()] for s,p,o in graph]}
