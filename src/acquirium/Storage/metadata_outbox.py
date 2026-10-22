"""Transactional handoff of resolved graph writes from the recording ledger."""
from __future__ import annotations

import json
from pathlib import Path
import threading
from uuid import uuid4
from weakref import WeakValueDictionary

from rdflib import Dataset, Graph
from oxrdflib.store import OxigraphStore
import pyoxigraph

from acquirium.Storage.graph_registry import METADATA_SOURCE_ID, source_graph_uri


def execute(conn, statement, parameters=()):
    if conn.__class__.__module__.startswith("psycopg"):
        statement = statement.replace("?", "%s")
    return conn.execute(statement, list(parameters))


def enqueue(conn, update):
    if isinstance(update, dict):
        update = json.dumps(update)
    version = execute(conn, "UPDATE experiment_metadata_state SET version=version+1 RETURNING version").fetchone()[0]
    execute(conn, "INSERT INTO experiment_metadata_outbox (version, update_text) VALUES (?, ?)", [version, update])


def migrate_documents(conn):
    """Move legacy snapshots to resolved, replayable graph writes atomically."""
    from acquirium.ExperimentMetadata import ExperimentMetadataGraph
    from acquirium.Experiments import ExperimentStore, timestamp

    execute(conn, "CREATE TABLE experiment_metadata_state (generation VARCHAR NOT NULL, version BIGINT NOT NULL, published_version BIGINT NOT NULL)")
    execute(conn, "INSERT INTO experiment_metadata_state VALUES (?, 0, 0)", [str(uuid4())])
    execute(conn, "CREATE TABLE experiment_metadata_outbox (version BIGINT PRIMARY KEY, update_text VARCHAR NOT NULL)")
    tables = {row[0] for row in execute(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema()").fetchall()}
    pins = {}
    if "experiment_metadata_resolutions" in tables:
        pins = {(text, kind): uri for text, kind, uri in execute(conn, "SELECT text, kind, uri FROM experiment_metadata_resolutions").fetchall()}
    graph = ExperimentMetadataGraph(None, lambda text, kind: pins.get((text, kind)), strict=False)
    records = []
    for identifier, name, created in execute(conn, "SELECT template_id, name, created_at FROM experiment_templates").fetchall():
        records.append(ExperimentStore._study_record(dict(template_id=identifier, name=name, created_at=timestamp(created))))
    for identifier, study, label, role, kind, metadata, created in execute(conn, "SELECT variable_id, template_id, label, role, kind, metadata_json, created_at FROM experiment_variables").fetchall():
        values = json.loads(metadata)
        if values.get("_adapter_format") == 1:
            values["metadata"] = json.loads(values.pop("adapter_metadata"))
            values["_adapter_format"] = 2
        records.append(ExperimentStore._variable_record(study, dict(variable_id=identifier, label=label, role=role, kind=kind, metadata=values, created_at=timestamp(created))))
    for identifier, study, status, metadata, started, finished in execute(conn, "SELECT run_id, template_id, status, metadata_json, started_at, finished_at FROM experiment_runs").fetchall():
        records.append(ExperimentStore._run_record(dict(run_id=identifier, template_id=study, status=status, metadata=json.loads(metadata), started_at=timestamp(started), finished_at=timestamp(finished) if finished else None)))
    for run, sequence, study, ref, start, end, metadata, created in execute(conn, "SELECT o.run_id,o.sequence,r.template_id,o.ref_uri,o.range_start,o.range_end,o.value_json,o.recorded_at FROM experiment_observations o JOIN experiment_runs r ON r.run_id=o.run_id WHERE o.variable_id IS NULL").fetchall():
        records.append(ExperimentStore._annotation_record(dict(annotation_id=f"{run}:{sequence}", run_id=run, template_id=study, ref_uri=ref, range_start=timestamp(start), range_end=timestamp(end) if end else None, metadata=json.loads(metadata), created_at=timestamp(created))))
    for record in records:
        enqueue(conn, graph.record_update(record))
    # Removing only the snapshots leaves lifecycle and observation data intact.
    # DuckDB secondary indexes must be recreated after the schema transaction.
    execute(conn, "DROP INDEX IF EXISTS experiment_variables_template_idx")
    execute(conn, "DROP INDEX IF EXISTS experiment_runs_template_idx")
    execute(conn, "ALTER TABLE experiment_variables DROP COLUMN metadata_json")
    execute(conn, "ALTER TABLE experiment_runs DROP COLUMN metadata_json")
    execute(conn, "UPDATE experiment_observations SET value_json=NULL WHERE variable_id IS NULL")
    execute(conn, "DROP TABLE IF EXISTS experiment_metadata_resolutions")


class LocalMetadataGraph:
    """The same durable RDF documents for callers without a server graph.

    Standalone ledger users share one embedded Oxigraph instance per directory.
    Server users supply their existing deployment graph instead.
    """
    _instances = WeakValueDictionary()
    _instances_lock = threading.Lock()
    source_graph_uri = staticmethod(source_graph_uri)

    @classmethod
    def open(cls, path):
        key = str(Path(path).resolve())
        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is None:
                instance = cls(key)
                cls._instances[key] = instance
            return instance

    def __init__(self, path):
        self.dataset = Dataset(store=OxigraphStore(store=pyoxigraph.Store(path)), default_union=True)
        self.version = 0

    def sparql_query(self, query, **kwargs):
        from acquirium.Metadata import overlay_documents

        # Keep native SPARQL term equality (including plain/xsd:string
        # equivalence) when querying the disposable effective view.
        graph = Graph(store=OxigraphStore(store=pyoxigraph.Store()))
        for context in self.dataset.graphs():
            graph += context
        overlay_documents(graph, self.dataset.graph(source_graph_uri(METADATA_SOURCE_ID)))
        result = graph.query(query)
        return {"columns": [str(v) for v in result.vars], "rows": [list(row) for row in result]}

    def metadata_documents(self, subjects):
        from acquirium.Metadata import DOCUMENT_NS, decode_document_rows

        subjects = sorted(set(map(str, subjects)))
        rows = [(subject, quad.predicate.value, quad.object.value, quad.graph_name.value)
                for subject in subjects
                for quad in self.dataset.store._inner.quads_for_pattern(pyoxigraph.NamedNode(subject), None, None, None)
                if quad.predicate.value.startswith(DOCUMENT_NS)]
        return decode_document_rows(subjects, rows)

    def sparql_update(self, update, *, graph_uri):
        self.dataset.graph(graph_uri).update(update)
        self.version += 1

    def graph_status(self):
        return {"source_version": self.version}

    def flush_metadata(self):
        self.dataset.store._inner.flush()

    def clear_graphs(self, graph_uris=(), *, prefix=None):
        targets = set(graph_uris)
        if prefix:
            targets.update(g.identifier for g in self.dataset.graphs() if str(g.identifier).startswith(prefix))
        for uri in targets:
            self.dataset.graph(uri).remove((None, None, None))
        self.version += 1
