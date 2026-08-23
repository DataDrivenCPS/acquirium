"""The single definition of every materialization table, per backend dialect.

Templates use ``$STR``/``$TS``/``$JSON``/``$DOUBLE`` type tokens so DuckDB and
PostgreSQL execute the same schema.  All statements are idempotent.
"""
from __future__ import annotations

from string import Template

_DIALECTS = {
    "duckdb": {"STR": "VARCHAR", "TS": "TIMESTAMP", "JSON": "VARCHAR", "DOUBLE": "DOUBLE"},
    "postgres": {"STR": "TEXT", "TS": "TIMESTAMPTZ", "JSON": "JSONB", "DOUBLE": "DOUBLE PRECISION"},
}

# The range manifest is also created by the canonical timeseries stores; this
# copy of the identical definition keeps a standalone materialization or epoch
# store self-sufficient against a fresh database.
_CHANGE_RANGE_TEMPLATES = (
    """CREATE TABLE IF NOT EXISTS stream_change_ranges (
        ref_uri $STR NOT NULL, stream_version BIGINT NOT NULL, publication_id $STR NOT NULL,
        start_ts $TS NOT NULL, end_ts $TS NOT NULL, change_kind $STR NOT NULL,
        row_count BIGINT NOT NULL,
        PRIMARY KEY (ref_uri, stream_version, start_ts, end_ts))""",
    "CREATE INDEX IF NOT EXISTS idx_stream_change_ranges_ref_version ON stream_change_ranges (ref_uri, stream_version)",
)

# The one immutable definition registry, shared by the support store
# (experiments, services) and the topology-epoch control plane
# (transformations); rows are content-addressed by definition_id.
_DEFINITIONS_TEMPLATE = """CREATE TABLE IF NOT EXISTS materialization_definitions (
        definition_id $STR PRIMARY KEY, name $STR NOT NULL, kind $STR NOT NULL,
        source_digest $STR NOT NULL, entrypoint $STR NOT NULL, spec_json $JSON NOT NULL,
        created_at $TS NOT NULL)"""

_SUPPORT_TEMPLATES = (
    _DEFINITIONS_TEMPLATE,
    """CREATE TABLE IF NOT EXISTS materialization_artifacts (
        digest $STR PRIMARY KEY, uri $STR NOT NULL, size_bytes BIGINT NOT NULL,
        media_type $STR NOT NULL, metadata_json $JSON NOT NULL, created_at $TS NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS materialization_artifact_requests (
        request_id $STR PRIMARY KEY, semantic_digest $STR UNIQUE NOT NULL, kind $STR NOT NULL,
        deployment_name $STR NOT NULL, binding_id $STR NOT NULL, previous_revision $STR,
        input_vector_json $JSON NOT NULL, range_start $TS NOT NULL, range_end $TS NOT NULL,
        metadata_json $JSON NOT NULL, status $STR NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
        lease_owner $STR, lease_expires_at $TS, result_revision $STR, error_json $JSON,
        created_at $TS NOT NULL, completed_at $TS)""",
    """CREATE TABLE IF NOT EXISTS materialization_state_revisions (
        revision_id $STR PRIMARY KEY, deployment_name $STR NOT NULL, binding_id $STR NOT NULL,
        parent_revision $STR, artifact_digest $STR NOT NULL, request_id $STR NOT NULL,
        policy $STR, effective_from $TS, status $STR NOT NULL, metrics_json $JSON NOT NULL,
        created_at $TS NOT NULL, activated_at $TS)""",
    """CREATE TABLE IF NOT EXISTS experiment_runs (
        run_id $STR PRIMARY KEY, definition_id $STR NOT NULL, graph_revision BIGINT NOT NULL,
        start_ts $TS NOT NULL, end_ts $TS NOT NULL, status $STR NOT NULL,
        params_json $JSON NOT NULL, params_schema_json $JSON NOT NULL, metadata_json $JSON NOT NULL,
        input_vector_json $JSON NOT NULL, binding_snapshot_json $JSON NOT NULL, state_revision $STR,
        started_at $TS NOT NULL, finished_at $TS, error_json $JSON,
        keep_reason $STR, collected_at $TS,
        execution_claim $STR)""",
    """CREATE TABLE IF NOT EXISTS experiment_run_metrics (
        run_id $STR NOT NULL, name $STR NOT NULL, value_json $JSON NOT NULL,
        recorded_at $TS NOT NULL, PRIMARY KEY (run_id, name))""",
    """CREATE TABLE IF NOT EXISTS experiment_run_artifacts (
        run_id $STR NOT NULL, name $STR NOT NULL, artifact_digest $STR NOT NULL,
        metadata_json $JSON NOT NULL, PRIMARY KEY (run_id, name))""",
    """CREATE TABLE IF NOT EXISTS experiment_run_outputs (
        run_id $STR NOT NULL, name $STR NOT NULL, ref_uri $STR NOT NULL,
        PRIMARY KEY (run_id, name), UNIQUE (ref_uri))""",
    """CREATE TABLE IF NOT EXISTS materialization_effect_intents (
        effect_id $STR PRIMARY KEY, execution_id $STR NOT NULL, kind $STR NOT NULL,
        destination $STR NOT NULL, payload_json $JSON NOT NULL, idempotency_key $STR UNIQUE NOT NULL,
        status $STR NOT NULL, attempts INTEGER NOT NULL, next_attempt_at $TS, error_json $JSON,
        lease_owner $STR, lease_expires_at $TS)""",
    """CREATE TABLE IF NOT EXISTS materialization_services (
        name $STR PRIMARY KEY, definition_id $STR NOT NULL, status $STR NOT NULL,
        health $STR NOT NULL, updated_at $TS NOT NULL, last_data_versions_json $JSON NOT NULL DEFAULT '{}',
        last_graph_revision BIGINT)""",
    """CREATE TABLE IF NOT EXISTS materialization_service_hints (
        service_name $STR PRIMARY KEY, token $STR NOT NULL, data_versions_json $JSON NOT NULL,
        graph_revision BIGINT, created_at $TS NOT NULL)""",
)

_EPOCH_TEMPLATES = (
    _DEFINITIONS_TEMPLATE,
    """CREATE TABLE IF NOT EXISTS topology_deployments (
        name $STR PRIMARY KEY, definition_id $STR NOT NULL,
        generation BIGINT NOT NULL, updated_at $TS NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS topology_epochs (
        epoch_id $STR PRIMARY KEY, graph_revision BIGINT NOT NULL, graph_digest $STR NOT NULL,
        catalog_digest $STR NOT NULL, status $STR NOT NULL, superseded_by $STR,
        created_at $TS NOT NULL, activated_at $TS, compacted_at $TS)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS topology_epochs_revision_catalog ON topology_epochs (graph_revision, catalog_digest)",
    """CREATE TABLE IF NOT EXISTS topology_epoch_binding_pins (
        epoch_id $STR NOT NULL, binding_id $STR NOT NULL, state_revision $STR,
        policy $STR, effective_from $TS,
        PRIMARY KEY (epoch_id, binding_id))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_bindings (
        epoch_id $STR NOT NULL, binding_id $STR NOT NULL, definition_id $STR NOT NULL,
        logical_key $STR NOT NULL, content_digest $STR NOT NULL, inputs_json $JSON NOT NULL,
        outputs_json $JSON NOT NULL, metadata_json $JSON NOT NULL, state_revision $STR,
        PRIMARY KEY (epoch_id, binding_id))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_edges (
        epoch_id $STR NOT NULL, source_binding_id $STR NOT NULL, target_binding_id $STR NOT NULL,
        PRIMARY KEY (epoch_id, source_binding_id, target_binding_id))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_components (
        epoch_id $STR NOT NULL, component_id $STR NOT NULL, binding_ids_json $JSON NOT NULL,
        status $STR NOT NULL, frontier BIGINT NOT NULL, sealed_frontier BIGINT NOT NULL,
        seal_publication_id $STR,
        PRIMARY KEY (epoch_id, component_id))""",
    """CREATE TABLE IF NOT EXISTS topology_binding_frontiers (
        epoch_id $STR NOT NULL, binding_id $STR NOT NULL, input_versions_json $JSON NOT NULL,
        PRIMARY KEY (epoch_id, binding_id))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_work (
        work_id $STR PRIMARY KEY, epoch_id $STR NOT NULL, component_id $STR NOT NULL,
        binding_id $STR NOT NULL, frontier BIGINT NOT NULL,
        write_start_ts $TS NOT NULL, write_end_ts $TS NOT NULL,
        read_start_ts $TS NOT NULL, read_end_ts $TS NOT NULL,
        input_versions_json $JSON NOT NULL, upstream_frontier_json $JSON NOT NULL,
        binding_digest $STR NOT NULL, status $STR NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
        next_attempt_at $TS, error_json $JSON, output_digest $STR, committed_at $TS)""",
    "CREATE INDEX IF NOT EXISTS topology_epoch_work_pending ON topology_epoch_work (epoch_id, status, write_start_ts, work_id)",
    """CREATE TABLE IF NOT EXISTS topology_epoch_outputs (
        epoch_id $STR NOT NULL, work_id $STR NOT NULL, ref_uri $STR NOT NULL,
        ts $TS NOT NULL, numeric_value $DOUBLE, text_value $STR,
        PRIMARY KEY (epoch_id, work_id, ref_uri, ts))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_retirements (
        epoch_id $STR NOT NULL, ref_uri $STR NOT NULL,
        PRIMARY KEY (epoch_id, ref_uri))""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_claims (
        claim_id $STR PRIMARY KEY, kind $STR NOT NULL, target_id $STR NOT NULL UNIQUE,
        owner $STR, attempt INTEGER NOT NULL DEFAULT 0, expires_at $TS)""",
    """CREATE TABLE IF NOT EXISTS topology_epoch_control (
        control_id INTEGER PRIMARY KEY,
        current_epoch_id $STR, active_epoch_id $STR,
        updated_at $TS NOT NULL)""",
)


def _render(templates: tuple[str, ...], dialect: str) -> tuple[str, ...]:
    types = _DIALECTS[dialect]
    return tuple(Template(template).substitute(types) for template in templates)


def change_range_statements(dialect: str) -> tuple[str, ...]:
    return _render(_CHANGE_RANGE_TEMPLATES, dialect)


def support_statements(dialect: str) -> tuple[str, ...]:
    return _render(_SUPPORT_TEMPLATES, dialect)


def epoch_statements(dialect: str) -> tuple[str, ...]:
    return _render(_EPOCH_TEMPLATES, dialect)
