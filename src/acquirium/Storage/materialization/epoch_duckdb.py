"""DuckDB topology-epoch control plane.

This module is intentionally independent of the retired deployment/generation
tables.  Canonical values are touched only through the publication protocol;
all other state belongs to an epoch-private overlay.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from typing import Any, Callable, Mapping, Sequence

import pyarrow as pa

from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.epochs import (
    EpochBinding, EpochClaim, EpochDefinition, EpochSnapshot, EpochSummary,
    EpochWork, EpochClaimError, StaleEpochError, table_from_rows,
)
from acquirium.Materialization.impact import TimeRange, coalesce_ranges
from acquirium.Materialization.impact import ImpactPolicy
from acquirium.Materialization.topology import resolve_bindings
from acquirium.Storage.duckdb_store import DuckDBStore, REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
from acquirium.Storage.materialization.epoch_common import canonical_json, epoch_binding, epoch_id, global_dag
from acquirium.Storage.materialization.ids import materialization_id
from acquirium.Storage.publication.types import MUTATION_SCHEMA


UTC = timezone.utc


class TopologyEpochDuckDB:
    """Durable epoch state machine backed by the server-owned DuckDB writer."""

    def __init__(self, store: DuckDBStore, *, state_revision_resolver: Callable[[str], str | None] | None = None,
                 transition_hook: Callable[[str], None] | None = None) -> None:
        self._store = store
        self._state_revision_resolver = state_revision_resolver
        self._transition_hook = transition_hook
        with store._lock, store._write_conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_definitions (
                definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, kind VARCHAR NOT NULL,
                source_digest VARCHAR NOT NULL, entrypoint VARCHAR NOT NULL, spec_json VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epochs (
                epoch_id VARCHAR PRIMARY KEY, graph_revision BIGINT NOT NULL, graph_digest VARCHAR NOT NULL,
                catalog_digest VARCHAR NOT NULL, status VARCHAR NOT NULL, superseded_by VARCHAR,
                created_at TIMESTAMP NOT NULL, activated_at TIMESTAMP, compacted_at TIMESTAMP)""")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS topology_epochs_revision_catalog ON topology_epochs (graph_revision, catalog_digest)")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_definition_pins (
                epoch_id VARCHAR NOT NULL, definition_id VARCHAR NOT NULL, state_revision VARCHAR,
                PRIMARY KEY (epoch_id, definition_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_bindings (
                epoch_id VARCHAR NOT NULL, binding_id VARCHAR NOT NULL, definition_id VARCHAR NOT NULL,
                logical_key VARCHAR NOT NULL, content_digest VARCHAR NOT NULL, inputs_json VARCHAR NOT NULL,
                outputs_json VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, state_revision VARCHAR,
                PRIMARY KEY (epoch_id, binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_edges (
                epoch_id VARCHAR NOT NULL, source_binding_id VARCHAR NOT NULL, target_binding_id VARCHAR NOT NULL,
                PRIMARY KEY (epoch_id, source_binding_id, target_binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_components (
                epoch_id VARCHAR NOT NULL, component_id VARCHAR NOT NULL, binding_ids_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL, seal_publication_id VARCHAR,
                PRIMARY KEY (epoch_id, component_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_work (
                work_id VARCHAR PRIMARY KEY, epoch_id VARCHAR NOT NULL, component_id VARCHAR NOT NULL,
                binding_id VARCHAR NOT NULL, start_ts TIMESTAMP NOT NULL, end_ts TIMESTAMP NOT NULL,
                input_versions_json VARCHAR NOT NULL, upstream_frontier_json VARCHAR NOT NULL,
                binding_digest VARCHAR NOT NULL, status VARCHAR NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                error_json VARCHAR, output_digest VARCHAR, committed_at TIMESTAMP)""")
            conn.execute("CREATE INDEX IF NOT EXISTS topology_epoch_work_pending ON topology_epoch_work (epoch_id, status, start_ts, work_id)")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_outputs (
                epoch_id VARCHAR NOT NULL, work_id VARCHAR NOT NULL, ref_uri VARCHAR NOT NULL,
                ts TIMESTAMP NOT NULL, numeric_value DOUBLE, text_value VARCHAR,
                PRIMARY KEY (epoch_id, work_id, ref_uri, ts))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_retirements (
                epoch_id VARCHAR NOT NULL, ref_uri VARCHAR NOT NULL,
                PRIMARY KEY (epoch_id, ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_claims (
                claim_id VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, target_id VARCHAR NOT NULL UNIQUE,
                owner VARCHAR, attempt INTEGER NOT NULL DEFAULT 0, expires_at TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_control (
                control_id INTEGER PRIMARY KEY, current_epoch_id VARCHAR, active_epoch_id VARCHAR,
                compaction_watermark BIGINT NOT NULL DEFAULT -1, updated_at TIMESTAMP NOT NULL)""")
            conn.execute("""INSERT INTO topology_epoch_control (control_id, compaction_watermark, updated_at)
                VALUES (1, -1, ?) ON CONFLICT (control_id) DO NOTHING""", [self._now()])

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC).replace(tzinfo=None)

    @staticmethod
    def _stored_timestamp(value: datetime) -> datetime:
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    @staticmethod
    def _json(value: object) -> str:
        return canonical_json(value)

    @staticmethod
    def _decode(value):
        return json.loads(value) if isinstance(value, str) else value

    def close(self) -> None:
        """The owning DuckDB store owns the connection lifecycle."""

    def _after_transition(self, name: str) -> None:
        """Test seam for simulating a process stop immediately after commit."""
        if self._transition_hook is not None:
            self._transition_hook(name)

    # ----- immutable definitions and epoch identity ---------------------

    def register_definition(self, definition: MaterializationDefinition) -> str:
        spec = definition_spec(definition)
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO topology_epoch_definitions
                (definition_id, name, kind, source_digest, entrypoint, spec_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (definition_id) DO NOTHING""",
                [definition.definition_id, definition.name, definition.kind, definition.source_digest,
                 definition.entrypoint, self._json(spec), self._now()])
        self._after_transition("definition_registered")
        return definition.definition_id

    def _catalog(self, conn) -> tuple[tuple[str, str, str, str, str], ...]:
        rows = conn.execute("""SELECT definition_id, name, source_digest, entrypoint, spec_json
            FROM topology_epoch_definitions WHERE kind = 'transformation' ORDER BY definition_id""").fetchall()
        return tuple(rows)

    def _catalog_digest(self, catalog: Sequence[tuple[str, str, str, str, str]], state_ids: Sequence[tuple]) -> str:
        return sha256(self._json({"catalog": catalog, "state_revisions": state_ids}).encode()).hexdigest()

    def ensure_epoch(self, graph_revision: int, graph_digest: str) -> str:
        """Persist the desired epoch and supersede older desired epochs.

        Supersession only changes control-plane eligibility.  Canonical rows
        remain untouched until a newer component seal publishes a complete
        replacement through the publication protocol.
        """
        # Resolve state pins before opening the epoch writer transaction.  A
        # manager's resolver normally reads the support store, which shares
        # the DuckDB file; nesting that read under this write lock can
        # deadlock.  The immutable definition ids make the resulting catalog
        # identity deterministic even though the read and insert are separate
        # transactions.
        with self._store._own_conn() as conn:
            catalog = self._catalog(conn)
        state_ids = [(row[0], row[1], self._state_revision_resolver(row[0]) if self._state_revision_resolver else None)
                     for row in catalog]
        catalog_digest = self._catalog_digest(catalog, state_ids)
        eid = epoch_id(graph_revision, graph_digest, (*catalog, *state_ids))
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO topology_epochs
                (epoch_id, graph_revision, graph_digest, catalog_digest, status, created_at)
                VALUES (?, ?, ?, ?, 'constructing', ?) ON CONFLICT (epoch_id) DO NOTHING""",
                [eid, graph_revision, graph_digest, catalog_digest, self._now()])
            conn.executemany(
                """INSERT INTO topology_epoch_definition_pins (epoch_id, definition_id, state_revision)
                   VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
                [(eid, definition_id, state_revision) for (definition_id, _, state_revision) in state_ids],
            )
            current = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if current != eid:
                conn.execute("""UPDATE topology_epochs SET status = 'superseded', superseded_by = ?
                    WHERE epoch_id != ? AND status IN ('constructing', 'ready', 'reconciling', 'active')
                    AND graph_revision <= ?""", [eid, eid, graph_revision])
                conn.execute("""UPDATE topology_epoch_components SET status = 'superseded'
                    WHERE epoch_id != ? AND status = 'pending' AND epoch_id IN
                    (SELECT epoch_id FROM topology_epochs WHERE status = 'superseded')""", [eid])
                conn.execute("UPDATE topology_epoch_work SET status = 'superseded' WHERE epoch_id != ? AND status IN ('pending', 'claimed')", [eid])
                conn.execute("UPDATE topology_epoch_control SET current_epoch_id = ?, updated_at = ? WHERE control_id = 1", [eid, self._now()])
        self._after_transition("epoch_ensured")
        return eid

    def current_epoch_id(self) -> str | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()
        return row[0] if row else None

    def epoch_summary(self, epoch_id_value: str) -> EpochSummary:
        with self._store._own_conn() as conn:
            return self._epoch_summary_conn(conn, epoch_id_value)

    def _epoch_summary_conn(self, conn, epoch_id_value: str) -> EpochSummary:
        row = conn.execute("SELECT epoch_id, graph_revision, graph_digest, status FROM topology_epochs WHERE epoch_id = ?", [epoch_id_value]).fetchone()
        if row is None:
            raise KeyError(epoch_id_value)
        components = conn.execute("SELECT count(*), sum(CASE WHEN status = 'sealed' THEN 1 ELSE 0 END) FROM topology_epoch_components WHERE epoch_id = ?", [epoch_id_value]).fetchone()
        return EpochSummary(row[0], row[1], row[2], row[3], int(components[0] or 0), int(components[1] or 0))

    def epoch_bindings(self, epoch_id_value: str) -> tuple[EpochBinding, ...]:
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT epoch_id, binding_id, definition_id, logical_key, content_digest,
                inputs_json, outputs_json, metadata_json, state_revision
                FROM topology_epoch_bindings WHERE epoch_id = ? ORDER BY binding_id""", [epoch_id_value]).fetchall()
        return tuple(EpochBinding(eid, bid, did, logical, digest, self._decode(inputs), self._decode(outputs), self._decode(metadata), state)
                     for eid, bid, did, logical, digest, inputs, outputs, metadata, state in rows)

    def _definition(self, conn, definition_id: str) -> EpochDefinition:
        row = conn.execute("""SELECT definition_id, name, source_digest, entrypoint, kind, spec_json
            FROM topology_epoch_definitions WHERE definition_id = ?""", [definition_id]).fetchone()
        if row is None:
            raise KeyError(definition_id)
        return EpochDefinition(row[0], row[1], row[2], row[3], row[4], self._decode(row[5]))

    def construct_epoch(self, epoch_id_value: str, graph: object, *, maximum_partition_duration: timedelta = timedelta(minutes=15), claim: EpochClaim | None = None) -> EpochSummary:
        """Resolve selectors once and persist the complete immutable topology."""
        if maximum_partition_duration <= timedelta():
            raise ValueError("maximum partition duration must be positive")
        with self._store._lock, self._store._write_conn() as conn:
            if claim is not None:
                self._require_claim(conn, claim)
            row = conn.execute("SELECT graph_revision, graph_digest, status FROM topology_epochs WHERE epoch_id = ?", [epoch_id_value]).fetchone()
            if row is None:
                raise KeyError(epoch_id_value)
            if row[2] in {"ready", "reconciling", "active", "superseded", "compacted"}:
                return self._epoch_summary_conn(conn, epoch_id_value)
            if row[2] != "constructing":
                raise ValueError(f"epoch is not constructible: {row[2]}")
            current = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if current != epoch_id_value:
                conn.execute("UPDATE topology_epochs SET status = 'superseded' WHERE epoch_id = ?", [epoch_id_value])
                return self._epoch_summary_conn(conn, epoch_id_value)
            catalog = self._catalog(conn)

        # Query resolution is deliberately outside the writer transaction.  No
        # worker receives this graph; only this control-plane builder does.
        resolved: list[EpochBinding] = []
        try:
            for definition_id, name, source_digest, entrypoint, spec_json in catalog:
                spec = self._decode(spec_json)
                bindings = resolve_bindings(spec, graph)
                resolved.extend(epoch_binding(epoch_id_value, definition_id, binding) for binding in bindings)
            edges, _, components = global_dag(resolved)
        except Exception as error:
            with self._store._lock, self._store._write_conn() as conn:
                conn.execute("UPDATE topology_epochs SET status = 'failed' WHERE epoch_id = ? AND status = 'constructing'", [epoch_id_value])
            raise ValueError(f"epoch construction failed: {error}") from error

        with self._store._lock, self._store._write_conn() as conn:
            current = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            active_epoch = conn.execute("SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch_id_value]).fetchone()[0]
            if current != epoch_id_value or status == "superseded":
                conn.execute("UPDATE topology_epochs SET status = 'superseded' WHERE epoch_id = ?", [epoch_id_value])
                return self._epoch_summary_conn(conn, epoch_id_value)
            pins = dict(conn.execute(
                "SELECT definition_id, state_revision FROM topology_epoch_definition_pins WHERE epoch_id = ?",
                [epoch_id_value],
            ).fetchall())
            from dataclasses import replace
            resolved = [replace(item, state_revision=pins.get(item.definition_id)) for item in resolved]
            binding_by_id = {item.binding_id: item for item in resolved}
            binding_rows = [
                    (item.epoch_id, item.binding_id, item.definition_id, item.logical_key, item.content_digest,
                     self._json({key: list(value) for key, value in item.inputs.items()}),
                     self._json({key: list(value) for key, value in item.outputs.items()}),
                     self._json(dict(item.metadata)), item.state_revision) for item in resolved]
            if binding_rows:
                conn.executemany("""INSERT INTO topology_epoch_bindings
                (epoch_id, binding_id, definition_id, logical_key, content_digest, inputs_json, outputs_json, metadata_json, state_revision)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""", [
                    row for row in binding_rows])
            edge_rows = [(epoch_id_value, source, target) for source, target in edges]
            if edge_rows:
                conn.executemany("INSERT INTO topology_epoch_edges VALUES (?, ?, ?) ON CONFLICT DO NOTHING", edge_rows)
            component_rows: list[tuple[str, str, str]] = []
            for members in components:
                component_id = materialization_id("epoch-component", epoch_id_value, *members)
                component_rows.append((epoch_id_value, component_id, self._json(list(members))))
            if active_epoch is not None:
                prior_outputs = {ref for (outputs_json,) in conn.execute("SELECT outputs_json FROM topology_epoch_bindings WHERE epoch_id = ?", [active_epoch]).fetchall()
                                 for refs in self._decode(outputs_json).values() for ref in refs}
                current_outputs = {ref for item in resolved for ref in item.output_refs}
                retired = sorted(prior_outputs - current_outputs)
                if retired:
                    retirement_component = materialization_id("epoch-retirements", epoch_id_value, *retired)
                    component_rows.append((epoch_id_value, retirement_component, self._json([])))
                    conn.executemany("INSERT INTO topology_epoch_retirements VALUES (?, ?) ON CONFLICT DO NOTHING",
                                     [(epoch_id_value, ref) for ref in retired])
            if component_rows:
                conn.executemany("""INSERT INTO topology_epoch_components
                    (epoch_id, component_id, binding_ids_json, status) VALUES (?, ?, ?, 'pending') ON CONFLICT DO NOTHING""", component_rows)

            # Each component receives one deterministic partition range set.
            # This makes upstream frontiers explicit and makes the seal boundary
            # the entire weakly connected dependency component.
            works: list[tuple] = []
            for _, component_id, members_json in component_rows:
                members = json.loads(members_json)
                input_refs = sorted({ref for binding_id in members for ref in binding_by_id[binding_id].input_refs})
                ranges = self._retained_ranges(conn, input_refs)
                ranges = self._partition_ranges(ranges, maximum_partition_duration)
                work_ids = {
                    (binding_id, interval.start, interval.end): materialization_id("epoch-work", epoch_id_value, binding_id, interval.start.isoformat(), interval.end.isoformat())
                    for binding_id in members for interval in ranges
                }
                for binding_id in sorted(members):
                    binding = binding_by_id[binding_id]
                    versions = self._stream_versions(conn, binding.input_refs)
                    # The map is keyed by source and the deterministic range is
                    # encoded into the work id; all members share the ranges.
                    for interval in ranges:
                        frontier = {source: work_ids[(source, interval.start, interval.end)] for source, target in edges if target == binding_id}
                        work_id = work_ids[(binding_id, interval.start, interval.end)]
                        works.append((work_id, epoch_id_value, component_id, binding_id,
                                      self._stored_timestamp(interval.start), self._stored_timestamp(interval.end),
                                      self._json(versions), self._json(frontier), binding.content_digest))
            if works:
                conn.executemany("""INSERT INTO topology_epoch_work
                (work_id, epoch_id, component_id, binding_id, start_ts, end_ts, input_versions_json,
                upstream_frontier_json, binding_digest, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                ON CONFLICT (work_id) DO NOTHING""", works)
            new_status = "reconciling" if works else ("ready" if component_rows else "active")
            conn.execute("UPDATE topology_epochs SET status = ? WHERE epoch_id = ? AND status = 'constructing'", [new_status, epoch_id_value])
            if not component_rows:
                conn.execute("UPDATE topology_epoch_control SET active_epoch_id = ?, updated_at = ? WHERE control_id = 1", [epoch_id_value, self._now()])
        self._after_transition("epoch_constructed")
        return self.epoch_summary(epoch_id_value)

    @staticmethod
    def _partition_ranges(ranges: Sequence[TimeRange], maximum: timedelta) -> tuple[TimeRange, ...]:
        result: list[TimeRange] = []
        for interval in coalesce_ranges(ranges):
            start = interval.start
            while start < interval.end:
                end = min(start + maximum, interval.end)
                result.append(TimeRange(start, end))
                start = end
        return tuple(result)

    def _retained_ranges(self, conn, refs: Sequence[str]) -> tuple[TimeRange, ...]:
        if not refs:
            return ()
        rows = conn.execute(f"""SELECT min(t.ts), max(t.ts) FROM {TIMESERIES_TABLE} t
            JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
            WHERE r.ref_uri IN ({','.join('?' for _ in refs)}) AND NOT t.deleted""", list(refs)).fetchone()
        if rows[0] is None:
            return ()
        return (TimeRange(self._aware(rows[0]), self._aware(rows[1]) + timedelta(microseconds=1)),)

    def _stream_versions(self, conn, refs: Sequence[str]) -> dict[str, int]:
        if not refs:
            return {}
        rows = conn.execute(f"""SELECT requested.ref_uri, coalesce(head.current_version, 0)
            FROM (VALUES {','.join('(?)' for _ in refs)}) requested(ref_uri)
            LEFT JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = requested.ref_uri
            LEFT JOIN {STREAM_HEADS_TABLE} head ON head.ref_id = ids.ref_id ORDER BY requested.ref_uri""", list(refs)).fetchall()
        return dict(rows)

    def plan_data_changes(self, *, maximum_partition_duration: timedelta = timedelta(minutes=15)) -> int:
        """Append a manifest revision for canonical input changes.

        The epoch topology stays immutable while its reconciliation frontier
        advances.  All bindings in an affected weak component receive the same
        dirty ranges, which is conservative but keeps the component seal
        invariant simple and deterministic.
        """
        with self._store._lock, self._store._write_conn() as conn:
            epoch = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if epoch is None:
                return 0
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch]).fetchone()[0]
            if status in {"superseded", "failed", "compacted"}:
                return 0
            component_members = {component: self._decode(members) for component, members in
                                 conn.execute("SELECT component_id, binding_ids_json FROM topology_epoch_components WHERE epoch_id = ?", [epoch]).fetchall()}
            component_for = {binding_id: component for component, members in component_members.items() for binding_id in members}
            binding_rows = [(binding_id, component_for[binding_id], definition_id, inputs_json)
                            for binding_id, definition_id, inputs_json in conn.execute("""SELECT binding_id, definition_id, inputs_json
                                FROM topology_epoch_bindings WHERE epoch_id = ? ORDER BY binding_id""", [epoch]).fetchall()]
            if not binding_rows:
                return 0
            owners = {ref for _, _, outputs_json in conn.execute("SELECT binding_id, definition_id, outputs_json FROM topology_epoch_bindings WHERE epoch_id = ?", [epoch]).fetchall()
                      for refs in self._decode(outputs_json).values() for ref in refs}
            dirty: dict[str, list[TimeRange]] = {binding_id: [] for binding_id, *_ in binding_rows}
            for binding_id, _, definition_id, inputs_json in binding_rows:
                binding_inputs = self._decode(inputs_json)
                raw_refs = sorted({ref for refs in binding_inputs.values() for ref in refs if ref not in owners})
                prior: dict[str, int] = {ref: 0 for ref in raw_refs}
                for (versions_json,) in conn.execute("SELECT input_versions_json FROM topology_epoch_work WHERE epoch_id = ? AND binding_id = ?", [epoch, binding_id]).fetchall():
                    versions = self._decode(versions_json)
                    for ref in raw_refs:
                        prior[ref] = max(prior[ref], int(versions.get(ref, 0)))
                definition = self._definition(conn, definition_id)
                impact = ImpactPolicy.from_json(self._decode(definition.spec).get("impact") or {"kind": "pointwise"})
                for ref in raw_refs:
                    head = self._stream_versions(conn, (ref,)).get(ref, 0)
                    if head <= prior[ref]:
                        continue
                    changes = conn.execute("""SELECT start_ts, end_ts FROM stream_change_ranges
                        WHERE ref_uri = ? AND stream_version > ? AND stream_version <= ? ORDER BY start_ts""", [ref, prior[ref], head]).fetchall()
                    for start, end in changes:
                        changed = TimeRange(self._aware(start), self._aware(end))
                        if impact.kind == "full_history":
                            dirty[binding_id].extend(self._retained_ranges(conn, raw_refs))
                        else:
                            dirty[binding_id].append(impact.affected(changed))
            component_dirty: dict[str, list[TimeRange]] = {component: [] for component in component_members}
            for binding_id, component, *_ in binding_rows:
                component_dirty[component].extend(dirty[binding_id])
            inserted = 0
            for component, members in sorted(component_members.items()):
                ranges = self._partition_ranges(coalesce_ranges(component_dirty[component]), maximum_partition_duration)
                if not ranges:
                    continue
                versions_by_binding = {
                    binding_id: self._stream_versions(conn, tuple(sorted({ref for refs in self._decode(inputs_json).values() for ref in refs})))
                    for binding_id, _, _, inputs_json in binding_rows if binding_id in members
                }
                work_ids = {(binding_id, interval.start, interval.end): materialization_id("epoch-data-work", epoch, binding_id, interval.start.isoformat(), interval.end.isoformat(), versions_by_binding[binding_id])
                            for binding_id in members for interval in ranges}
                edge_rows = conn.execute("SELECT source_binding_id, target_binding_id FROM topology_epoch_edges WHERE epoch_id = ?", [epoch]).fetchall()
                for binding_id, _, definition_id, inputs_json in binding_rows:
                    if binding_id not in members:
                        continue
                    binding_row = conn.execute("SELECT content_digest FROM topology_epoch_bindings WHERE epoch_id = ? AND binding_id = ?", [epoch, binding_id]).fetchone()
                    for interval in ranges:
                        frontier = {source: work_ids[(source, interval.start, interval.end)] for source, target in edge_rows if target == binding_id and source in members}
                        work_id = work_ids[(binding_id, interval.start, interval.end)]
                        conn.execute("""INSERT INTO topology_epoch_work
                            (work_id, epoch_id, component_id, binding_id, start_ts, end_ts, input_versions_json,
                             upstream_frontier_json, binding_digest, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                            ON CONFLICT (work_id) DO NOTHING""", [work_id, epoch, component, binding_id,
                            self._stored_timestamp(interval.start), self._stored_timestamp(interval.end),
                            self._json(versions_by_binding[binding_id]), self._json(frontier), binding_row[0]])
                        inserted += 1
                conn.execute("UPDATE topology_epoch_components SET status = 'pending', seal_publication_id = NULL WHERE epoch_id = ? AND component_id = ?", [epoch, component])
            if inserted:
                conn.execute("UPDATE topology_epochs SET status = 'reconciling' WHERE epoch_id = ?", [epoch])
        if inserted:
            self._after_transition("data_frontier_planned")
        return inserted

    # ----- one shared claim contract -------------------------------------

    def claim(self, kind: str, target_id: str, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        if not owner or duration <= timedelta():
            raise ValueError("claim owner and positive duration are required")
        now = self._now()
        expires = now + duration
        claim_id = materialization_id("topology-claim", kind, target_id)
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT claim_id, kind, target_id, owner, attempt, expires_at FROM topology_epoch_claims WHERE target_id = ?", [target_id]).fetchone()
            if row is not None and row[3] is not None and row[5] > now:
                return None
            attempt = int(row[4]) + 1 if row else 1
            conn.execute("""INSERT INTO topology_epoch_claims (claim_id, kind, target_id, owner, attempt, expires_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (target_id) DO UPDATE SET kind = excluded.kind,
                owner = excluded.owner, attempt = excluded.attempt, expires_at = excluded.expires_at""",
                         [claim_id, kind, target_id, owner, attempt, expires])
        self._after_transition("claim_acquired")
        return EpochClaim(claim_id, kind, target_id, owner, attempt, expires.replace(tzinfo=UTC))

    def _require_claim(self, conn, claim: EpochClaim, *, now: datetime | None = None) -> None:
        now = now or self._now()
        row = conn.execute("SELECT owner, attempt, expires_at FROM topology_epoch_claims WHERE claim_id = ?", [claim.claim_id]).fetchone()
        if row is None or row[0] != claim.owner or row[1] != claim.attempt or row[2] <= now:
            raise EpochClaimError("claim is stale")

    def release_claim(self, claim: EpochClaim) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            self._require_claim(conn, claim)
            conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
        self._after_transition("claim_released")

    # ----- execution against persisted epoch bindings -------------------

    def claim_next_work(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        now = self._now()
        with self._store._lock, self._store._write_conn() as conn:
            # Expired claims make claimed work retryable.  A claim is only a
            # liveness marker; the work row is the durable desired state.
            conn.execute("""UPDATE topology_epoch_work SET status = 'pending'
                WHERE status = 'claimed' AND work_id IN
                (SELECT work_id FROM topology_epoch_work w LEFT JOIN topology_epoch_claims c
                 ON c.target_id = w.work_id WHERE c.owner IS NULL OR c.expires_at <= ?)""", [now])
            rows = conn.execute("""SELECT w.work_id, w.upstream_frontier_json FROM topology_epoch_work w
                JOIN topology_epochs e ON e.epoch_id = w.epoch_id
                WHERE w.status = 'pending' AND e.status IN ('reconciling', 'ready')
                AND e.epoch_id = (SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1)
                ORDER BY w.start_ts, w.work_id""").fetchall()
        for work_id, frontier_json in rows:
            frontier = self._decode(frontier_json)
            with self._store._own_conn() as conn:
                if any(conn.execute("SELECT status FROM topology_epoch_work WHERE work_id = ?", [dependency]).fetchone() != ("committed",) for dependency in frontier.values()):
                    continue
            claim = self.claim("reconcile", work_id, owner, duration=duration)
            if claim is None:
                continue
            claimed = False
            with self._store._lock, self._store._write_conn() as conn:
                changed = conn.execute("UPDATE topology_epoch_work SET status = 'claimed', attempt = attempt + 1 WHERE work_id = ? AND status = 'pending'", [work_id]).rowcount
                if changed:
                    claimed = True
                else:
                    conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
            if claimed:
                self._after_transition("work_claimed")
                return claim
        return None

    def _work(self, conn, work_id: str) -> EpochWork:
        row = conn.execute("""SELECT work_id, epoch_id, component_id, binding_id, start_ts, end_ts,
            input_versions_json, upstream_frontier_json, binding_digest, status, attempt
            FROM topology_epoch_work WHERE work_id = ?""", [work_id]).fetchone()
        if row is None:
            raise KeyError(work_id)
        return EpochWork(row[0], row[1], row[2], row[3], TimeRange(self._aware(row[4]), self._aware(row[5])),
                         self._decode(row[6]), self._decode(row[7]), row[8], row[9], row[10])

    def snapshot(self, claim: EpochClaim) -> EpochSnapshot:
        if claim.kind != "reconcile":
            raise EpochClaimError("claim is not a reconcile claim")
        with self._store._own_conn() as conn:
            self._require_claim(conn, claim)
            work = self._work(conn, claim.target_id)
            if work.status != "claimed" or work.attempt != claim.attempt:
                raise EpochClaimError("work attempt is stale")
            binding_row = conn.execute("""SELECT epoch_id, binding_id, definition_id, logical_key, content_digest,
                inputs_json, outputs_json, metadata_json, state_revision
                FROM topology_epoch_bindings WHERE epoch_id = ? AND binding_id = ?""", [work.epoch_id, work.binding_id]).fetchone()
            if binding_row is None:
                raise KeyError(work.binding_id)
            binding = EpochBinding(binding_row[0], binding_row[1], binding_row[2], binding_row[3], binding_row[4],
                                   json.loads(binding_row[5]), json.loads(binding_row[6]), json.loads(binding_row[7]), binding_row[8])
            definition = self._definition(conn, binding.definition_id)
            # Output ownership is recovered from the immutable binding rows so
            # no graph query or worker-side selector evaluation is possible.
            owners = {ref for row in conn.execute("SELECT outputs_json FROM topology_epoch_bindings WHERE epoch_id = ?", [work.epoch_id]).fetchall()
                      for refs in self._decode(row[0]).values() for ref in refs}
            rows: list[dict[str, Any]] = []
            for ref in binding.input_refs:
                if ref in owners:
                    source_rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM (
                        SELECT o.ref_uri, o.ts, o.numeric_value, o.text_value,
                               row_number() OVER (PARTITION BY o.ref_uri, o.ts ORDER BY w.committed_at DESC, o.work_id DESC) AS recency
                        FROM topology_epoch_outputs o JOIN topology_epoch_work w ON w.work_id = o.work_id
                        WHERE o.epoch_id = ? AND o.ref_uri = ? AND w.status = 'committed'
                          AND o.ts >= ? AND o.ts < ?) latest WHERE recency = 1 ORDER BY ts""",
                                               [work.epoch_id, ref, work.interval.start.replace(tzinfo=None), work.interval.end.replace(tzinfo=None)]).fetchall()
                else:
                    source_rows = conn.execute(f"""SELECT r.ref_uri, t.ts, t.numeric_value, t.text_value
                        FROM {TIMESERIES_TABLE} t JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
                        WHERE r.ref_uri = ? AND t.ts >= ? AND t.ts < ? AND NOT t.deleted ORDER BY t.ts""",
                                               [ref, work.interval.start.replace(tzinfo=None), work.interval.end.replace(tzinfo=None)]).fetchall()
                rows.extend({"operation": "upsert", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": numeric, "text_value": text}
                            for _, ts, numeric, text in source_rows)
            rows.sort(key=lambda item: (item["ref_uri"], item["ts"]))
            return EpochSnapshot(work, binding, definition, table_from_rows(rows), dict(work.input_versions))

    def _current_raw_versions(self, conn, refs: Sequence[str], managed: set[str]) -> dict[str, int]:
        return {ref: version for ref, version in self._stream_versions(conn, refs).items() if ref not in managed}

    def commit_work(self, snapshot: EpochSnapshot, replacement: pa.Table, claim: EpochClaim) -> str:
        """Persist a replacement in the private overlay after all validations."""
        if claim.kind != "reconcile" or claim.target_id != snapshot.work.work_id:
            raise EpochClaimError("claim does not own work")
        required = {"ref_uri", "ts", "numeric_value", "text_value"}
        if not required.issubset(replacement.column_names):
            raise ValueError("replacement must contain ref_uri, ts, numeric_value, and text_value")
        rows = replacement.select(["ref_uri", "ts", "numeric_value", "text_value"]).to_pylist()
        if any(row["ref_uri"] not in snapshot.binding.output_refs or not (snapshot.work.interval.start <= row["ts"] < snapshot.work.interval.end) for row in rows):
            raise ValueError("replacement lies outside the persisted epoch binding or work range")
        digest = sha256(self._json([(row["ref_uri"], row["ts"].isoformat(), row["numeric_value"], row["text_value"]) for row in rows]).encode()).hexdigest()
        with self._store._lock, self._store._write_conn() as conn:
            state = conn.execute("SELECT status, attempt, binding_digest, epoch_id, output_digest FROM topology_epoch_work WHERE work_id = ?", [snapshot.work.work_id]).fetchone()
            if state is None:
                raise KeyError(snapshot.work.work_id)
            current = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            epoch_status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [snapshot.work.epoch_id]).fetchone()[0]
            if current != snapshot.work.epoch_id or epoch_status == "superseded" or state[0] == "superseded":
                if state[0] != "committed":
                    conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                raise StaleEpochError("epoch was superseded")
            if state[0] == "committed":
                if state[2] != snapshot.work.binding_digest:
                    raise StaleEpochError("binding identity changed")
                return state[4] if len(state) > 4 else state[2]
            self._require_claim(conn, claim)
            if state[:4] != ("claimed", claim.attempt, snapshot.work.binding_digest, snapshot.work.epoch_id):
                raise EpochClaimError("work attempt is stale")
            binding_state = conn.execute(
                "SELECT binding_id, content_digest FROM topology_epoch_bindings WHERE epoch_id = ? AND binding_id = ?",
                [snapshot.binding.epoch_id, snapshot.binding.binding_id],
            ).fetchone()
            if binding_state is None or binding_state[1] != snapshot.binding.content_digest:
                raise StaleEpochError("resolved binding identity changed")
            managed = {ref for row in conn.execute("SELECT outputs_json FROM topology_epoch_bindings WHERE epoch_id = ?", [snapshot.work.epoch_id]).fetchall()
                       for refs in self._decode(row[0]).values() for ref in refs}
            actual_versions = self._current_raw_versions(conn, snapshot.binding.input_refs, managed)
            expected_versions = {ref: version for ref, version in snapshot.input_versions.items() if ref not in managed}
            if actual_versions != expected_versions:
                conn.execute("UPDATE topology_epoch_work SET status = 'pending', error_json = ? WHERE work_id = ?", [self._json({"type": "stale_input"}), snapshot.work.work_id])
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                raise StaleEpochError("raw input versions changed after snapshot")
            for dependency in self._decode(conn.execute("SELECT upstream_frontier_json FROM topology_epoch_work WHERE work_id = ?", [snapshot.work.work_id]).fetchone()[0]).values():
                dep = conn.execute("SELECT status FROM topology_epoch_work WHERE work_id = ?", [dependency]).fetchone()
                if dep != ("committed",):
                    raise StaleEpochError("upstream dependency frontier is not committed")
            conn.execute("DELETE FROM topology_epoch_outputs WHERE epoch_id = ? AND work_id = ?", [snapshot.work.epoch_id, snapshot.work.work_id])
            if rows:
                conn.executemany("""INSERT INTO topology_epoch_outputs
                    (epoch_id, work_id, ref_uri, ts, numeric_value, text_value) VALUES (?, ?, ?, ?, ?, ?)""",
                                 [(snapshot.work.epoch_id, snapshot.work.work_id, row["ref_uri"], self._stored_timestamp(row["ts"]), row["numeric_value"], row["text_value"]) for row in rows])
            conn.execute("""UPDATE topology_epoch_work SET status = 'committed', output_digest = ?, committed_at = ?
                WHERE work_id = ?""", [digest, self._now(), snapshot.work.work_id])
            conn.execute("UPDATE topology_epochs SET status = 'reconciling' WHERE epoch_id = ? AND status = 'ready'", [snapshot.work.epoch_id])
            conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
        self._after_transition("work_committed")
        return digest

    def fail_work(self, claim: EpochClaim, error: Mapping[str, object]) -> None:
        """Return claimed work to the queue; an abandoned claim recovers too."""
        if claim.kind != "reconcile":
            raise EpochClaimError("claim is not a reconcile claim")
        with self._store._lock, self._store._write_conn() as conn:
            self._require_claim(conn, claim)
            changed = conn.execute("""UPDATE topology_epoch_work SET status = 'pending', error_json = ?
                WHERE work_id = ? AND status = 'claimed' AND attempt = ?""",
                                  [self._json(dict(error)), claim.target_id, claim.attempt]).rowcount
            if not changed:
                raise EpochClaimError("work attempt is stale")
            conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
        self._after_transition("work_failed")

    # ----- atomic component sealing and activation ----------------------

    def claim_next_component(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT c.epoch_id, c.component_id FROM topology_epoch_components c
                JOIN topology_epochs e ON e.epoch_id = c.epoch_id
                WHERE c.status = 'pending' AND e.status IN ('reconciling', 'ready')
                  AND e.epoch_id = (SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1)
                ORDER BY c.component_id""").fetchall()
            for epoch, component in rows:
                work_states = conn.execute("SELECT status FROM topology_epoch_work WHERE epoch_id = ? AND component_id = ?", [epoch, component]).fetchall()
                if any(row[0] != "committed" for row in work_states):
                    continue
                claim = self.claim("seal", f"{epoch}:{component}", owner, duration=duration)
                if claim is not None:
                    return claim
        return None

    def _canonical_rows(self, conn, refs: Sequence[str], start: datetime, end: datetime) -> list[tuple[str, datetime]]:
        if not refs:
            return []
        return conn.execute(f"""SELECT r.ref_uri, t.ts FROM {TIMESERIES_TABLE} t
            JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
            WHERE r.ref_uri IN ({','.join('?' for _ in refs)}) AND t.ts >= ? AND t.ts < ? AND NOT t.deleted""",
                            [*refs, start.replace(tzinfo=None), end.replace(tzinfo=None)]).fetchall()

    def _all_canonical_rows(self, conn, refs: Sequence[str]) -> list[tuple[str, datetime]]:
        if not refs:
            return []
        return conn.execute(f"""SELECT r.ref_uri, t.ts FROM {TIMESERIES_TABLE} t
            JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
            WHERE r.ref_uri IN ({','.join('?' for _ in refs)}) AND NOT t.deleted""", list(refs)).fetchall()

    def seal_component(self, claim: EpochClaim) -> str | None:
        if claim.kind != "seal":
            raise EpochClaimError("claim is not a seal claim")
        epoch, component = claim.target_id.split(":", 1)
        with self._store._lock, self._store._write_conn() as conn:
            self._require_claim(conn, claim)
            control, active_epoch = conn.execute(
                "SELECT current_epoch_id, active_epoch_id FROM topology_epoch_control WHERE control_id = 1"
            ).fetchone()
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch]).fetchone()
            component_row = conn.execute("SELECT binding_ids_json, status FROM topology_epoch_components WHERE epoch_id = ? AND component_id = ?", [epoch, component]).fetchone()
            if status is None or component_row is None:
                raise KeyError(claim.target_id)
            if control != epoch or status[0] == "superseded":
                conn.execute("UPDATE topology_epoch_components SET status = 'superseded' WHERE epoch_id = ? AND component_id = ?", [epoch, component])
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                raise StaleEpochError("epoch was superseded before seal")
            if component_row[1] == "sealed":
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                return conn.execute("SELECT seal_publication_id FROM topology_epoch_components WHERE epoch_id = ? AND component_id = ?", [epoch, component]).fetchone()[0]
            work_states = conn.execute("SELECT status FROM topology_epoch_work WHERE epoch_id = ? AND component_id = ?", [epoch, component]).fetchall()
            if any(row[0] != "committed" for row in work_states):
                raise ValueError("component has unfinished work")
            binding_ids = self._decode(component_row[0])
            output_refs = sorted({ref for binding_id in binding_ids for row in conn.execute("SELECT outputs_json FROM topology_epoch_bindings WHERE epoch_id = ? AND binding_id = ?", [epoch, binding_id]).fetchone() for refs in self._decode(row).values() for ref in refs})
            if not binding_ids:
                output_refs.extend(row[0] for row in conn.execute("SELECT ref_uri FROM topology_epoch_retirements WHERE epoch_id = ?", [epoch]).fetchall()
                                   if row[0] not in output_refs)
            work_intervals = [
                (self._aware(start), self._aware(end))
                for start, end in conn.execute(
                    "SELECT start_ts, end_ts FROM topology_epoch_work WHERE epoch_id = ? AND component_id = ? ORDER BY start_ts, end_ts",
                    [epoch, component],
                ).fetchall()
            ]
            if not work_intervals and output_refs:
                retained = self._retained_ranges(conn, output_refs)
                if retained:
                    work_intervals = [(retained[0].start, retained[-1].end)]
            mutations: list[dict[str, object]] = []
            if work_intervals:
                # A newly constructed epoch is a complete replacement for
                # its component, so rows outside the new retained frontier
                # must also be removed.  A data-frontier manifest on the
                # already-active epoch is incremental and only owns its
                # explicit work intervals.
                if active_epoch is not None and active_epoch != epoch:
                    existing = set(self._all_canonical_rows(conn, output_refs))
                else:
                    existing = {
                        (ref, ts)
                        for start, end in work_intervals
                        for ref, ts in self._canonical_rows(conn, output_refs, start, end)
                    }
                staged = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM (
                    SELECT o.ref_uri, o.ts, o.numeric_value, o.text_value,
                           row_number() OVER (PARTITION BY o.ref_uri, o.ts ORDER BY w.committed_at DESC, o.work_id DESC) AS recency
                    FROM topology_epoch_outputs o JOIN topology_epoch_work w ON w.work_id = o.work_id
                    WHERE o.epoch_id = ? AND w.epoch_id = ? AND w.component_id = ?) latest
                    WHERE recency = 1 ORDER BY ref_uri, ts""", [epoch, epoch, component]).fetchall()
                staged_keys = {(ref, ts) for ref, ts, _, _ in staged}
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": None, "text_value": None}
                                 for ref, ts in sorted(existing - staged_keys))
                mutations.extend({"operation": "upsert", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": numeric, "text_value": text}
                                 for ref, ts, numeric, text in staged)
            seal_digest = sha256(self._json(mutations).encode()).hexdigest() if mutations else "empty"
            publication_id = f"topology-epoch:{epoch}:component:{component}:seal:{seal_digest}"
            if mutations:
                receipt = self._apply_canonical_publication(conn, publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
                publication_id = receipt.publication_id
            else:
                publication_id = None
            conn.execute("UPDATE topology_epoch_components SET status = 'sealed', seal_publication_id = ? WHERE epoch_id = ? AND component_id = ?", [publication_id, epoch, component])
            pending = conn.execute("SELECT count(*) FROM topology_epoch_components WHERE epoch_id = ? AND status != 'sealed'", [epoch]).fetchone()[0]
            if pending == 0:
                conn.execute("UPDATE topology_epochs SET status = 'active', activated_at = ? WHERE epoch_id = ?", [self._now(), epoch])
                conn.execute("UPDATE topology_epoch_control SET active_epoch_id = ?, updated_at = ? WHERE control_id = 1", [epoch, self._now()])
            conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
        self._after_transition("component_sealed")
        return publication_id

    def _apply_canonical_publication(self, conn, publication_id: str, mutations: pa.Table):
        from acquirium.Storage.publication.duckdb import PublicationDuckDB
        return PublicationDuckDB(self._store)._apply_publication(conn, publication_id, mutations)

    def activate_ready(self, owner: str = "activation", *, duration: timedelta = timedelta(minutes=5)) -> tuple[str, ...]:
        sealed: list[str] = []
        while True:
            claim = self.claim_next_component(owner, duration=duration)
            if claim is None:
                break
            self.seal_component(claim)
            sealed.append(claim.target_id)
        return tuple(sealed)

    def active_epoch_id(self) -> str | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()
        return row[0] if row else None

    # ----- retention/compaction -----------------------------------------

    def compact(self, graph_revision: int) -> int:
        """Discard old private overlays while retaining epoch identity and receipts."""
        with self._store._lock, self._store._write_conn() as conn:
            active = conn.execute("SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if active is None:
                rows = conn.execute(
                    "SELECT epoch_id FROM topology_epochs WHERE graph_revision < ? AND status = 'superseded' AND compacted_at IS NULL",
                    [graph_revision],
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT epoch_id FROM topology_epochs WHERE graph_revision < ? AND status = 'superseded' AND epoch_id != ? AND compacted_at IS NULL",
                    [graph_revision, active],
                ).fetchall()
            for (epoch,) in rows:
                conn.execute("DELETE FROM topology_epoch_outputs WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_work WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_retirements WHERE epoch_id = ?", [epoch])
                conn.execute("UPDATE topology_epochs SET status = 'compacted', compacted_at = ? WHERE epoch_id = ?", [self._now(), epoch])
            conn.execute("UPDATE topology_epoch_control SET compaction_watermark = ?, updated_at = ? WHERE control_id = 1", [graph_revision, self._now()])
        if rows:
            self._after_transition("epochs_compacted")
        return len(rows)
