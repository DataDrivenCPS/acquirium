"""DuckDB topology-epoch control plane.

Canonical values are touched only through the publication protocol; all other
state belongs to an epoch-private overlay selected by named deployments.
"""
from __future__ import annotations

from dataclasses import replace
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
from acquirium.Storage.materialization.types import MAX_PARTITION_ATTEMPTS
from acquirium.Materialization.impact import TimeRange, coalesce_ranges
from acquirium.Materialization.impact import ImpactPolicy
from acquirium.Materialization.topology import resolve_bindings
from acquirium.Storage.duckdb_store import DuckDBStore, REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
from acquirium.Storage.materialization.dialect import DuckDBCodecs
from acquirium.Storage.materialization.epoch_common import epoch_binding, epoch_id, global_dag
from acquirium.Storage.materialization.ids import materialization_id
from acquirium.Storage.materialization.schema import change_range_statements, epoch_statements
from acquirium.Storage.publication.types import MUTATION_SCHEMA


UTC = timezone.utc


class TopologyEpochDuckDB(DuckDBCodecs):
    """Durable epoch state machine backed by the server-owned DuckDB writer."""

    def __init__(self, store: DuckDBStore, *, state_revision_resolver: Callable[[], Mapping[str, object]] | None = None,
                 query_resolver: Callable[..., Any] | None = None,
                 transition_hook: Callable[[str], None] | None = None) -> None:
        self._store = store
        self._state_revision_resolver = state_revision_resolver
        self._query_resolver = query_resolver
        self._transition_hook = transition_hook
        with self._store._lock, self._store._write_conn() as conn:
            for statement in (*change_range_statements(self._DIALECT), *epoch_statements(self._DIALECT)):
                conn.execute(statement)
            conn.execute("""INSERT INTO topology_epoch_control (control_id, updated_at)
                VALUES (1, ?) ON CONFLICT (control_id) DO NOTHING""", [self._now()])

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
            conn.execute("""INSERT INTO materialization_definitions
                (definition_id, name, kind, source_digest, entrypoint, spec_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (definition_id) DO NOTHING""",
                [definition.definition_id, definition.name, definition.kind, definition.source_digest,
                 definition.entrypoint, self._json(spec), self._now()])
        self._after_transition("definition_registered")
        return definition.definition_id

    def deploy_definition(self, name: str, definition_id: str, graph: object) -> int:
        """Validate and select one immutable definition for a deployment name."""
        if not name:
            raise ValueError("deployment name is required")
        with self._store._lock, self._store._write_conn() as conn:
            self._lock_deployments(conn)
            selected = dict(conn.execute("SELECT name, definition_id FROM topology_deployments").fetchall())
            selected[name] = definition_id
            self._validate_deployments(conn, selected, graph)
            prior = conn.execute("SELECT generation FROM topology_deployments WHERE name = ?", [name]).fetchone()
            generation = int(prior[0]) + 1 if prior else 1
            conn.execute("""INSERT INTO topology_deployments (name, definition_id, generation, updated_at)
                VALUES (?, ?, ?, ?) ON CONFLICT (name) DO UPDATE SET
                definition_id = excluded.definition_id, generation = excluded.generation,
                updated_at = excluded.updated_at""", [name, definition_id, generation, self._now()])
        self._after_transition("definition_deployed")
        return generation

    def remove_deployment(self, name: str, graph: object) -> None:
        """Validate and remove one named deployment from desired topology."""
        with self._store._lock, self._store._write_conn() as conn:
            self._lock_deployments(conn)
            selected = dict(conn.execute("SELECT name, definition_id FROM topology_deployments").fetchall())
            if name not in selected:
                raise KeyError(name)
            del selected[name]
            self._validate_deployments(conn, selected, graph)
            conn.execute("DELETE FROM topology_deployments WHERE name = ?", [name])
        self._after_transition("definition_undeployed")

    @staticmethod
    def _lock_deployments(conn) -> None:
        """Serialize updates to the deployment map (DuckDB has one writer)."""

    @staticmethod
    def _lock_component(conn, epoch: str, component: str) -> None:
        """Serialize one component frontier (DuckDB has one writer)."""

    def _validate_deployments(self, conn, selected: Mapping[str, str], graph: object) -> None:
        rows = conn.execute("""SELECT definition_id, source_digest, entrypoint, spec_json FROM materialization_definitions
            WHERE kind = 'transformation'""").fetchall()
        specs = {row[0]: (row[1], row[2], self._decode(row[3])) for row in rows}
        missing = sorted(set(selected.values()) - set(specs))
        if missing:
            raise KeyError(missing[0])
        resolved: list[EpochBinding] = []
        for _, definition_id in sorted(selected.items()):
            source_digest, entrypoint, spec = specs[definition_id]
            resolved.extend(epoch_binding("candidate", definition_id, binding)
                            for binding in resolve_bindings(
                                spec, graph, entrypoint=entrypoint,
                                source_digest=source_digest, query_resolver=self._query_resolver))
        global_dag(resolved)

    def _catalog(self, conn) -> tuple[tuple[str, str, str, str, str], ...]:
        rows = conn.execute("""SELECT d.definition_id, p.name, d.source_digest, d.entrypoint, d.spec_json
            FROM topology_deployments p JOIN materialization_definitions d
              ON d.definition_id = p.definition_id
            ORDER BY p.name""").fetchall()
        return tuple(rows)

    def _catalog_digest(self, catalog: Sequence[tuple[str, str, str, str, str]], state_ids: Sequence[tuple]) -> str:
        return sha256(self._json({"catalog": catalog, "state_revisions": state_ids}).encode()).hexdigest()

    def ensure_epoch(self, graph_revision: int, graph_digest: str) -> str:
        """Persist a candidate epoch without disturbing the current topology."""
        # Read the catalog and active state pins before opening the epoch
        # writer transaction; a manager's resolver normally reads the support
        # store, which shares the DuckDB file, and nesting that read under
        # this write lock can deadlock.  The resolver returns every active
        # revision keyed by binding id -- no query resolution happens here,
        # and pins for bindings absent from the constructed topology are
        # harmless because construction never consults them.
        with self._store._own_conn() as conn:
            catalog = self._catalog(conn)
        state_ids: list[tuple[str, str | None, str | None, datetime | None]] = []
        if self._state_revision_resolver is not None:
            for binding_id, revision in self._state_revision_resolver().items():
                if revision is None:
                    continue
                if isinstance(revision, str):
                    state_ids.append((binding_id, revision, None, None))
                else:
                    state_ids.append((binding_id, getattr(revision, "revision_id"),
                                      getattr(revision, "policy", None),
                                      getattr(revision, "effective_from", None)))
        state_ids.sort(key=lambda item: item[0])
        catalog_digest = self._catalog_digest(catalog, state_ids)
        eid = epoch_id(graph_revision, graph_digest, (*catalog, *state_ids))
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO topology_epochs
                (epoch_id, graph_revision, graph_digest, catalog_digest, status, created_at)
                VALUES (?, ?, ?, ?, 'constructing', ?) ON CONFLICT (epoch_id) DO NOTHING""",
                [eid, graph_revision, graph_digest, catalog_digest, self._now()])
            if state_ids:
                conn.executemany(
                    """INSERT INTO topology_epoch_binding_pins
                       (epoch_id, binding_id, state_revision, policy, effective_from)
                       VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
                    [(eid, binding_id, state_revision, policy,
                      self._stored_timestamp(effective_from) if effective_from else None)
                     for binding_id, state_revision, policy, effective_from in state_ids],
                )
            epoch_status = conn.execute(
                "SELECT status FROM topology_epochs WHERE epoch_id = ?", [eid]
            ).fetchone()[0]
            candidate = conn.execute("SELECT candidate_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if epoch_status == "constructing" and candidate != eid:
                conn.execute("""UPDATE topology_epochs SET status = 'superseded', superseded_by = ?
                    WHERE epoch_id != ? AND status = 'constructing'""", [eid, eid])
                conn.execute("UPDATE topology_epoch_control SET candidate_epoch_id = ?, updated_at = ? WHERE control_id = 1", [eid, self._now()])
        self._after_transition("epoch_ensured")
        return eid

    def current_epoch_id(self) -> str | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()
        return row[0] if row else None

    def candidate_epoch_id(self) -> str | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT candidate_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()
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
            FROM materialization_definitions WHERE definition_id = ?""", [definition_id]).fetchone()
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
            candidate = conn.execute("SELECT candidate_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if candidate != epoch_id_value:
                conn.execute("UPDATE topology_epochs SET status = 'superseded' WHERE epoch_id = ?", [epoch_id_value])
                return self._epoch_summary_conn(conn, epoch_id_value)
            catalog = self._catalog(conn)

        # Query resolution is deliberately outside the writer transaction.  No
        # worker receives this graph; only this control-plane builder does.
        resolved: list[EpochBinding] = []
        try:
            for definition_id, name, source_digest, entrypoint, spec_json in catalog:
                spec = self._decode(spec_json)
                bindings = resolve_bindings(
                    spec, graph, entrypoint=entrypoint, source_digest=source_digest,
                    query_resolver=self._query_resolver)
                resolved.extend(epoch_binding(epoch_id_value, definition_id, binding) for binding in bindings)
            edges, _, components = global_dag(resolved)
        except Exception as error:
            with self._store._lock, self._store._write_conn() as conn:
                conn.execute("UPDATE topology_epochs SET status = 'failed' WHERE epoch_id = ? AND status = 'constructing'", [epoch_id_value])
            raise ValueError(f"epoch construction failed: {error}") from error

        with self._store._lock, self._store._write_conn() as conn:
            candidate = conn.execute("SELECT candidate_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            active_epoch = conn.execute("SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch_id_value]).fetchone()[0]
            if candidate != epoch_id_value or status == "superseded":
                conn.execute("UPDATE topology_epochs SET status = 'superseded' WHERE epoch_id = ?", [epoch_id_value])
                return self._epoch_summary_conn(conn, epoch_id_value)
            pin_rows = conn.execute(
                """SELECT binding_id, state_revision, policy, effective_from
                   FROM topology_epoch_binding_pins WHERE epoch_id = ?""",
                [epoch_id_value],
            ).fetchall()
            pins = {row[0]: row[1] for row in pin_rows}
            promotion = {row[0]: (row[2], self._aware(row[3]) if row[3] else None)
                         for row in pin_rows}
            resolved = [replace(item, state_revision=pins.get(item.binding_id)) for item in resolved]
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
                    (epoch_id, component_id, binding_ids_json, status, frontier, sealed_frontier)
                    VALUES (?, ?, ?, 'pending', 1, 0) ON CONFLICT DO NOTHING""", component_rows)

            owners = {ref: item.binding_id for item in resolved for ref in item.output_refs}
            component_members = {
                component_id: json.loads(members_json)
                for _, component_id, members_json in component_rows
            }
            component_for = {
                binding_id: component_id
                for component_id, members in component_members.items()
                for binding_id in members
            }
            # A newly constructed epoch owes its complete retained history, so
            # every raw input's full retained range counts as changed.
            raw_changes = {
                binding_id: self._retained_ranges(
                    conn, tuple(ref for ref in binding.input_refs if ref not in owners))
                for binding_id, binding in binding_by_id.items()
            }
            dirty = self._propagate_dirty(
                conn, binding_by_id, edges, component_for, raw_changes,
                self._component_raw_ranges(conn, component_members, binding_by_id, owners),
                promotion=promotion,
            )

            works: list[tuple] = []
            for component_id, members in component_members.items():
                works.extend(self._work_rows(
                    conn, epoch_id_value, component_id, members, binding_by_id,
                    edges, dirty, maximum_partition_duration, frontier=1, prefix="epoch-work",
                ))
            if works:
                conn.executemany("""INSERT INTO topology_epoch_work
                (work_id, epoch_id, component_id, binding_id, frontier,
                 write_start_ts, write_end_ts, read_start_ts, read_end_ts,
                 input_versions_json, upstream_frontier_json, binding_digest, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                ON CONFLICT (work_id) DO NOTHING""", works)
            new_status = "reconciling" if works else ("ready" if component_rows else "active")
            conn.execute("UPDATE topology_epochs SET status = ? WHERE epoch_id = ? AND status = 'constructing'", [new_status, epoch_id_value])
            conn.execute("""UPDATE topology_epochs SET status = 'superseded', superseded_by = ?
                WHERE epoch_id != ? AND status IN ('ready', 'reconciling', 'active')""",
                [epoch_id_value, epoch_id_value])
            conn.execute("""UPDATE topology_epoch_components SET status = 'superseded'
                WHERE epoch_id != ? AND status = 'pending' AND epoch_id IN
                (SELECT epoch_id FROM topology_epochs WHERE status = 'superseded')""", [epoch_id_value])
            conn.execute("UPDATE topology_epoch_work SET status = 'superseded' WHERE epoch_id != ? AND status IN ('pending', 'claimed')", [epoch_id_value])
            conn.execute("""UPDATE topology_epoch_control SET current_epoch_id = ?, candidate_epoch_id = NULL,
                updated_at = ? WHERE control_id = 1""", [epoch_id_value, self._now()])
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

    def _retained_ranges(self, conn, refs: Sequence[str], *, include_deleted: bool = False) -> tuple[TimeRange, ...]:
        if not refs:
            return ()
        live_filter = "" if include_deleted else " AND NOT t.deleted"
        rows = conn.execute(f"""SELECT min(t.ts), max(t.ts) FROM {TIMESERIES_TABLE} t
            JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
            WHERE r.ref_uri IN ({','.join('?' for _ in refs)}){live_filter}""", list(refs)).fetchone()
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

    def _definition_impact(self, conn, definition_id: str) -> ImpactPolicy:
        definition = self._definition(conn, definition_id)
        value = self._decode(definition.spec).get("impact") or {"kind": "pointwise"}
        return ImpactPolicy.from_json(value)

    @staticmethod
    def _read_interval(write: TimeRange, impact: ImpactPolicy) -> TimeRange:
        """Return the input halo required to compute one owned output range."""
        if impact.kind == "full_history":
            return write
        return TimeRange(write.start - impact.before, write.end + impact.after)

    @staticmethod
    def _affected_ranges(
        changed: Sequence[TimeRange],
        impact: ImpactPolicy,
        retained: Sequence[TimeRange],
        historical: Sequence[TimeRange] = (),
    ) -> tuple[TimeRange, ...]:
        if not changed:
            return ()
        if impact.kind == "full_history":
            # If the source became empty, the change ranges are still needed
            # to remove the previously materialized history.
            return coalesce_ranges([*retained, *historical] or list(changed))
        return coalesce_ranges(impact.affected(interval) for interval in changed)

    def _component_raw_ranges(self, conn, component_members: Mapping[str, Sequence[str]],
                              bindings: Mapping[str, EpochBinding], owners: Mapping[str, str],
                              *, include_deleted: bool = False) -> dict[str, tuple[TimeRange, ...]]:
        """Retained event-time bounds of each component's raw (unmanaged) inputs."""
        return {
            component: self._retained_ranges(conn, sorted({
                ref for binding_id in members for ref in bindings[binding_id].input_refs
                if ref not in owners
            }), include_deleted=include_deleted)
            for component, members in component_members.items()
        }

    def _propagate_dirty(
        self,
        conn,
        bindings: Mapping[str, EpochBinding],
        edges: Sequence[tuple[str, str]],
        component_for: Mapping[str, str],
        raw_changes: Mapping[str, Sequence[TimeRange]],
        component_retained: Mapping[str, Sequence[TimeRange]],
        component_history: Mapping[str, Sequence[TimeRange]] | None = None,
        *,
        promotion: Mapping[str, tuple[str | None, datetime | None]] | None = None,
    ) -> dict[str, tuple[TimeRange, ...]]:
        """Propagate changed input ranges through the DAG in topological order.

        A consumer applies its own impact policy to both raw changes and the
        output changes of its producers; this is what makes window semantics
        compose through a DAG.  Both epoch construction and incremental data
        planning derive their work from this one function.  ``promotion``
        clamps a binding's dirty ranges *before* they reach its consumers, so
        a prospective state revision suppresses downstream recomputation too.
        """
        incoming: dict[str, list[str]] = {binding_id: [] for binding_id in bindings}
        children: dict[str, list[str]] = {binding_id: [] for binding_id in bindings}
        indegree = {binding_id: 0 for binding_id in bindings}
        for source, target in edges:
            incoming[target].append(source)
            children[source].append(target)
            indegree[target] += 1
        ready = sorted(binding_id for binding_id, degree in indegree.items() if degree == 0)
        dirty: dict[str, tuple[TimeRange, ...]] = {}
        while ready:
            binding_id = ready.pop(0)
            changed = list(raw_changes.get(binding_id, ()))
            for source in incoming[binding_id]:
                changed.extend(dirty[source])
            affected = self._affected_ranges(
                changed,
                self._definition_impact(conn, bindings[binding_id].definition_id),
                component_retained[component_for[binding_id]],
                (component_history or {}).get(component_for[binding_id], ()),
            )
            policy, effective_from = (promotion or {}).get(binding_id, (None, None))
            if policy == "prospective":
                affected = ()
            elif policy == "recompute_from":
                if effective_from is None:
                    raise ValueError("recompute_from state revision lacks effective_from")
                affected = tuple(
                    TimeRange(max(interval.start, effective_from), interval.end)
                    for interval in affected if interval.end > effective_from
                )
            dirty[binding_id] = affected
            for child in sorted(children[binding_id]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        return dirty

    def _work_rows(
        self,
        conn,
        epoch: str,
        component: str,
        members: Sequence[str],
        bindings: Mapping[str, EpochBinding],
        edges: Sequence[tuple[str, str]],
        dirty: Mapping[str, Sequence[TimeRange]],
        maximum_partition_duration: timedelta,
        *,
        frontier: int,
        prefix: str,
    ) -> list[tuple]:
        """Build deterministic work rows and overlap-based DAG frontiers."""
        partitions = {
            binding_id: self._partition_ranges(dirty.get(binding_id, ()), maximum_partition_duration)
            for binding_id in members
        }
        identities: dict[str, list[tuple[TimeRange, TimeRange, str]]] = {}
        for binding_id in members:
            impact = self._definition_impact(conn, bindings[binding_id].definition_id)
            versions = self._stream_versions(conn, bindings[binding_id].input_refs)
            items: list[tuple[TimeRange, TimeRange, str]] = []
            for write in partitions[binding_id]:
                read = self._read_interval(write, impact)
                work_id = materialization_id(
                    prefix, epoch, binding_id, write.start.isoformat(),
                    write.end.isoformat(), versions,
                )
                items.append((write, read, work_id))
            identities[binding_id] = items

        rows: list[tuple] = []
        incoming = {
            target: tuple(source for source, candidate in edges if candidate == target)
            for target in members
        }
        for binding_id in sorted(members):
            binding = bindings[binding_id]
            versions = self._stream_versions(conn, binding.input_refs)
            for write, read, work_id in identities[binding_id]:
                dependency_frontier = {
                    source: [candidate_id for source_write, _, candidate_id in identities[source]
                             if source_write.intersects(read)]
                    for source in incoming[binding_id]
                }
                dependency_frontier = {source: ids for source, ids in dependency_frontier.items() if ids}
                rows.append((
                    work_id, epoch, component, binding_id, frontier,
                    self._stored_timestamp(write.start), self._stored_timestamp(write.end),
                    self._stored_timestamp(read.start), self._stored_timestamp(read.end),
                    self._json(versions), self._json(dependency_frontier), binding.content_digest,
                ))
        return rows

    def _seal_input_frontiers(
        self, conn, epoch: str, frontier: int, binding_ids: Sequence[str]
    ) -> None:
        """Advance durable input heads from the exact work being published."""
        for binding_id in binding_ids:
            versions: dict[str, int] = {}
            for (versions_json,) in conn.execute("""SELECT input_versions_json
                FROM topology_epoch_work WHERE epoch_id = ? AND binding_id = ? AND frontier = ?""",
                [epoch, binding_id, frontier]).fetchall():
                for ref, version in self._decode(versions_json).items():
                    versions[ref] = max(versions.get(ref, 0), int(version))
            if not versions:
                continue
            conn.execute("""INSERT INTO topology_binding_frontiers
                (epoch_id, binding_id, input_versions_json) VALUES (?, ?, ?)
                ON CONFLICT (epoch_id, binding_id) DO UPDATE SET
                input_versions_json = excluded.input_versions_json""",
                [epoch, binding_id, self._json(versions)])

    def _component_frontier_covers_current_inputs(
        self, conn, epoch: str, component: str, frontier: int,
        members: Sequence[str], bindings: Mapping[str, EpochBinding], owners: Mapping[str, str],
    ) -> bool:
        planned: dict[str, int] = {}
        for (versions_json,) in conn.execute("""SELECT input_versions_json FROM topology_epoch_work
            WHERE epoch_id = ? AND component_id = ? AND frontier = ?""",
            [epoch, component, frontier]).fetchall():
            for ref, version in self._decode(versions_json).items():
                planned[ref] = max(planned.get(ref, 0), int(version))
        raw_refs = sorted({ref for binding_id in members for ref in bindings[binding_id].input_refs
                           if ref not in owners})
        current = self._stream_versions(conn, raw_refs)
        return all(planned.get(ref, 0) >= version for ref, version in current.items())

    def plan_data_changes(self, *, maximum_partition_duration: timedelta = timedelta(minutes=15)) -> int:
        """Derive missing work from canonical changes and propagate it through the DAG."""
        with self._store._lock, self._store._write_conn() as conn:
            epoch = conn.execute("SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()[0]
            if epoch is None:
                return 0
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch]).fetchone()[0]
            if status in {"superseded", "failed", "compacted"}:
                return 0
            component_members = {
                component: self._decode(members)
                for component, members in conn.execute(
                    "SELECT component_id, binding_ids_json FROM topology_epoch_components WHERE epoch_id = ?",
                    [epoch],
                ).fetchall()
            }
            component_for = {binding_id: component for component, members in component_members.items() for binding_id in members}
            binding_rows = conn.execute("""SELECT epoch_id, binding_id, definition_id, logical_key,
                content_digest, inputs_json, outputs_json, metadata_json, state_revision
                FROM topology_epoch_bindings WHERE epoch_id = ? ORDER BY binding_id""", [epoch]).fetchall()
            bindings = {
                row[1]: EpochBinding(
                    row[0], row[1], row[2], row[3], row[4], self._decode(row[5]),
                    self._decode(row[6]), self._decode(row[7]), row[8],
                )
                for row in binding_rows
            }
            if not bindings:
                return 0
            owners = {ref: binding_id for binding_id, binding in bindings.items() for ref in binding.output_refs}
            edges = tuple(conn.execute(
                "SELECT source_binding_id, target_binding_id FROM topology_epoch_edges WHERE epoch_id = ?",
                [epoch],
            ).fetchall())

            raw_changes: dict[str, list[TimeRange]] = {binding_id: [] for binding_id in bindings}
            for binding_id, binding in bindings.items():
                raw_refs = tuple(ref for ref in binding.input_refs if ref not in owners)
                saved = conn.execute("""SELECT input_versions_json FROM topology_binding_frontiers
                    WHERE epoch_id = ? AND binding_id = ?""", [epoch, binding_id]).fetchone()
                versions = self._decode(saved[0]) if saved else {}
                prior: dict[str, int] = {ref: int(versions.get(ref, 0)) for ref in raw_refs}
                for ref in raw_refs:
                    head = self._stream_versions(conn, (ref,)).get(ref, 0)
                    if head <= prior[ref]:
                        continue
                    changes = conn.execute("""SELECT start_ts, end_ts FROM stream_change_ranges
                        WHERE ref_uri = ? AND stream_version > ? AND stream_version <= ? ORDER BY start_ts""", [ref, prior[ref], head]).fetchall()
                    for start, end in changes:
                        raw_changes[binding_id].append(TimeRange(self._aware(start), self._aware(end)))

            dirty = self._propagate_dirty(
                conn, bindings, edges, component_for, raw_changes,
                self._component_raw_ranges(conn, component_members, bindings, owners),
                self._component_raw_ranges(conn, component_members, bindings, owners, include_deleted=True),
            )

            inserted = 0
            for component, members in sorted(component_members.items()):
                if not any(dirty[binding_id] for binding_id in members):
                    continue
                self._lock_component(conn, epoch, component)
                component_state = conn.execute("""SELECT frontier, status FROM topology_epoch_components
                    WHERE epoch_id = ? AND component_id = ?""", [epoch, component]).fetchone()
                current_frontier = int(component_state[0])
                if component_state[1] != "sealed":
                    if self._component_frontier_covers_current_inputs(
                        conn, epoch, component, current_frontier, members, bindings, owners
                    ):
                        continue
                    # Coalesce a newer input head into a replacement frontier;
                    # old attempts remain fenced and cannot publish.
                    conn.execute("""UPDATE topology_epoch_work SET status = 'superseded'
                        WHERE epoch_id = ? AND component_id = ? AND frontier = ?
                        AND status != 'superseded'""", [epoch, component, current_frontier])
                    conn.execute("""DELETE FROM topology_epoch_outputs WHERE epoch_id = ? AND work_id IN
                        (SELECT work_id FROM topology_epoch_work
                         WHERE epoch_id = ? AND component_id = ? AND frontier = ?)""",
                        [epoch, epoch, component, current_frontier])
                frontier = current_frontier + 1
                rows = self._work_rows(
                    conn, epoch, component, members, bindings, edges, dirty,
                    maximum_partition_duration, frontier=frontier, prefix="epoch-data-work",
                )
                for row in rows:
                    changed = self._changed(conn, """INSERT INTO topology_epoch_work
                        (work_id, epoch_id, component_id, binding_id, frontier,
                         write_start_ts, write_end_ts, read_start_ts, read_end_ts,
                         input_versions_json, upstream_frontier_json, binding_digest, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                        ON CONFLICT (work_id) DO NOTHING""", row)
                    inserted += int(bool(changed))
                if rows:
                    conn.execute("""UPDATE topology_epoch_components
                        SET status = 'pending', frontier = ?, seal_publication_id = NULL
                        WHERE epoch_id = ? AND component_id = ?""", [frontier, epoch, component])
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
        return EpochClaim(claim_id, kind, target_id, owner, attempt, self._aware(expires))

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

    def renew_claim(self, claim: EpochClaim, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim:
        """Extend a held claim without changing its fencing attempt."""
        if duration <= timedelta():
            raise ValueError("claim duration must be positive")
        expires = self._now() + duration
        with self._store._lock, self._store._write_conn() as conn:
            self._require_claim(conn, claim)
            conn.execute("UPDATE topology_epoch_claims SET expires_at = ? WHERE claim_id = ?",
                         [expires, claim.claim_id])
        return EpochClaim(claim.claim_id, claim.kind, claim.target_id, claim.owner,
                          claim.attempt, self._aware(expires))

    # ----- execution against persisted epoch bindings -------------------

    def claim_next_work(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        now = self._now()
        with self._store._lock, self._store._write_conn() as conn:
            # Expired claims make claimed work retryable.  A claim is only a
            # liveness marker; the work row is the durable desired state.
            conn.execute("""UPDATE topology_epoch_work SET status = 'pending', next_attempt_at = ?
                WHERE status = 'claimed' AND work_id IN
                (SELECT work_id FROM topology_epoch_work w LEFT JOIN topology_epoch_claims c
                 ON c.target_id = w.work_id WHERE c.owner IS NULL OR c.expires_at <= ?)""", [now, now])
            rows = conn.execute("""SELECT w.work_id, w.upstream_frontier_json FROM topology_epoch_work w
                JOIN topology_epochs e ON e.epoch_id = w.epoch_id
                WHERE w.status = 'pending' AND e.status IN ('reconciling', 'ready')
                AND (w.next_attempt_at IS NULL OR w.next_attempt_at <= ?)
                AND e.epoch_id = (SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1)
                ORDER BY w.attempt, w.write_start_ts, w.work_id""", [now]).fetchall()
        for work_id, frontier_json in rows:
            frontier = self._decode(frontier_json)
            with self._store._own_conn() as conn:
                dependencies = (dependency for work_ids in frontier.values() for dependency in work_ids)
                if any(conn.execute("SELECT status FROM topology_epoch_work WHERE work_id = ?", [dependency]).fetchone() != ("committed",) for dependency in dependencies):
                    continue
            claim = self.claim("reconcile", work_id, owner, duration=duration)
            if claim is None:
                continue
            claimed = False
            with self._store._lock, self._store._write_conn() as conn:
                changed = self._changed(conn, """UPDATE topology_epoch_work
                    SET status = 'claimed', attempt = attempt + 1, next_attempt_at = NULL
                    WHERE work_id = ? AND status = 'pending'
                    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)""", [work_id, now])
                if changed:
                    claimed = True
                else:
                    conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
            if claimed:
                self._after_transition("work_claimed")
                return claim
        return None

    def _work(self, conn, work_id: str) -> EpochWork:
        row = conn.execute("""SELECT work_id, epoch_id, component_id, binding_id,
            write_start_ts, write_end_ts, read_start_ts, read_end_ts,
            input_versions_json, upstream_frontier_json, binding_digest, status, attempt
            FROM topology_epoch_work WHERE work_id = ?""", [work_id]).fetchone()
        if row is None:
            raise KeyError(work_id)
        return EpochWork(
            row[0], row[1], row[2], row[3],
            TimeRange(self._aware(row[4]), self._aware(row[5])),
            TimeRange(self._aware(row[6]), self._aware(row[7])),
            self._decode(row[8]),
            {source: tuple(ids) for source, ids in self._decode(row[9]).items()},
            row[10], row[11], row[12],
        )

    def _staged_dependency_rows(self, conn, epoch_id_value: str, ref: str,
                                dependency_ids: Sequence[str], interval: TimeRange) -> list[tuple]:
        """Read the newest committed staged value per timestamp for one input."""
        placeholders = ",".join("?" for _ in dependency_ids)
        return conn.execute(f"""SELECT ref_uri, ts, numeric_value, text_value FROM (
            SELECT o.ref_uri, o.ts, o.numeric_value, o.text_value,
                   row_number() OVER (PARTITION BY o.ref_uri, o.ts ORDER BY w.committed_at DESC, o.work_id DESC) AS recency
            FROM topology_epoch_outputs o JOIN topology_epoch_work w ON w.work_id = o.work_id
            WHERE o.epoch_id = ? AND o.ref_uri = ? AND o.work_id IN ({placeholders})
              AND w.status = 'committed' AND o.ts >= ? AND o.ts < ?) latest
            WHERE recency = 1 ORDER BY ts""", [
                epoch_id_value, ref, *dependency_ids,
                self._stored_timestamp(interval.start), self._stored_timestamp(interval.end),
            ]).fetchall()

    def _dependency_intervals(self, conn, dependency_ids: Sequence[str]) -> list[TimeRange]:
        placeholders = ",".join("?" for _ in dependency_ids)
        rows = conn.execute(f"""SELECT write_start_ts, write_end_ts
            FROM topology_epoch_work WHERE work_id IN ({placeholders})""", list(dependency_ids)).fetchall()
        return [TimeRange(self._aware(start), self._aware(end)) for start, end in rows]

    def _live_rows(self, conn, ref: str, interval: TimeRange) -> list[tuple]:
        """Read one canonical stream's live rows in a half-open interval."""
        return conn.execute(f"""SELECT r.ref_uri, t.ts, t.numeric_value, t.text_value
            FROM {TIMESERIES_TABLE} t JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
            WHERE r.ref_uri = ? AND t.ts >= ? AND t.ts < ? AND NOT t.deleted ORDER BY t.ts""",
            [ref, self._stored_timestamp(interval.start), self._stored_timestamp(interval.end)]).fetchall()

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
                                   self._decode(binding_row[5]), self._decode(binding_row[6]), self._decode(binding_row[7]), binding_row[8])
            definition = self._definition(conn, binding.definition_id)
            # Output ownership is recovered from the immutable binding rows so
            # no graph query or worker-side selector evaluation is possible.
            owners = {
                ref: binding_id
                for binding_id, outputs_json in conn.execute(
                    "SELECT binding_id, outputs_json FROM topology_epoch_bindings WHERE epoch_id = ?",
                    [work.epoch_id],
                ).fetchall()
                for refs in self._decode(outputs_json).values()
                for ref in refs
            }
            active_epoch = conn.execute(
                "SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1"
            ).fetchone()[0]
            rows: list[dict[str, Any]] = []
            for ref in binding.input_refs:
                if ref in owners:
                    dependency_ids = tuple(work.upstream_frontier.get(owners[ref], ()))
                    staged_rows = []
                    replaced: list[TimeRange] = []
                    if dependency_ids:
                        staged_rows = self._staged_dependency_rows(
                            conn, work.epoch_id, ref, dependency_ids, work.read_interval)
                        replaced = self._dependency_intervals(conn, dependency_ids)
                    baseline_rows = []
                    if active_epoch == work.epoch_id:
                        baseline_rows = [row for row in self._live_rows(conn, ref, work.read_interval)
                                         if not any(interval.start <= self._aware(row[1]) < interval.end
                                                    for interval in replaced)]
                    by_timestamp = {row[1]: row for row in baseline_rows}
                    by_timestamp.update({row[1]: row for row in staged_rows})
                    source_rows = [by_timestamp[ts] for ts in sorted(by_timestamp)]
                else:
                    source_rows = self._live_rows(conn, ref, work.read_interval)
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
        if any(row["ref_uri"] not in snapshot.binding.output_refs or not (snapshot.work.write_interval.start <= row["ts"] < snapshot.work.write_interval.end) for row in rows):
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
            frontier = self._decode(conn.execute("SELECT upstream_frontier_json FROM topology_epoch_work WHERE work_id = ?", [snapshot.work.work_id]).fetchone()[0])
            for dependency in (work_id for work_ids in frontier.values() for work_id in work_ids):
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

    def fail_work(self, claim: EpochClaim, error: Mapping[str, object], *,
                  retry_after: timedelta | None = None,
                  max_attempts: int = MAX_PARTITION_ATTEMPTS) -> None:
        """Back off retryable work and dead-letter deterministic failures."""
        if claim.kind != "reconcile":
            raise EpochClaimError("claim is not a reconcile claim")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if retry_after is None:
            retry_after = timedelta(seconds=min(300, 2 ** (claim.attempt - 1)))
        if retry_after < timedelta():
            raise ValueError("retry_after cannot be negative")
        status = "failed" if claim.attempt >= max_attempts else "pending"
        next_attempt = None if status == "failed" else self._now() + retry_after
        with self._store._lock, self._store._write_conn() as conn:
            self._require_claim(conn, claim)
            changed = self._changed(conn, """UPDATE topology_epoch_work
                SET status = ?, next_attempt_at = ?, error_json = ?
                WHERE work_id = ? AND status = 'claimed' AND attempt = ?""",
                                  [status, next_attempt, self._json(dict(error)),
                                   claim.target_id, claim.attempt])
            if not changed:
                raise EpochClaimError("work attempt is stale")
            conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
        self._after_transition("work_failed")

    # ----- atomic component sealing and activation ----------------------

    def claim_next_component(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT c.epoch_id, c.component_id, c.frontier FROM topology_epoch_components c
                JOIN topology_epochs e ON e.epoch_id = c.epoch_id
                WHERE c.status = 'pending' AND e.status IN ('reconciling', 'ready')
                  AND e.epoch_id = (SELECT current_epoch_id FROM topology_epoch_control WHERE control_id = 1)
                ORDER BY c.component_id""").fetchall()
            for epoch, component, frontier in rows:
                work_states = conn.execute("""SELECT status FROM topology_epoch_work
                    WHERE epoch_id = ? AND component_id = ? AND frontier = ?""",
                    [epoch, component, frontier]).fetchall()
                if any(row[0] != "committed" for row in work_states):
                    continue
                claim = self.claim("seal", f"{epoch}:{component}:{frontier}", owner, duration=duration)
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
        epoch, component, frontier_text = claim.target_id.rsplit(":", 2)
        frontier = int(frontier_text)
        with self._store._lock, self._store._write_conn() as conn:
            self._lock_component(conn, epoch, component)
            self._require_claim(conn, claim)
            control, active_epoch = conn.execute(
                "SELECT current_epoch_id, active_epoch_id FROM topology_epoch_control WHERE control_id = 1"
            ).fetchone()
            status = conn.execute("SELECT status FROM topology_epochs WHERE epoch_id = ?", [epoch]).fetchone()
            component_row = conn.execute("""SELECT binding_ids_json, status, frontier, sealed_frontier
                FROM topology_epoch_components WHERE epoch_id = ? AND component_id = ?""",
                [epoch, component]).fetchone()
            if status is None or component_row is None:
                raise KeyError(claim.target_id)
            if control != epoch or status[0] == "superseded":
                conn.execute("UPDATE topology_epoch_components SET status = 'superseded' WHERE epoch_id = ? AND component_id = ?", [epoch, component])
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                raise StaleEpochError("epoch was superseded before seal")
            if component_row[2] != frontier:
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                raise StaleEpochError("component frontier advanced before seal")
            if component_row[1] == "sealed" and component_row[3] == frontier:
                conn.execute("UPDATE topology_epoch_claims SET owner = NULL, expires_at = NULL WHERE claim_id = ?", [claim.claim_id])
                return conn.execute("SELECT seal_publication_id FROM topology_epoch_components WHERE epoch_id = ? AND component_id = ?", [epoch, component]).fetchone()[0]
            work_states = conn.execute("""SELECT status FROM topology_epoch_work
                WHERE epoch_id = ? AND component_id = ? AND frontier = ?""",
                [epoch, component, frontier]).fetchall()
            if any(row[0] != "committed" for row in work_states):
                raise ValueError("component has unfinished work")
            binding_ids = self._decode(component_row[0])
            promoted_policies = {
                row[0]: (row[1], row[2])
                for row in conn.execute("""SELECT binding_id, policy, effective_from
                    FROM topology_epoch_binding_pins
                    WHERE epoch_id = ? AND binding_id IN ({})""".format(
                        ",".join("?" for _ in binding_ids)
                    ), [epoch, *binding_ids]).fetchall()
            } if binding_ids else {}
            preserves_prior_rows = any(
                promotion[0] in {"prospective", "recompute_from"}
                for promotion in promoted_policies.values()
            )
            recompute_from = [
                self._aware(effective_from)
                for policy, effective_from in promoted_policies.values()
                if policy == "recompute_from" and effective_from is not None
            ]
            output_refs = sorted({ref for binding_id in binding_ids for row in conn.execute("SELECT outputs_json FROM topology_epoch_bindings WHERE epoch_id = ? AND binding_id = ?", [epoch, binding_id]).fetchone() for refs in self._decode(row).values() for ref in refs})
            if not binding_ids:
                output_refs.extend(row[0] for row in conn.execute("SELECT ref_uri FROM topology_epoch_retirements WHERE epoch_id = ?", [epoch]).fetchall()
                                   if row[0] not in output_refs)
            work_intervals = [
                (self._aware(start), self._aware(end))
                for start, end in conn.execute(
                    """SELECT write_start_ts, write_end_ts FROM topology_epoch_work
                    WHERE epoch_id = ? AND component_id = ? AND frontier = ?
                    ORDER BY write_start_ts, write_end_ts""",
                    [epoch, component, frontier],
                ).fetchall()
            ]
            if not work_intervals and output_refs and not preserves_prior_rows:
                retained = self._retained_ranges(conn, output_refs)
                if retained:
                    work_intervals = [(retained[0].start, retained[-1].end)]
            elif not work_intervals and output_refs and recompute_from:
                retained = self._retained_ranges(conn, output_refs)
                if retained:
                    effective_from = min(recompute_from)
                    start = max(retained[0].start, effective_from)
                    if retained[-1].end > start:
                        work_intervals = [(start, retained[-1].end)]
            mutations: list[dict[str, object]] = []
            if work_intervals:
                # A newly constructed epoch is a complete replacement for
                # its component, so rows outside the new retained frontier
                # must also be removed.  A data-frontier manifest on the
                # already-active epoch is incremental and only owns its
                # explicit work intervals.
                if active_epoch is not None and active_epoch != epoch and not preserves_prior_rows:
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
                    WHERE o.epoch_id = ? AND w.epoch_id = ? AND w.component_id = ?
                      AND w.frontier = ?) latest
                    WHERE recency = 1 ORDER BY ref_uri, ts""",
                    [epoch, epoch, component, frontier]).fetchall()
                staged_keys = {(ref, ts) for ref, ts, _, _ in staged}
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": None, "text_value": None}
                                 for ref, ts in sorted(existing - staged_keys))
                mutations.extend({"operation": "upsert", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": numeric, "text_value": text}
                                 for ref, ts, numeric, text in staged)
            seal_digest = sha256(self._json(mutations).encode()).hexdigest() if mutations else "empty"
            publication_id = f"topology-epoch:{epoch}:component:{component}:frontier:{frontier}:seal:{seal_digest}"
            if mutations:
                receipt = self._apply_canonical_publication(conn, publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
                publication_id = receipt.publication_id
            else:
                publication_id = None
            self._seal_input_frontiers(conn, epoch, frontier, binding_ids)
            conn.execute("""UPDATE topology_epoch_components
                SET status = 'sealed', sealed_frontier = ?, seal_publication_id = ?
                WHERE epoch_id = ? AND component_id = ?""",
                [frontier, publication_id, epoch, component])
            # The canonical publication is now the durable baseline. Work and
            # staged rows are execution scratch, not retained history.
            conn.execute("""DELETE FROM topology_epoch_outputs WHERE epoch_id = ? AND work_id IN
                (SELECT work_id FROM topology_epoch_work
                 WHERE epoch_id = ? AND component_id = ? AND frontier <= ?)""",
                [epoch, epoch, component, frontier])
            conn.execute("""DELETE FROM topology_epoch_work
                WHERE epoch_id = ? AND component_id = ? AND frontier <= ?""",
                [epoch, component, frontier])
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

    def active_epoch_id(self) -> str | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1").fetchone()
        return row[0] if row else None

    def status(self, *, failed_limit: int = 50) -> dict[str, object]:
        """Return a compact operational view of the durable state machine."""
        if failed_limit < 0:
            raise ValueError("failed_limit cannot be negative")
        with self._store._own_conn() as conn:
            candidate, current, active = conn.execute("""SELECT candidate_epoch_id,
                current_epoch_id, active_epoch_id FROM topology_epoch_control WHERE control_id = 1""").fetchone()
            deployments = [
                {"name": name, "definition_id": definition_id, "generation": int(generation)}
                for name, definition_id, generation in conn.execute("""SELECT name, definition_id, generation
                    FROM topology_deployments ORDER BY name""").fetchall()
            ]
            work = {
                status: int(count)
                for status, count in conn.execute("""SELECT status, count(*) FROM topology_epoch_work
                    WHERE epoch_id = ? GROUP BY status ORDER BY status""", [current]).fetchall()
            } if current else {}
            components = [
                {"component_id": component, "status": status, "frontier": int(frontier),
                 "sealed_frontier": int(sealed)}
                for component, status, frontier, sealed in conn.execute("""SELECT component_id, status,
                    frontier, sealed_frontier FROM topology_epoch_components
                    WHERE epoch_id = ? ORDER BY component_id""", [current]).fetchall()
            ] if current else []
            failed = [
                {"work_id": work_id, "binding_id": binding_id, "attempt": int(attempt),
                 "error": self._decode(error) if error else None}
                for work_id, binding_id, attempt, error in conn.execute("""SELECT work_id, binding_id,
                    attempt, error_json FROM topology_epoch_work WHERE epoch_id = ? AND status = 'failed'
                    ORDER BY work_id LIMIT ?""", [current, failed_limit]).fetchall()
            ] if current and failed_limit else []
        return {
            "candidate_epoch_id": candidate,
            "current_epoch_id": current,
            "active_epoch_id": active,
            "deployments": deployments,
            "work": work,
            "components": components,
            "failed_work": failed,
        }

    # ----- retention/compaction -----------------------------------------

    def compact(self) -> int:
        """Discard superseded topology state not named by a live pointer."""
        with self._store._lock, self._store._write_conn() as conn:
            candidate, current, active = conn.execute("""SELECT candidate_epoch_id,
                current_epoch_id, active_epoch_id FROM topology_epoch_control WHERE control_id = 1""").fetchone()
            live = [epoch for epoch in (candidate, current, active) if epoch is not None]
            if live:
                rows = conn.execute(f"""SELECT epoch_id FROM topology_epochs
                    WHERE status = 'superseded' AND compacted_at IS NULL
                    AND epoch_id NOT IN ({','.join('?' for _ in live)})""", live).fetchall()
            else:
                rows = conn.execute("""SELECT epoch_id FROM topology_epochs
                    WHERE status = 'superseded' AND compacted_at IS NULL""").fetchall()
            for (epoch,) in rows:
                conn.execute("DELETE FROM topology_epoch_outputs WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_work WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_retirements WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_binding_frontiers WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_edges WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_components WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_bindings WHERE epoch_id = ?", [epoch])
                conn.execute("DELETE FROM topology_epoch_binding_pins WHERE epoch_id = ?", [epoch])
                conn.execute("UPDATE topology_epochs SET status = 'compacted', compacted_at = ? WHERE epoch_id = ?", [self._now(), epoch])
        if rows:
            self._after_transition("epochs_compacted")
        return len(rows)
