"""Bridge loaded calculation batches and durable timeseries state.

Inside a step, every read and write here runs on the step's own write
connection: the batch is read from the transaction that is about to commit
it, so a layer sees the raw rows applied by the same step and the outputs of
the layers before it. Standalone, with no connection supplied, a batch is
prepared in a consistent read transaction, user code runs outside it, and
publication opens its own write transaction that validates the prepared work
is still current before replacing outputs and advancing progress together.
Transform failures therefore leave progress available for retry, and stale
workers cannot overwrite a newer accepted result.

Revisions identify writes, not event timestamps. The database retains current
values and incremental output tombstones, not historical row versions. Whole-stream
replacement physically deletes rows and records one latest reset per stream.
A revision range selects what needs recalculation; the input window supplies current values from
the read snapshot, including context outside that range.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, ContextManager, Iterable, Mapping, Protocol
from uuid import uuid4
import pyarrow as pa
import pyarrow.compute as pc
from acquirium.Materialization.models import (
    UTC, Batch, Binding, InputBatch, StreamDescriptor, StreamSet, TimeWindow,
)


class RevisionBackend(Protocol):
    """Required backend boundary. All mutations share _lock; _write_conn is
    transactional. _connect returns an independent connection whose explicit
    read transaction can pin a snapshot across multiple queries.
    """
    _lock: Any
    def _connect(self) -> Any: ...
    def _own_conn(self) -> ContextManager[Any]: ...
    def _write_conn(self) -> ContextManager[Any]: ...
    def _next_revision(self, conn: Any) -> int: ...
    def _insert_frame(self, conn: Any, frame: Any, revision: int) -> None: ...

class RevisionStore:
    """Durable revision-frontier persistence shared by supported stores."""
    def __init__(self, store: RevisionBackend, unit_converter: Any = None):
        self.store = store
        self.unit_converter = unit_converter
        # A runtime installs progress_key -> generation as the publication
        # authority and updates it under the backend write lock. None permits
        # standalone scheduler use; an empty mapping permits no publications.
        # Do not conflate those two cases in the validation below.
        self.active_bindings: dict[str, str] | None = None
        # Set to a list by a step's evaluator to collect what the step
        # writes per derived stream; None collects nothing.
        self.capture: list | None = None
        with store._lock, store._write_conn() as conn:
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_work (
                progress_key VARCHAR PRIMARY KEY, work_id VARCHAR NOT NULL,
                cursor_ts VARCHAR NOT NULL, end_ts VARCHAR NOT NULL,
                from_revision BIGINT NOT NULL, to_revision BIGINT NOT NULL)""")

    @property
    def _postgres(self) -> bool:
        return getattr(self.store, "materialization_backend", None) == "postgres"

    def _sql(self, query: str) -> str:
        """Translate the only parameter syntax the shared runtime needs."""
        return query.replace("?", "%s") if self._postgres else query

    def _time(self, value: datetime) -> datetime:
        value = value.astimezone(UTC)
        return value if self._postgres else value.replace(tzinfo=None)

    @property
    def _timeseries_source(self) -> str:
        return "timeseries t JOIN ref_ids r ON r.ref_id=t.ref_id"

    @property
    def _ref(self) -> str:
        return "r.ref_uri"

    def _execute(self, conn: Any, query: str, params: Iterable[Any] = ()) -> Any:
        return conn.execute(self._sql(query), list(params))
    def current_revision(self) -> int:
        with self.store._own_conn() as conn: return int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
    def progress_snapshot(self, conn: Any = None) -> tuple[int, dict[str, int]]:
        if conn is not None:
            return self._progress_on(conn)
        with self.store._own_conn() as conn:
            return self._progress_on(conn)

    def _progress_on(self, conn: Any) -> tuple[int, dict[str, int]]:
        current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
        progress = dict(self._execute(conn, "SELECT progress_key, consumed_revision FROM binding_progress").fetchall())
        return current, progress

    def initialise(self, binding: Binding, backfill: bool = False, conn: Any = None) -> int:
        if conn is not None:
            return self._initialise_on(conn, binding, backfill)
        with self.store._own_conn() as reader:
            row = self._execute(reader, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is not None:
                return int(row[0])
        with self.store._lock, self.store._write_conn() as writer:
            return self._initialise_on(writer, binding, backfill)

    def _initialise_on(self, conn: Any, binding: Binding, backfill: bool) -> int:
        row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
        if row is not None:
            return int(row[0])
        if self.active_bindings is not None and self.active_bindings.get(binding.progress_key) != binding.generation:
            raise ValueError("binding is no longer active")
        current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
        # Backfill deliberately replays retained history for a newly seen
        # binding; otherwise only future changes are processed.
        consumed = 0 if backfill else current
        self._execute(conn, "INSERT INTO binding_progress VALUES (?, ?)", [binding.progress_key, consumed])
        return consumed

    def pending_keys(self, conn: Any = None) -> set[str]:
        if conn is not None:
            return {row[0] for row in self._execute(conn, "SELECT progress_key FROM materialization_work").fetchall()}
        with self.store._own_conn() as conn:
            return self.pending_keys(conn)

    def change_index(self, floor: int, conn: Any = None) -> tuple[int, dict[str, int], dict[str, int]]:
        """Which streams changed above ``floor``, from one snapshot.

        Returns the snapshot's current revision, each written stream's latest
        revision (tombstones included, since a removal is a change), and each
        reset stream's reset revision. One indexed scan answers readiness for
        every binding in a layer; ``next_batch`` then reads only for the
        bindings whose inputs appear here. On a step's connection the index
        includes what the step has already written.
        """
        if conn is not None:
            return self._change_index_on(conn, floor)
        conn = self.store._connect()
        try:
            if self._postgres:
                conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                conn.begin()
            result = self._change_index_on(conn, floor)
            conn.commit()
            return result
        except BaseException:
            conn.rollback(); raise
        finally:
            conn.close()

    def _change_index_on(self, conn: Any, floor: int) -> tuple[int, dict[str, int], dict[str, int]]:
        current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
        changed = {row[0]: int(row[1]) for row in self._execute(
            conn, f"SELECT {self._ref}, max(t.last_revision) FROM {self._timeseries_source} "
                  f"WHERE t.last_revision>? GROUP BY {self._ref}", [floor]).fetchall()}
        resets = {row[0]: int(row[1]) for row in self._execute(
            conn, "SELECT ref_uri, last_revision FROM stream_resets WHERE last_revision>?", [floor]).fetchall()}
        return current, changed, resets

    def advance_idle(self, items: Iterable[tuple[Binding, int, int]], conn: Any = None) -> None:
        """Move frontiers past revisions that touched none of a binding's inputs.

        ``items`` are ``(binding, previous, target)`` where ``target`` is the
        current revision of the snapshot that showed no change for the
        binding. One compare-and-swap per binding, so a batch that is already
        running cannot be overtaken.
        """
        items = tuple(items)
        if not items:
            return
        if conn is not None:
            self._advance_idle_on(conn, items)
            return
        with self.store._lock, self.store._write_conn() as writer:
            self._advance_idle_on(writer, items)

    def _advance_idle_on(self, conn: Any, items: tuple[tuple[Binding, int, int], ...]) -> None:
        for binding, previous, target in items:
            if self.active_bindings is not None and self.active_bindings.get(binding.progress_key) != binding.generation:
                continue
            self._execute(conn, "UPDATE binding_progress SET consumed_revision=? WHERE progress_key=? AND consumed_revision=?",
                          [target, binding.progress_key, previous])

    def retained_window(self, binding: Binding) -> TimeWindow | None:
        # Include outputs: after input membership shrinks, obsolete results may
        # extend beyond the remaining inputs and still need to be removed.
        refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values}
                      | {p.ref_uri for p in binding.outputs.values()})
        with self.store._own_conn() as conn:
            marks = ','.join('?' for _ in refs)
            extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks})", refs).fetchone()
        return binding.clip(TimeWindow(*(self._utc(t) for t in extent))) if extent[0] is not None else None

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    def retire(self, output_refs: Iterable[str], since: datetime, conn: Any = None) -> None:
        """Tombstone the derived streams ``output_refs`` from ``since`` on, at one revision.

        The model says their bindings stopped existing at ``since``; rows
        before it stay as the record of what the model said then. The
        tombstones carry a new revision so downstream bindings see the
        removal and retract their own results.
        """
        refs = sorted(set(output_refs))
        if not refs:
            return
        if conn is None:
            with self.store._lock, self.store._write_conn() as own:
                return self.retire(refs, since, conn=own)
        revision = None
        for ref in refs:
            exists = self._execute(conn, "SELECT 1 FROM timeseries WHERE ref_id IN (SELECT ref_id FROM ref_ids WHERE ref_uri=?) "
                                         "AND ts>=? AND NOT deleted LIMIT 1", [ref, self._time(since)]).fetchone()
            if exists is None:
                continue
            if revision is None:
                revision = self.store._next_revision(conn)
            self._execute(conn, "UPDATE timeseries SET deleted=TRUE, last_revision=? WHERE ref_id IN "
                                "(SELECT ref_id FROM ref_ids WHERE ref_uri=?) AND ts>=? AND NOT deleted",
                          [revision, ref, self._time(since)])

    def request_reprocess(self, bindings: Iterable[Binding], window: TimeWindow) -> None:
        with self.store._lock, self.store._write_conn() as conn:
            for binding in bindings:
                owned = binding.clip(window)
                if owned is None:
                    continue
                if binding.every:
                    epoch = datetime(1970, 1, 1, tzinfo=UTC)
                    floor = lambda t: epoch + ((t - epoch) // binding.every) * binding.every
                    owned = TimeWindow(floor(window.start), floor(window.end) + binding.every - timedelta(microseconds=1))
                progress = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
                if progress is None:
                    raise ValueError("binding must be initialized before reprocessing")
                if self._execute(conn, "SELECT 1 FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone():
                    raise ValueError("binding already has pending work; wait for it to finish")
                # Equal source and target revisions distinguish explicit repair
                # from catch-up: finishing it must preserve the input frontier.
                # Reject overlapping requests instead of losing a work cursor.
                self._execute(conn, "INSERT INTO materialization_work VALUES (?, ?, ?, ?, ?, ?)",
                              [binding.progress_key, uuid4().hex, owned.start.isoformat(), owned.end.isoformat(), progress[0], progress[0]])

    def _work_batch(self, conn: Any, binding: Binding, work) -> Batch:
        from dataclasses import replace
        work_id, cursor, end, previous, target = work
        start, finish = datetime.fromisoformat(cursor), datetime.fromisoformat(end)
        # Inclusive microsecond bounds let adjacent chunks meet without a gap
        # or overlap. Round the end out to a whole bucket before moving the
        # cursor; a bucket must never be computed from two partial input loads.
        stop = min(finish, start + timedelta(days=1) - timedelta(microseconds=1))
        if binding.every:
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            stop = min(finish, epoch + (((stop - epoch) // binding.every) + 1) * binding.every - timedelta(microseconds=1))
        window = TimeWindow(start, stop)
        batch = self._window_batch(conn, binding, previous, target, window, output_window=window)
        following = (stop + timedelta(microseconds=1)).isoformat() if stop < finish else None
        return replace(batch, context=replace(batch.context, work_id=work_id, work_cursor=cursor, work_next=following))

    def next_batch(self, binding: Binding, conn: Any = None) -> Batch | None:
        """Read one binding's next batch.

        On a step's connection the batch comes from the step's own
        transaction and a skipped revision range advances the frontier there.
        Standalone, the frontier, changed extent, and every input alias come
        from one read snapshot; per-query snapshots could combine a new
        revision with old data, then advance progress past values the
        transform never saw. All Arrow data is loaded before returning, so no
        read transaction is held open while application code executes.
        """
        if conn is not None:
            batch, skipped = self._next_batch_on(conn, binding, writer=conn)
            if skipped is not None:
                self._advance_idle_on(conn, (skipped,))
            return batch
        conn = self.store._connect()
        try:
            # DuckDB exposes ``begin()`` while psycopg starts an explicit
            # snapshot transaction through SQL.
            if self._postgres:
                conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                conn.begin()
            batch, skipped = self._next_batch_on(conn, binding, writer=None)
            conn.commit()
        except BaseException:
            conn.rollback(); raise
        finally: conn.close()
        if skipped is not None:
            # Revisions for unrelated streams can be safely skipped. The
            # compare makes this race-safe with an in-flight invocation.
            with self.store._lock, self.store._write_conn() as writer:
                self._advance_idle_on(writer, (skipped,))
        return batch

    def _next_batch_on(self, conn: Any, binding: Binding, *, writer: Any) -> tuple[Batch | None, tuple | None]:
        row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
        if row is None: raise KeyError("binding was not initialised")
        previous = int(row[0]); target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
        work = self._execute(conn, "SELECT work_id, cursor_ts, end_ts, from_revision, to_revision FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone()
        if self._has_reset(conn, binding, previous, target):
            batch = self._reset_batch(conn, binding, previous, target, work)
        else:
            batch = self._work_batch(conn, binding, work) if work else self._build_batch(conn, binding, previous, target, partition=True, writer=writer)
        skipped = (binding, previous, target) if batch is None and previous != target else None
        return batch, skipped

    def _has_reset(self, conn: Any, binding: Binding, previous: int, target: int) -> bool:
        refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values})
        if not refs or previous >= target:
            return False
        marks = ','.join('?' for _ in refs)
        return self._execute(conn, f"SELECT 1 FROM stream_resets WHERE ref_uri IN ({marks}) AND last_revision>? AND last_revision<=? LIMIT 1",
                             [*refs, previous, target]).fetchone() is not None

    def _reset_batch(self, conn: Any, binding: Binding, previous: int, target: int, work) -> Batch:
        """Rebuild from all current inputs; old outputs also bound the repair.

        Resets are deliberately unpartitioned. Output replacement and progress
        commit together, so failure/restart retries the full rebuild and an old
        backfill cursor can only be discarded after the rebuild succeeds.
        """
        from dataclasses import replace
        refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values}
                      | {p.ref_uri for p in binding.outputs.values()})
        marks = ','.join('?' for _ in refs)
        extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks})", refs).fetchone()
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        retained = TimeWindow(*(self._utc(t) for t in extent)) if extent[0] is not None else TimeWindow(epoch, epoch)
        if binding.bounded:
            edge = binding.valid_from or binding.valid_to - timedelta(microseconds=1)
            retained = binding.clip(retained) or TimeWindow(edge, edge)
        expanded = self._output_window(binding, retained) or retained
        window = TimeWindow(min(retained.start, expanded.start), max(retained.end, expanded.end))
        # All retained rows are changes for a full rebuild, including unchanged
        # sibling inputs in a multi-input app.
        inputs = {alias: replace(self._stream_set(conn, alias, descriptors, window, 0, target), every=binding.every, _scheduled=True)
                  for alias, descriptors in binding.inputs.items()}
        return Batch(inputs, InputBatch(binding.signature, binding.graph_revision, previous,
                     target, retained, window, binding.row, binding.result, output_window=window,
                     work_id=work[0] if work else None, work_cursor=work[1] if work else None,
                     full_reset=True))

    def preview_batch(self, binding: Binding) -> Batch | None:
        """Build a batch over all stored input data, touching no durable state.

        This is the read half of an invocation without the write half: no
        progress row is created or advanced, so a dry run neither disturbs a
        deployed app nor leaves anything behind.
        """
        conn = self.store._connect()
        try:
            if self._postgres: conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else: conn.begin()
            target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            batch = self._build_batch(conn, binding, 0, target)
            conn.commit()
            return batch
        except BaseException:
            conn.rollback(); raise
        finally: conn.close()

    def _build_batch(self, conn: Any, binding: Binding, previous: int, target: int, *, partition: bool = False, writer: Any = None) -> Batch | None:
        """Read one coherent batch for the revisions in ``(previous, target]``."""
        refs = [d.ref_uri for values in binding.inputs.values() for d in values]
        if not refs or target == previous: return None
        marks = ",".join("?" for _ in refs)
        # Include tombstones in the changed extent. Filtering deleted rows here
        # would hide removals from consumers, even though _stream_set correctly
        # excludes those rows from the values delivered to the transform.
        # Changes before the binding's validity are not its concern.
        since = ("" if binding.valid_from is None else " AND t.ts>=?") + ("" if binding.valid_to is None else " AND t.ts<?")
        params = ([*refs, previous, target] + ([] if binding.valid_from is None else [self._time(binding.valid_from)])
                  + ([] if binding.valid_to is None else [self._time(binding.valid_to)]))
        changed = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.last_revision>? AND t.last_revision<=?{since}", params).fetchone()
        if changed[0] is None: return None
        changed_window = TimeWindow(self._utc(changed[0]), self._utc(changed[1]))
        # Persist a cursor before executing a long range so a restart can resume
        # its unfinished intervals. Whole-history apps cannot be partitioned this
        # way: each invocation explicitly depends on the full retained extent.
        if partition and binding.lookback is not None and changed_window.end - changed_window.start > timedelta(days=1):
            window = self._output_window(binding, changed_window)
            if window is None: return None
            work = (uuid4().hex, window.start.isoformat(), window.end.isoformat(), previous, target)
            if self.active_bindings is not None and self.active_bindings.get(binding.progress_key) != binding.generation:
                return None
            if writer is not None:
                self._execute(writer, "INSERT INTO materialization_work VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (progress_key) DO NOTHING", [binding.progress_key, *work])
            else:
                with self.store._lock, self.store._write_conn() as own:
                    self._execute(own, "INSERT INTO materialization_work VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (progress_key) DO NOTHING", [binding.progress_key, *work])
            return self._work_batch(conn, binding, work)
        return self._window_batch(conn, binding, previous, target, changed_window)

    def _window_batch(self, conn: Any, binding: Binding, previous: int, target: int,
                      changed_window: TimeWindow, *, output_window: TimeWindow | None = None) -> Batch:
        # There are two expansions, serving different purposes. A corrected
        # reading at noon in a ten-minute rolling calculation affects outputs
        # through 12:10. Recomputing those outputs needs inputs starting at 11:50.
        # _output_window finds the affected outputs; this method adds their
        # input context. Publication must clip back to the output interval.
        if binding.lookback is None:
            refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values}
                          | {p.ref_uri for p in binding.outputs.values()})
            marks = ",".join("?" for _ in refs)
            extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks})", refs).fetchone()
            window = TimeWindow(self._utc(extent[0]), self._utc(extent[1])) if extent[0] is not None else changed_window
            window = binding.clip(window) or changed_window
            read = window
            if binding.custom_window is not None:
                window = self._output_window(binding, changed_window) or window
        else:
            window = output_window or self._output_window(binding, changed_window) or changed_window
            read = TimeWindow(window.start - binding.lookback, window.end + binding.lookahead)
        if output_window is not None and binding.lookback is None:
            window = output_window
        from dataclasses import replace
        inputs = {alias: replace(self._stream_set(conn, alias, descriptors, read, previous, target), every=binding.every, _scheduled=True)
                  for alias, descriptors in binding.inputs.items()}
        return Batch(inputs, InputBatch(binding.signature, binding.graph_revision, previous,
                                        target, changed_window, read, binding.row, binding.result, output_window=window))

    @staticmethod
    def _output_window(binding: Binding, changed: TimeWindow) -> TimeWindow | None:
        """The outputs a change can affect, within the binding's validity."""
        if binding.custom_window is not None:
            window = binding.custom_window(changed)
            if not isinstance(window, TimeWindow):
                raise TypeError("output_window must return a TimeWindow")
            return binding.clip(window)
        window = TimeWindow(changed.start - binding.lookahead,
                            changed.end + (binding.lookback or timedelta()))
        if binding.every is not None:
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            floor = lambda t: epoch + ((t - epoch) // binding.every) * binding.every
            window = TimeWindow(floor(window.start), floor(window.end) + binding.every - timedelta(microseconds=1))
        return binding.clip(window)

    def _stream_set(self, conn: Any, alias: str, descriptors: tuple[StreamDescriptor,...], window: TimeWindow, previous: int, target: int) -> StreamSet:
        refs = [x.ref_uri for x in descriptors]
        if not refs: return StreamSet(alias, window, descriptors, converter=self.unit_converter)
        marks = ",".join("?" for _ in refs)
        # Read current context, not only rows written in (previous, target].
        # Durable work retains its original target across chunks, but later
        # chunks read fresh snapshots; newer corrections are processed again
        # after the range completes. This is not a historical as-of read.
        query = f"""SELECT {self._ref},t.ts,t.numeric_value,t.text_value,t.last_revision FROM {self._timeseries_source}
                    WHERE {self._ref} IN ({marks}) AND NOT t.deleted AND t.ts>=? AND t.ts<=? ORDER BY {self._ref},t.ts"""
        cursor = self._execute(conn, query, [*refs, self._time(window.start), self._time(window.end)])
        if self._postgres:
            rows = cursor.fetchall()
            raw = pa.table({
                "ref": pa.array([r[0] for r in rows], pa.string()),
                "time": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
                "numeric": pa.array([r[2] for r in rows], pa.float64()),
                "text": pa.array([r[3] for r in rows], pa.string()),
                "revision": pa.array([r[4] for r in rows], pa.int64()),
            })
        else:
            raw = cursor.to_arrow_table().rename_columns(["ref", "time", "numeric", "text", "revision"])
        numeric = raw["text"].null_count == raw.num_rows
        if not raw.num_rows:
            # Physical reset may leave no values from which to infer a type.
            # Preserve declared text inputs even after their last row is gone.
            numeric = all(d.value_kind != "text" for d in descriptors) and self._execute(
                conn, f"SELECT 1 FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.text_value IS NOT NULL LIMIT 1", refs
            ).fetchone() is None
        values = raw["numeric"] if numeric else pc.coalesce(raw["text"], pc.cast(raw["numeric"], pa.string()))
        table = pa.table({"ref_uri": raw["ref"],
                          "time": pc.cast(raw["time"], pa.timestamp("us", tz="UTC")),
                          "value": values})
        changed = table.filter(pc.and_(pc.greater(raw["revision"], previous), pc.less_equal(raw["revision"], target)))
        return StreamSet(alias, window, descriptors, table, changed, converter=self.unit_converter)
    def commit(self, binding: Binding, batch: Batch, results: Mapping[str, pa.Table], conn: Any = None) -> bool:
        return self.commit_wave(((binding, batch, results),), conn=conn).get(binding.signature, False)

    def commit_wave(self, commits: Iterable[tuple[Binding, Batch, Mapping[str, pa.Table]]], conn: Any = None) -> Mapping[str, bool]:
        """Publish still-current results from one layer chunk.

        The return mapping contains accepted signatures, including invocations
        that advanced progress without emitting rows. Rejected work is omitted.
        On a step's connection the writes join the step's transaction;
        standalone they share one transaction of their own. A write failure
        rolls them back together. The backend's _insert_frame must use the
        supplied connection and revision, never commit independently.
        """
        completed = tuple(commits)
        if not completed:
            return {}
        if conn is not None:
            return self._commit_wave_on(conn, completed)
        with self.store._lock, self.store._write_conn() as own:
            return self._commit_wave_on(own, completed)

    def _commit_wave_on(self, conn: Any, completed: tuple) -> Mapping[str, bool]:
        # Check authorization and progress under the same write lock used
        # by deployment revocation. Checking outside it would leave a gap
        # in which an obsolete result could become authorized to overwrite
        # the replacement deployment's data.
        accepted = []
        current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
        for binding, batch, results in completed:
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            active = self.active_bindings
            valid = active is None or active.get(binding.progress_key) == binding.generation
            # A physical reset invalidates any snapshot prepared before it,
            # including a pending backfill chunk. Retry against the reset.
            valid = valid and not self._has_reset(conn, binding, batch.context.to_revision, current)
            # Intermediate chunks share the same frontier. The work ID and
            # cursor provide the additional compare-and-swap condition that
            # prevents a duplicate chunk or cancelled repair from publishing.
            if batch.context.work_id:
                work = self._execute(conn, "SELECT work_id, cursor_ts FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone()
                valid = valid and work == (batch.context.work_id, batch.context.work_cursor)
            if valid and row is not None and int(row[0]) == batch.context.from_revision:
                accepted.append((binding, batch, results))
        # Incremental writes replace an owned interval using tombstones.
        # Full rebuilds physically replace assigned streams and propagate
        # reset markers, including when both old and new streams are empty.
        import polars as pl
        # Allocate a revision for nonempty output or removal of existing
        # rows, or a propagated reset. An ordinary empty interval with
        # nothing to remove needs only a progress update.
        revision = None
        for binding, batch, results in accepted:
            window = batch.context.output_window
            for name, table in results.items():
                port = binding.outputs[name]
                mask = pc.and_(pc.greater_equal(table["time"], pa.scalar(window.start)),
                               pc.less_equal(table["time"], pa.scalar(window.end)))
                table = table.filter(mask)
                ref_filter = "ref_id IN (SELECT ref_id FROM ref_ids WHERE ref_uri=?)"
                existing = self._execute(conn, f"SELECT 1 FROM timeseries WHERE {ref_filter} AND ts>=? AND ts<=? AND NOT deleted LIMIT 1",
                                         [port.ref_uri, self._time(window.start), self._time(window.end)]).fetchone()
                if not batch.context.full_reset and not table.num_rows and existing is None:
                    continue
                if revision is None:
                    revision = self.store._next_revision(conn)
                if batch.context.full_reset and binding.bounded:
                    # A bounded binding shares its output with the owners of
                    # other intervals: rebuild only its own.
                    self._execute(conn, f"DELETE FROM timeseries WHERE {ref_filter} AND ts>=? AND ts<=?",
                                  [port.ref_uri, self._time(window.start), self._time(window.end)])
                    self._execute(conn, "INSERT INTO stream_resets (ref_uri, last_revision) VALUES (?, ?) ON CONFLICT (ref_uri) DO UPDATE SET last_revision=EXCLUDED.last_revision",
                                  [port.ref_uri, revision])
                elif batch.context.full_reset:
                    self._execute(conn, f"DELETE FROM timeseries WHERE {ref_filter}", [port.ref_uri])
                    self._execute(conn, "INSERT INTO stream_resets (ref_uri, last_revision) VALUES (?, ?) ON CONFLICT (ref_uri) DO UPDATE SET last_revision=EXCLUDED.last_revision",
                                  [port.ref_uri, revision])
                else:
                    self._execute(conn, f"UPDATE timeseries SET deleted=TRUE, last_revision=? WHERE {ref_filter} AND ts>=? AND ts<=? AND NOT deleted",
                                  [revision, port.ref_uri, self._time(window.start), self._time(window.end)])
                self._execute(conn, """INSERT INTO streams (ref_uri, point_uri, source_id, ref_name, value_kind)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (ref_uri) DO NOTHING""",
                    [port.ref_uri, port.point_uri, f"derived:{binding.application_name}", port.ref_name, port.spec.value_kind])
                if self.capture is not None:
                    from acquirium.Materialization.changes import StreamChange
                    self.capture.append(StreamChange(port.ref_uri, binding.application_name, name,
                                                     (window.start.isoformat(), window.end.isoformat()),
                                                     table.select(["time", "value"]), port.spec.trigger))
                if table.num_rows:
                    frame = pl.from_arrow(table).rename({"time": "ts"})
                    if not self._postgres:
                        frame = frame.with_columns(pl.col("ts").dt.replace_time_zone(None))
                    numeric = port.spec.value_kind == "numeric"
                    frame = frame.select(
                        pl.lit(port.ref_uri).alias("ref_uri"), "ts",
                        (pl.col("value") if numeric else pl.lit(None, dtype=pl.Float64)).alias("numeric_value"),
                        (pl.lit(None, dtype=pl.String) if numeric else pl.col("value")).alias("text_value"))
                    self.store._insert_frame(conn, frame, revision)
        # Cursor movement is atomic with this chunk's output. Keep the input
        # frontier fixed until the final chunk, otherwise restart would skip
        # unfinished portions of the captured revision range.
        for binding, batch, _ in accepted:
            if batch.context.full_reset:
                # The full rebuild supersedes every older interval, even
                # work queued after its read snapshot was prepared.
                self._execute(conn, "DELETE FROM materialization_work WHERE progress_key=?", [binding.progress_key])
            elif batch.context.work_id:
                if batch.context.work_next:
                    self._execute(conn, "UPDATE materialization_work SET cursor_ts=? WHERE progress_key=?", [batch.context.work_next, binding.progress_key])
                    continue
                self._execute(conn, "DELETE FROM materialization_work WHERE progress_key=?", [binding.progress_key])
            self._execute(conn, "UPDATE binding_progress SET consumed_revision=? WHERE progress_key=?", [batch.context.to_revision, binding.progress_key])
        return {binding.signature: True for binding, _, _ in accepted}
