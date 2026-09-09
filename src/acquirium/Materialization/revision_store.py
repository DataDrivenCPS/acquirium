"""Revision snapshots, bounded work cursors, and atomic publication."""
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
        # None is the standalone store contract; a runtime installs its active plan.
        self.active_bindings: dict[str, str] | None = None
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
        return "timeseries t" if self._postgres else "timeseries t JOIN ref_ids r ON r.ref_id=t.ref_id"

    @property
    def _ref(self) -> str:
        return "t.ref_uri" if self._postgres else "r.ref_uri"

    def _execute(self, conn: Any, query: str, params: Iterable[Any] = ()) -> Any:
        return conn.execute(self._sql(query), list(params))
    def current_revision(self) -> int:
        with self.store._own_conn() as conn: return int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
    def progress_snapshot(self) -> tuple[int, dict[str, int]]:
        with self.store._own_conn() as conn:
            current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            progress = dict(self._execute(conn, "SELECT progress_key, consumed_revision FROM binding_progress").fetchall())
        return current, progress

    def initialise(self, binding: Binding, backfill: bool = False) -> int:
        with self.store._own_conn() as conn:
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is not None:
                return int(row[0])
        with self.store._lock, self.store._write_conn() as conn:
            if self.active_bindings is not None and self.active_bindings.get(binding.progress_key) != binding.generation:
                raise ValueError("binding is no longer active")
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is not None: return int(row[0])
            current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            # Backfill deliberately replays retained history for a newly seen
            # binding; otherwise only future changes are processed.
            consumed = 0 if backfill else current
            self._execute(conn, "INSERT INTO binding_progress VALUES (?, ?)", [binding.progress_key, consumed])
            return consumed
    def pending_keys(self) -> set[str]:
        with self.store._own_conn() as conn:
            return {row[0] for row in self._execute(conn, "SELECT progress_key FROM materialization_work").fetchall()}

    def retained_window(self, binding: Binding) -> TimeWindow | None:
        refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values}
                      | {p.ref_uri for p in binding.outputs.values()})
        with self.store._own_conn() as conn:
            marks = ','.join('?' for _ in refs)
            extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks})", refs).fetchone()
        return TimeWindow(*extent) if extent[0] is not None else None

    def request_reprocess(self, bindings: Iterable[Binding], window: TimeWindow) -> None:
        with self.store._lock, self.store._write_conn() as conn:
            for binding in bindings:
                owned = window
                if binding.every:
                    epoch = datetime(1970, 1, 1, tzinfo=UTC)
                    floor = lambda t: epoch + ((t - epoch) // binding.every) * binding.every
                    owned = TimeWindow(floor(window.start), floor(window.end) + binding.every - timedelta(microseconds=1))
                progress = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
                if progress is None:
                    raise ValueError("binding must be initialized before reprocessing")
                if self._execute(conn, "SELECT 1 FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone():
                    raise ValueError("binding already has pending work; wait for it to finish")
                self._execute(conn, "INSERT INTO materialization_work VALUES (?, ?, ?, ?, ?, ?)",
                              [binding.progress_key, uuid4().hex, owned.start.isoformat(), owned.end.isoformat(), progress[0], progress[0]])

    def _work_batch(self, conn: Any, binding: Binding, work) -> Batch:
        from dataclasses import replace
        work_id, cursor, end, previous, target = work
        start, finish = datetime.fromisoformat(cursor), datetime.fromisoformat(end)
        stop = min(finish, start + timedelta(days=1) - timedelta(microseconds=1))
        if binding.every:
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            stop = min(finish, epoch + (((stop - epoch) // binding.every) + 1) * binding.every - timedelta(microseconds=1))
        window = TimeWindow(start, stop)
        batch = self._window_batch(conn, binding, previous, target, window, output_window=window)
        following = (stop + timedelta(microseconds=1)).isoformat() if stop < finish else None
        return replace(batch, context=replace(batch.context, work_id=work_id, work_cursor=cursor, work_next=following))

    def next_batch(self, binding: Binding) -> Batch | None:
        # One read transaction is the snapshot boundary described by proposal.
        conn = self.store._connect()
        try:
            # DuckDB exposes ``begin()`` while psycopg starts an explicit
            # snapshot transaction through SQL.
            if self._postgres:
                conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                conn.begin()
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is None: raise KeyError("binding was not initialised")
            previous = int(row[0]); target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            work = self._execute(conn, "SELECT work_id, cursor_ts, end_ts, from_revision, to_revision FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone()
            batch = self._work_batch(conn, binding, work) if work else self._build_batch(conn, binding, previous, target, partition=True)
            conn.commit()
            if batch is None and previous != target:
                # Revisions for unrelated streams can be safely skipped.  The
                # compare makes this race-safe with an in-flight invocation.
                with self.store._lock, self.store._write_conn() as writer:
                    if self.active_bindings is None or self.active_bindings.get(binding.progress_key) == binding.generation:
                        self._execute(writer, "UPDATE binding_progress SET consumed_revision=? WHERE progress_key=? AND consumed_revision=?", [target, binding.progress_key, previous])
            return batch
        except BaseException:
            conn.rollback(); raise
        finally: conn.close()

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

    def _build_batch(self, conn: Any, binding: Binding, previous: int, target: int, *, partition: bool = False) -> Batch | None:
        """Read one coherent batch for the revisions in ``(previous, target]``."""
        refs = [d.ref_uri for values in binding.inputs.values() for d in values]
        if not refs or target == previous: return None
        marks = ",".join("?" for _ in refs)
        changed = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.last_revision>? AND t.last_revision<=?", [*refs, previous, target]).fetchone()
        if changed[0] is None: return None
        changed_window = TimeWindow(changed[0].replace(tzinfo=UTC) if changed[0].tzinfo is None else changed[0], changed[1].replace(tzinfo=UTC) if changed[1].tzinfo is None else changed[1])
        if partition and binding.lookback is not None and changed_window.end - changed_window.start > timedelta(days=1):
            window = self._output_window(binding, changed_window)
            work = (uuid4().hex, window.start.isoformat(), window.end.isoformat(), previous, target)
            with self.store._lock, self.store._write_conn() as writer:
                if self.active_bindings is not None and self.active_bindings.get(binding.progress_key) != binding.generation:
                    return None
                self._execute(writer, "INSERT INTO materialization_work VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (progress_key) DO NOTHING", [binding.progress_key, *work])
            return self._work_batch(conn, binding, work)
        return self._window_batch(conn, binding, previous, target, changed_window)

    def _window_batch(self, conn: Any, binding: Binding, previous: int, target: int,
                      changed_window: TimeWindow, *, output_window: TimeWindow | None = None) -> Batch:
        # lookback describes a trailing dependency; a correction also affects
        # following outputs. lookahead is the corresponding leading dependency.
        if binding.lookback is None:
            refs = sorted({d.ref_uri for values in binding.inputs.values() for d in values}
                          | {p.ref_uri for p in binding.outputs.values()})
            marks = ",".join("?" for _ in refs)
            extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks})", refs).fetchone()
            window = TimeWindow(extent[0], extent[1]) if extent[0] is not None else changed_window
            read = window
            if binding.custom_window is not None:
                window = self._output_window(binding, changed_window)
        else:
            window = output_window or self._output_window(binding, changed_window)
            read = TimeWindow(window.start - binding.lookback, window.end + binding.lookahead)
        if output_window is not None and binding.lookback is None:
            window = output_window
        from dataclasses import replace
        inputs = {alias: replace(self._stream_set(conn, alias, descriptors, read, previous, target), every=binding.every, _scheduled=True)
                  for alias, descriptors in binding.inputs.items()}
        return Batch(inputs, InputBatch(binding.signature, binding.graph_revision, previous,
                                        target, changed_window, read, binding.row, binding.result, window))

    @staticmethod
    def _output_window(binding: Binding, changed: TimeWindow) -> TimeWindow:
        if binding.custom_window is not None:
            window = binding.custom_window(changed)
            if not isinstance(window, TimeWindow):
                raise TypeError("output_window must return a TimeWindow")
            return window
        window = TimeWindow(changed.start - binding.lookahead,
                            changed.end + (binding.lookback or timedelta()))
        if binding.every is not None:
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            floor = lambda t: epoch + ((t - epoch) // binding.every) * binding.every
            window = TimeWindow(floor(window.start), floor(window.end) + binding.every - timedelta(microseconds=1))
        return window

    def _stream_set(self, conn: Any, alias: str, descriptors: tuple[StreamDescriptor,...], window: TimeWindow, previous: int, target: int) -> StreamSet:
        refs = [x.ref_uri for x in descriptors]
        if not refs: return StreamSet(alias, window, descriptors, converter=self.unit_converter)
        marks = ",".join("?" for _ in refs)
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
            numeric = self._execute(conn, f"SELECT 1 FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.text_value IS NOT NULL LIMIT 1", refs).fetchone() is None
        values = raw["numeric"] if numeric else pc.coalesce(raw["text"], pc.cast(raw["numeric"], pa.string()))
        table = pa.table({"ref_uri": raw["ref"],
                          "time": pc.cast(raw["time"], pa.timestamp("us", tz="UTC")),
                          "value": values})
        changed = table.filter(pc.and_(pc.greater(raw["revision"], previous), pc.less_equal(raw["revision"], target)))
        return StreamSet(alias, window, descriptors, table, changed, converter=self.unit_converter)
    def commit(self, binding: Binding, batch: Batch, results: Mapping[str, pa.Table]) -> bool:
        return self.commit_wave(((binding, batch, results),)).get(binding.signature, False)

    def commit_wave(self, commits: Iterable[tuple[Binding, Batch, Mapping[str, pa.Table]]]) -> Mapping[str, bool]:
        """Commit independent completed work in one revision transaction."""
        completed = tuple(commits)
        if not completed:
            return {}
        with self.store._lock, self.store._write_conn() as conn:
            accepted = []
            for binding, batch, results in completed:
                row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
                active = self.active_bindings
                valid = active is None or active.get(binding.progress_key) == binding.generation
                if batch.context.work_id:
                    work = self._execute(conn, "SELECT work_id, cursor_ts FROM materialization_work WHERE progress_key=?", [binding.progress_key]).fetchone()
                    valid = valid and work == (batch.context.work_id, batch.context.work_cursor)
                if valid and row is not None and int(row[0]) == batch.context.from_revision:
                    accepted.append((binding, batch, results))
            # Assigned ports replace their owned interval. Tombstones remain
            # discoverable by downstream revision scans, even for empty results.
            import polars as pl
            revision = None
            for binding, batch, results in accepted:
                window = batch.context.output_window or batch.context.changed_window
                for name, table in results.items():
                    port = binding.outputs[name]
                    mask = pc.and_(pc.greater_equal(table["time"], pa.scalar(window.start)),
                                   pc.less_equal(table["time"], pa.scalar(window.end)))
                    table = table.filter(mask)
                    ref_filter = "ref_uri=?" if self._postgres else "ref_id IN (SELECT ref_id FROM ref_ids WHERE ref_uri=?)"
                    existing = self._execute(conn, f"SELECT 1 FROM timeseries WHERE {ref_filter} AND ts>=? AND ts<=? AND NOT deleted LIMIT 1",
                                             [port.ref_uri, self._time(window.start), self._time(window.end)]).fetchone()
                    if not table.num_rows and existing is None:
                        continue
                    if revision is None:
                        revision = self.store._next_revision(conn)
                    self._execute(conn, f"UPDATE timeseries SET deleted=TRUE, last_revision=? WHERE {ref_filter} AND ts>=? AND ts<=? AND NOT deleted",
                                  [revision, port.ref_uri, self._time(window.start), self._time(window.end)])
                    self._execute(conn, """INSERT INTO streams (ref_uri, point_uri, source_id, ref_name, value_kind)
                        VALUES (?, ?, ?, ?, ?) ON CONFLICT (ref_uri) DO NOTHING""",
                        [port.ref_uri, port.point_uri, f"derived:{binding.application_name}", port.ref_name, port.spec.value_kind])
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
            for binding, batch, _ in accepted:
                if batch.context.work_id:
                    if batch.context.work_next:
                        self._execute(conn, "UPDATE materialization_work SET cursor_ts=? WHERE progress_key=?", [batch.context.work_next, binding.progress_key])
                        continue
                    self._execute(conn, "DELETE FROM materialization_work WHERE progress_key=?", [binding.progress_key])
                self._execute(conn, "UPDATE binding_progress SET consumed_revision=? WHERE progress_key=?", [batch.context.to_revision, binding.progress_key])
            return {binding.signature: True for binding, _, _ in accepted}
