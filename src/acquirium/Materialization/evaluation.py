"""Evaluate the view plan inside a step's transaction.

A step applies the inbox, then hands its connection to ``ViewEvaluator``,
which recomputes every output point the step's writes can change and writes
the results on the same connection, so raw and derived rows commit together.

What changed since the previous step comes from three places: data rows with
a revision above the evaluator's frontier, model writes recorded since, and
deployment changes recorded since. Model and deployment changes change the
plan; the region where an instance's presence or a row's life differs between
the old and the new plan is that view's structural change. An output point
is directly affected if its writer or match changed there, if a life that
clips one of its reads changed inside its window, or if it reads a changed
point. Affected points are recomputed in the order of the plan's schedule:
components writers first; inside a loop, slab by slab of the schedule time
``t + phi[view]``, each slab ``epsilon`` wide, so every read of a loop's own
outputs finds them final. A recomputed point whose value did not change does
not propagate.

Reads are clipped as the formalism says: a stream bound by a row is read only
inside the run of that row's life that contains the output time; named
inputs of a per-match instance by its row's run; a call is split wherever a
life it reads begins or ends, so one call never mixes two runs. Writes are
restricted to where the instance is present; a view's derived stream that no
present instance writes is retracted.

Times are integer microseconds; intervals are half-open.
"""
from __future__ import annotations

import math
from bisect import bisect_left, insort
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from time import perf_counter
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from acquirium.internals.intervals import (
    NEG_INF, POS_INF, Interval, expand, from_micros, intersect, merge, run_at, subtract, symmetric_difference,
    to_micros,
)
from acquirium.Materialization.models import (
    App, InputBatch, OutputBuilder, StreamDescriptor, StreamSet, TimeWindow,
)
from acquirium.Materialization.planner import PlannedInstance, TemporalPlan
from acquirium.Storage.metadata_outbox import execute

UTC = timezone.utc
ALL: tuple[Interval, ...] = ((NEG_INF, POS_INF),)


# Unbounded ends become these when a time reaches SQL or a datetime.
SAFE_LOW = to_micros(datetime(1000, 1, 1, tzinfo=UTC))
SAFE_HIGH = to_micros(datetime(9000, 1, 1, tzinfo=UTC))


def _safe(micros: int) -> int:
    return max(SAFE_LOW, min(SAFE_HIGH, micros))


def _dt(micros: int) -> datetime:
    return from_micros(_safe(micros))


def _bucket(value: int, every: int, *, up: bool) -> int:
    if value in (NEG_INF, POS_INF):
        return value
    floor = (value // every) * every
    return floor + every if up and floor != value else floor


@dataclass
class EvaluationReport:
    calls: int = 0
    points_written: int = 0
    points_changed: int = 0
    slabs: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=lambda: defaultdict(float))


class _Stream:
    """The rows of one stream known to a step: sorted times, their values,
    and the intervals over which they are complete."""
    __slots__ = ("times", "values", "covered")

    def __init__(self) -> None:
        self.times: list[int] = []
        self.values: dict[int, Any] = {}
        self.covered: list[Interval] = []

    def put(self, t: int, value: Any) -> None:
        if t not in self.values:
            insort(self.times, t)
        self.values[t] = value

    def drop(self, t: int) -> None:
        if t in self.values:
            del self.values[t]
            del self.times[bisect_left(self.times, t)]

    def within(self, start: int, end: int) -> list[int]:
        return self.times[bisect_left(self.times, start):bisect_left(self.times, end)]


class _Reader:
    """The step's view of the time-series tables.

    Rows a step needs are loaded into memory once and kept there; what the
    step writes is applied to that copy at once, so later reads in the step
    see it, and to the database by ``flush`` at the end of the step.
    """

    def __init__(self, store: Any, conn: Any) -> None:
        self.store, self.conn = store, conn
        self.postgres = getattr(store, "materialization_backend", None) == "postgres"
        self.revision: int | None = None
        self.streams: dict[str, _Stream] = {}
        self._ops: list = []
        self.queries = 0

    def _t(self, micros: int) -> datetime:
        value = _dt(micros)
        return value if self.postgres else value.replace(tzinfo=None)

    def _q(self, sql: str, params: Iterable[Any] = ()):
        self.queries += 1
        return execute(self.conn, sql, list(params))

    def changed_points(self, after: int, upto: int) -> dict[str, list[int]]:
        """Every (stream, time) written or deleted at a revision in ``(after, upto]``."""
        rows = self._q(
            "SELECT r.ref_uri, t.ts FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
            "WHERE t.last_revision > ? AND t.last_revision <= ?", [after, upto]).fetchall()
        out: dict[str, list[int]] = defaultdict(list)
        for ref, ts in rows:
            out[str(ref)].append(to_micros(ts))
        return out

    def current_revision(self) -> int:
        return int(self._q("SELECT current_revision FROM system_state").fetchone()[0])

    def extent(self, refs: Iterable[str]) -> Interval | None:
        refs = sorted(set(refs))
        if not refs:
            return None
        marks = ",".join("?" for _ in refs)
        low, high = self._q(
            f"SELECT min(t.ts), max(t.ts) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
            f"WHERE r.ref_uri IN ({marks}) AND NOT t.deleted", refs).fetchone()
        spans = [] if low is None else [(to_micros(low), to_micros(high) + 1)]
        for ref in refs:
            stream = self.streams.get(ref)
            if stream is not None and stream.times:
                spans.append((stream.times[0], stream.times[-1] + 1))
        if not spans:
            return None
        return (min(s for s, _ in spans), max(e for _, e in spans))

    def extents(self, refs: Iterable[str]) -> dict[str, Interval]:
        """Each stream's extent in one query, the step's own writes included."""
        refs = sorted(set(refs))
        out: dict[str, Interval] = {}
        if refs:
            marks = ",".join("?" for _ in refs)
            for ref, low, high in self._q(
                    f"SELECT r.ref_uri, min(t.ts), max(t.ts) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                    f"WHERE r.ref_uri IN ({marks}) AND NOT t.deleted GROUP BY r.ref_uri", refs).fetchall():
                out[ref] = (to_micros(low), to_micros(high) + 1)
        for ref in refs:
            stream = self.streams.get(ref)
            if stream is not None and stream.times:
                span = (stream.times[0], stream.times[-1] + 1)
                old = out.get(ref)
                out[ref] = span if old is None else (min(old[0], span[0]), max(old[1], span[1]))
        return out

    def load(self, needs: Mapping[str, list[Interval]]) -> None:
        """Bring what is not yet known of ``needs`` into memory, in one query."""
        missing = {}
        for ref, spans in needs.items():
            known = self.streams[ref].covered if ref in self.streams else []
            rest = subtract([(_safe(s), _safe(e)) for s, e in spans], known)
            if rest:
                missing[ref] = rest
        if not missing:
            return
        refs = sorted(missing)
        low = min(spans[0][0] for spans in missing.values())
        high = max(spans[-1][1] for spans in missing.values())
        marks = ",".join("?" for _ in refs)
        cursor = self._q(
            f"SELECT r.ref_uri, t.ts, t.numeric_value, t.text_value FROM timeseries t "
            f"JOIN ref_ids r ON r.ref_id = t.ref_id WHERE r.ref_uri IN ({marks}) AND NOT t.deleted "
            f"AND t.ts >= ? AND t.ts < ?", [*refs, self._t(low), self._t(high)])
        rows = cursor.fetchall()
        hull = (low, high)
        for ref in refs:
            stream = self.streams.setdefault(ref, _Stream())
            fresh = subtract([hull], stream.covered)
            stream.covered = merge(stream.covered + [hull])
            missing[ref] = fresh
        for ref, ts, numeric, text_value in rows:
            t = to_micros(ts)
            stream = self.streams[ref]
            if run_at(missing[ref], t) is not None:
                stream.put(t, text_value if text_value is not None else numeric)

    def read(self, ranges: Mapping[str, list[Interval]]) -> pa.Table:
        """Rows of each stream inside its ranges, as (ref_uri, time, numeric, text)."""
        ranges = {ref: spans for ref, spans in ranges.items() if spans}
        self.load(ranges)
        refs, times, numeric, texts = [], [], [], []
        for ref in sorted(ranges):
            stream = self.streams[ref]
            for start, end in ranges[ref]:
                for t in stream.within(_safe(start), _safe(end)):
                    value = stream.values[t]
                    refs.append(ref)
                    times.append(t)
                    if isinstance(value, str):
                        numeric.append(None)
                        texts.append(value)
                    else:
                        numeric.append(value)
                        texts.append(None)
        return pa.table({
            "ref_uri": pa.array(refs, pa.string()),
            "time": pa.array(times, pa.timestamp("us", tz="UTC")),
            "numeric": pa.array(numeric, pa.float64()),
            "text": pa.array(texts, pa.string()),
        })

    def _revision(self) -> int:
        if self.revision is None:
            self.revision = self.store._next_revision(self.conn)
        return self.revision

    def stage(self, ref: str, window: Interval, values: Mapping[int, Any], *, port: Any, application: str) -> list[int]:
        """Make ``ref`` hold exactly ``values`` inside ``window``; return the
        times whose value changes. The database follows at ``flush``."""
        self.load({ref: [window]})
        stream = self.streams[ref]
        before = {t: stream.values[t] for t in stream.within(window[0], window[1])}
        diff = sorted(t for t in set(before) | set(values) if before.get(t) != values.get(t))
        if not diff:
            return []
        for t in diff:
            if t in values:
                stream.put(t, values[t])
            else:
                stream.drop(t)
        removed = any(t not in values for t in diff)
        written = sorted(values) if removed else [t for t in diff if t in values]
        numeric = port is None or port.spec.value_kind == "numeric"
        self._ops.append((ref, window, removed, [(t, values[t]) for t in written], numeric, port, application))
        return diff

    def retract(self, ref: str, window: Interval) -> list[int]:
        return self.stage(ref, window, {}, port=None, application="")

    def flush(self) -> None:
        """Write every staged change: tombstones, then one insert."""
        ops, self._ops = self._ops, []
        if not ops:
            return
        revision = self._revision()
        rows, register = [], {}
        for ref, window, removed, written, numeric, port, application in ops:
            if removed:
                self._q(
                    "UPDATE timeseries SET deleted = TRUE, last_revision = ? WHERE ref_id = "
                    "(SELECT ref_id FROM ref_ids WHERE ref_uri = ?) AND ts >= ? AND ts < ? AND NOT deleted",
                    [revision, ref, self._t(window[0]), self._t(window[1])])
            rows.extend((ref, t, value, numeric) for t, value in written)
            if port is not None:
                register[ref] = (port, application)
        if register:
            refs = sorted(register)
            marks = ",".join("?" for _ in refs)
            known = {row[0] for row in self._q(f"SELECT ref_uri FROM streams WHERE ref_uri IN ({marks})", refs).fetchall()}
            for ref in refs:
                if ref not in known:
                    port, application = register[ref]
                    self._q("INSERT INTO streams (ref_uri, point_uri, source_id, ref_name, value_kind) "
                            "VALUES (?, ?, ?, ?, ?) ON CONFLICT (ref_uri) DO NOTHING",
                            [ref, port.point_uri, f"derived:{application}", port.ref_name, port.spec.value_kind])
        if rows:
            frame = pl.DataFrame({
                "ref_uri": [r[0] for r in rows],
                "ts": pl.Series([_dt(r[1]) for r in rows], dtype=pl.Datetime("us", "UTC")),
                "numeric_value": pl.Series([float(r[2]) if r[3] else None for r in rows], dtype=pl.Float64),
                "text_value": pl.Series([None if r[3] else str(r[2]) for r in rows], dtype=pl.String),
            })
            if not self.postgres:
                frame = frame.with_columns(pl.col("ts").dt.replace_time_zone(None))
            self.store._insert_frame(self.conn, frame, revision)

    def derived_streams(self, application: str) -> list[str]:
        return [row[0] for row in self._q("SELECT ref_uri FROM streams WHERE source_id = ?",
                                           [f"derived:{application}"]).fetchall()]


@dataclass(frozen=True)
class _Input:
    """One stream an instance reads: input name, descriptor, and the life
    that clips it."""
    name: str
    descriptor: StreamDescriptor
    life: tuple[Interval, ...]


class _Runner:
    """Evaluates one plan for one step."""

    def __init__(self, plan: TemporalPlan, reader: _Reader, unit_converter: Any, report: EvaluationReport) -> None:
        self.plan, self.reader, self.converter, self.report = plan, reader, unit_converter, report
        self._extents: dict[int, Interval | None] = {}
        self._changed: dict[str, list[int]] = defaultdict(list)
        self.by_view: dict[str, list[PlannedInstance]] = defaultdict(list)
        for instance in plan.instances:
            self.by_view[instance.view].append(instance)
        # stream -> the (instance, input) pairs that read it
        self.readers: dict[str, list[tuple[PlannedInstance, _Input]]] = defaultdict(list)
        for instance in plan.instances:
            for item in self.inputs(instance):
                self.readers[item.descriptor.ref_uri].append((instance, item))

    # ---- shape of an instance ----
    def deployment(self, instance: PlannedInstance):
        return self.plan.deployments[instance.text]

    def inputs(self, instance: PlannedInstance) -> list[_Input]:
        out = []
        for row in instance.rows:
            for name, descriptors in row.streams.items():
                out.extend(_Input(name, d, row.life) for d in descriptors)
        named_life = instance.match_life if instance.match_life is not None else ALL
        out.extend(_Input(name, d, named_life) for name, d in instance.named.items())
        return out

    def window(self, instance: PlannedInstance, name: str) -> tuple[int, int]:
        """An input's (lookback, lookahead): an output at a bucket's start
        reads to the bucket's end, so the bucket width adds to the lookahead."""
        deployment = self.deployment(instance)
        lb, la = deployment.window(name)
        if deployment.every is not None:
            la += int(deployment.every.total_seconds() * 1_000_000)
        return lb, la

    def widest(self, instance: PlannedInstance) -> tuple[int, int]:
        names = {item.name for item in self.inputs(instance)} or {""}
        windows = [self.window(instance, name) for name in names]
        return max(max(lb for lb, _ in windows), 0), max(max(la for _, la in windows), 0)

    def bucketed(self, instance: PlannedInstance, region: list[Interval]) -> list[Interval]:
        every = self.deployment(instance).every
        if every is None:
            return region
        step = int(every.total_seconds() * 1_000_000)
        return merge((_bucket(s, step, up=False), _bucket(e, step, up=True)) for s, e in region)

    def clamp(self, instance: PlannedInstance, region: list[Interval], extent: Interval | None) -> list[Interval]:
        """Bound infinite ends by the data the instance reads and writes."""
        if not region:
            return []
        if extent is None:
            return [span for span in region if span[0] != NEG_INF and span[1] != POS_INF]
        lb, la = self.widest(instance)
        low = extent[0] - la - 1 if extent[0] - la - 1 > NEG_INF else NEG_INF
        high = extent[1] + (0 if lb == POS_INF else lb) + 1
        return intersect(region, [(low, min(high, POS_INF))])

    def extent(self, instance: PlannedInstance) -> Interval | None:
        refs = [item.descriptor.ref_uri for item in self.inputs(instance)]
        refs += [port.ref_uri for port in instance.outputs.values()]
        return self.reader.extent(refs)

    # ---- one call ----
    def compute(self, instance: PlannedInstance, window: Interval) -> None:
        """Recompute ``instance`` over ``window`` (inside its presence) and
        stage the results; ``flush`` writes them."""
        if window[0] >= window[1]:
            return
        inputs = self.inputs(instance)
        cuts = sorted({p for item in inputs for span in item.life for p in span if window[0] < p < window[1]})
        points = [window[0], *cuts, window[1]]
        for left, right in zip(points, points[1:]):
            self._call(instance, (left, right), inputs)

    def flush(self) -> dict[str, list[Interval]]:
        """The points changed since the last call, per stream, as intervals."""
        changed, self._changed = self._changed, defaultdict(list)
        self.report.points_changed += sum(len(times) for times in changed.values())
        return {stream: merge((t, t + 1) for t in times) for stream, times in changed.items()}

    def retract(self, ref: str, window: Interval) -> None:
        moved = self.reader.retract(ref, window)
        if moved:
            self._changed[ref].extend(moved)

    def preload(self, instance: PlannedInstance, windows: Iterable[Interval]) -> dict[str, list[Interval]]:
        """What computing ``instance`` over ``windows`` reads and writes."""
        deployment = self.deployment(instance)
        needs: dict[str, list[Interval]] = defaultdict(list)
        for window in windows:
            for item in self.inputs(instance):
                lb, la = self.window(instance, item.name)
                low = NEG_INF if lb == POS_INF else window[0] - lb
                needs[item.descriptor.ref_uri].append((low, window[1] + max(la, 0)))
            for port in instance.outputs.values():
                needs[port.ref_uri].append(window)
        return needs

    def extent_of(self, instance: PlannedInstance) -> Interval | None:
        key = id(instance)
        if key not in self._extents:
            self._extents[key] = self.extent(instance)
        return self._extents[key]

    def prefetch_extents(self, instances: Iterable[PlannedInstance]) -> None:
        """Know the extents of many instances with one query."""
        instances = [i for i in instances if id(i) not in self._extents]
        if not instances:
            return
        refs_of = {id(i): [item.descriptor.ref_uri for item in self.inputs(i)] +
                          [port.ref_uri for port in i.outputs.values()] for i in instances}
        found = self.reader.extents(ref for refs in refs_of.values() for ref in refs)
        for instance in instances:
            spans = [found[ref] for ref in refs_of[id(instance)] if ref in found]
            self._extents[id(instance)] = (min(s for s, _ in spans), max(e for _, e in spans)) if spans else None

    def _call(self, instance: PlannedInstance, window: Interval, inputs: list[_Input]) -> None:
        deployment = self.deployment(instance)
        app = self.plan.apps[instance.definition]
        started = perf_counter()
        ranges: dict[str, list[Interval]] = defaultdict(list)
        by_name: dict[str, list[StreamDescriptor]] = defaultdict(list)
        rows_alive = []
        for row in instance.rows:
            if run_at(row.life, window[0]) is not None:
                rows_alive.append(row)
        for item in inputs:
            run = run_at(item.life, window[0])
            if run is None:
                continue
            lb, la = self.window(instance, item.name)
            low = NEG_INF if lb == POS_INF else window[0] - lb
            high = window[1] + la
            span = intersect([(low, high)], [run])
            if span:
                ranges[item.descriptor.ref_uri].extend(span)
            by_name[item.name].append(item.descriptor)
        table = self.reader.read({ref: merge(spans) for ref, spans in ranges.items()})
        self.report.timings["read"] += perf_counter() - started
        lows = [spans[0][0] for spans in ranges.values() if spans]
        highs = [spans[-1][1] for spans in ranges.values() if spans]
        read_window = TimeWindow(_dt(min(lows) if lows else window[0]),
                                 _dt((max(highs) if highs else window[1]) - 1))
        output_window = TimeWindow(_dt(window[0]), _dt(window[1] - 1))
        streams: dict[str, StreamSet] = {}
        names = {item.name for item in inputs} | set(instance.aliases) | set(instance.named)
        for name in sorted(names):
            descriptors = tuple({d.ref_uri: d for d in by_name.get(name, ())}.values())
            refs = [d.ref_uri for d in descriptors]
            part = table.filter(pc.is_in(table["ref_uri"], pa.array(refs, pa.string()))) if refs else table.slice(0, 0)
            numeric = part["text"].null_count == part.num_rows
            values = part["numeric"] if numeric else pc.coalesce(part["text"], pc.cast(part["numeric"], pa.string()))
            data = pa.table({"ref_uri": part["ref_uri"], "time": part["time"], "value": values})
            every = deployment.every
            streams[name] = StreamSet(name, read_window, descriptors, data, data, converter=self.converter,
                                      every=every, _scheduled=True)
        per_match = deployment.grouping == "per_match"
        row = dict(instance.rows[0].columns) if per_match and instance.rows else (
            dict(rows_alive[0].columns) if len(rows_alive) == 1 else None)
        result = () if per_match else tuple(dict(r.columns) for r in rows_alive)
        context = InputBatch(instance.id, 0, 0, 0, output_window, read_window,
                             row, result, output_window=output_window)
        builder = OutputBuilder(instance.outputs)
        computing = perf_counter()
        app.transform(streams, builder, context)
        self.report.timings["transform"] += perf_counter() - computing
        self.report.calls += 1
        writing = perf_counter()
        for port_name, port in instance.outputs.items():
            if port_name not in builder.values:
                continue
            out = builder.values[port_name]
            times = pc.cast(out["time"], pa.int64()).to_pylist()
            values = {t: v for t, v in zip(times, out["value"].to_pylist()) if window[0] <= t < window[1]}
            self.report.points_written += len(values)
            moved = self.reader.stage(port.ref_uri, window, values, port=port, application=instance.view)
            if moved:
                self._changed[port.ref_uri].extend(moved)
        self.report.timings["write"] += perf_counter() - writing

    # ---- affected regions ----
    def data_affected(self, instance: PlannedInstance, changed: Mapping[str, list[Interval]]) -> list[Interval]:
        deployment = self.deployment(instance)
        app = self.plan.apps[instance.definition]
        custom = type(app).output_window is not App.output_window
        out: list[Interval] = []
        for item in self.inputs(instance):
            spans = changed.get(item.descriptor.ref_uri)
            if not spans:
                continue
            touched = intersect(spans, item.life)
            if not touched:
                continue
            if custom:
                # The app maps changed inputs to the outputs they affect.
                for start, end in touched:
                    window = app.output_window(TimeWindow(_dt(_safe(start)), _dt(_safe(end) - 1)))
                    out.append((to_micros(window.start), to_micros(window.end) + 1))
                continue
            lb, la = self.window(instance, item.name)
            out.extend(expand(touched, lookback=lb, lookahead=la))
        return merge(out)


class ViewEvaluator:
    """The time-aware view evaluation a step runs after the inbox.

    ``prepare`` (outside the step's transaction) returns the plan to use;
    ``evaluate`` (inside) recomputes what changed and advances the frontier
    kept in ``view_engine_state``.
    """

    def __init__(self, store: Any, materializer: Any, unit_converter: Any = None) -> None:
        self.store, self.materializer, self.unit_converter = store, materializer, unit_converter
        self.timings: dict[str, float] = {}
        self.last_report: EvaluationReport | None = None
        # Instances whose transform failed, until a later call succeeds. Their
        # windows are queued in view_pending and retried by every step.
        self.errors: dict[str, str] = {}
        self._failed: list[tuple[str, Interval]] = []
        with store._lock, store._write_conn() as conn:
            execute(conn, """CREATE TABLE IF NOT EXISTS view_engine_state (
                id INTEGER PRIMARY KEY, data_revision BIGINT NOT NULL,
                model_head BIGINT NOT NULL, deployment_head BIGINT NOT NULL)""")
            execute(conn, """CREATE TABLE IF NOT EXISTS view_pending (
                name VARCHAR NOT NULL, valid_from BIGINT NOT NULL, valid_to BIGINT NOT NULL)""")

    def prepare(self) -> Any:
        return self.materializer.current_plan()

    def committed(self, report: Any) -> None:
        self.materializer.step_committed(self._applied)

    def evaluate(self, conn: Any, prepared: Any) -> bool:
        plan, model_head, deployment_head = prepared
        reader = _Reader(self.store, conn)
        report = EvaluationReport()
        started = perf_counter()
        state = execute(conn, "SELECT data_revision, model_head, deployment_head FROM view_engine_state WHERE id = 1").fetchone()
        current = reader.current_revision()
        old_plan = self.materializer.applied_plan()
        structural: dict[str, list[Interval]] = defaultdict(list)
        first = state is None
        known = self.materializer.known_views()
        if first:
            for view in {i.view for i in plan.instances} | set(_names(plan)) | known:
                structural[view] = list(ALL)
            data: dict[str, list[Interval]] = {}
        else:
            data_revision, old_model, old_deployment = (int(v) for v in state)
            if (old_model, old_deployment) != (model_head, deployment_head):
                if old_plan is not None and old_plan[1:] == (old_model, old_deployment):
                    for view, region in _plan_difference(old_plan[0], plan).items():
                        structural[view].extend(region)
                else:
                    # The plan these heads were evaluated with is not known
                    # (a restart): every view any deployment ever named,
                    # wherever anything changed.
                    region = self.materializer.changed_region(old_model, model_head, old_deployment, deployment_head)
                    for view in {i.view for i in plan.instances} | set(_names(plan)) | known:
                        structural[view] = list(region)
            points = reader.changed_points(data_revision, current)
            data = {ref: merge((t, t + 1) for t in times) for ref, times in points.items()}
        for name, start, end in execute(conn, "SELECT name, valid_from, valid_to FROM view_pending").fetchall():
            structural[name].append((int(start), int(end)))
        execute(conn, "DELETE FROM view_pending")
        self._failed = []
        report.timings["changes"] = perf_counter() - started
        runner = _Runner(plan, reader, self.unit_converter, report)
        changed: dict[str, list[Interval]] = defaultdict(list)
        for ref, spans in data.items():
            changed[ref].extend(spans)
        self._run(plan, runner, structural, changed, report, old_plan[0] if old_plan else None)
        flushing = perf_counter()
        reader.flush()
        report.timings["flush"] = perf_counter() - flushing
        for name, (start, end) in self._failed:
            execute(conn, "INSERT INTO view_pending VALUES (?, ?, ?)", [name, start, end])
        report.errors = dict(self.errors)
        final = reader.current_revision()
        if state is None:
            execute(conn, "INSERT INTO view_engine_state VALUES (1, ?, ?, ?)", [final, model_head, deployment_head])
        else:
            execute(conn, "UPDATE view_engine_state SET data_revision = ?, model_head = ?, deployment_head = ? WHERE id = 1",
                    [final, model_head, deployment_head])
        self._applied = (plan, model_head, deployment_head)
        report.timings["evaluate"] = perf_counter() - started
        report.timings["queries"] = reader.queries
        self.timings = dict(report.timings)
        self.last_report = report
        return report.points_changed > 0 or reader.revision is not None

    # ---- the schedule ----
    def _run(self, plan: TemporalPlan, runner: _Runner, structural: Mapping[str, list[Interval]],
             changed: dict[str, list[Interval]], report: EvaluationReport, old_plan: TemporalPlan | None) -> None:
        schedule = plan.schedule
        # Views of the old plan that are gone keep streams to retract.
        views_in_order = [name for component in schedule.components for name in component]
        for name in sorted(set(structural) - set(views_in_order)):
            self._retract_view(runner, name, structural[name], {})
        for stream, spans in runner.flush().items():
            changed[stream].extend(spans)
        for component in schedule.components:
            loop = any(name in schedule.loops for name in component)
            if loop:
                self._run_loop(plan, runner, component, structural, changed, report)
            else:
                (name,) = component
                self._run_view(runner, name, structural.get(name, []), changed, report)

    def _compute(self, runner: _Runner, instance: PlannedInstance, window: Interval) -> None:
        try:
            runner.compute(instance, window)
        except Exception as error:
            self.errors[instance.id] = f"{type(error).__name__}: {error}"
            self._failed.append((instance.view, window))
        else:
            self.errors.pop(instance.id, None)

    def _structural(self, runner: _Runner, instance: PlannedInstance, region: list[Interval]) -> list[Interval]:
        if not region:
            return []
        lb, la = runner.widest(instance)
        grown = expand(region, lookback=lb, lookahead=la) if (lb, la) != (0, 0) else []
        return merge(list(region) + grown)

    def _regions(self, runner: _Runner, instance: PlannedInstance, structural: list[Interval],
                 changed: Mapping[str, list[Interval]]) -> list[Interval]:
        region = merge(self._structural(runner, instance, structural) + runner.data_affected(instance, changed))
        region = intersect(region, instance.present)
        if not region:
            return []
        if region[0][0] == NEG_INF or region[-1][1] == POS_INF:
            region = runner.clamp(instance, region, runner.extent_of(instance))
        return intersect(runner.bucketed(instance, region), instance.present)

    def _run_view(self, runner: _Runner, name: str, structural: list[Interval],
                  changed: dict[str, list[Interval]], report: EvaluationReport) -> None:
        written: dict[str, list[Interval]] = defaultdict(list)
        work = []
        needs: dict[str, list[Interval]] = defaultdict(list)
        if structural:
            runner.prefetch_extents(runner.by_view.get(name, []))
        for instance in runner.by_view.get(name, []):
            for port in instance.outputs.values():
                written[port.ref_uri].extend(instance.present)
            region = self._regions(runner, instance, structural, changed)
            if region:
                work.append((instance, region))
                for ref, spans in runner.preload(instance, region).items():
                    needs[ref].extend(spans)
        runner.reader.load({ref: merge(spans) for ref, spans in needs.items()})
        for instance, region in work:
            for window in region:
                self._compute(runner, instance, window)
        self._retract_view(runner, name, structural, written)
        for stream, spans in runner.flush().items():
            changed[stream].extend(spans)

    def _retract_view(self, runner: _Runner, name: str, structural: list[Interval],
                      written: Mapping[str, list[Interval]]) -> None:
        """Stage the removal of a view's outputs where no present instance
        writes them, inside its structural change."""
        if not structural:
            return
        for ref in runner.reader.derived_streams(name):
            gone = subtract(structural, written.get(ref, []))
            if not gone:
                continue
            extent = runner.reader.extent([ref])
            if extent is None:
                continue
            for window in intersect(gone, [extent]):
                runner.retract(ref, window)

    def _run_loop(self, plan: TemporalPlan, runner: _Runner, component: tuple[str, ...],
                  structural: Mapping[str, list[Interval]], changed: dict[str, list[Interval]],
                  report: EvaluationReport) -> None:
        schedule = plan.schedule
        epsilon = schedule.epsilon
        members = [instance for name in component for instance in runner.by_view.get(name, [])]
        index = {id(instance): instance for instance in members}
        runner.prefetch_extents(members)
        pending: dict[int, list[Interval]] = {}
        for instance in members:
            pending[id(instance)] = self._regions(runner, instance, structural.get(instance.view, []), changed)
        own = {port.ref_uri for instance in members for port in instance.outputs.values()}
        firsts = [spans[0][0] for spans in pending.values() if spans]
        if firsts:
            # A loop may run on to the end of its data: load it all from the
            # first pending time on.
            needs: dict[str, list[Interval]] = defaultdict(list)
            for instance in members:
                for ref, spans in runner.preload(instance, [(min(firsts), SAFE_HIGH)]).items():
                    needs[ref].extend(spans)
            runner.reader.load({ref: merge(spans) for ref, spans in needs.items()})
        while True:
            starts = [(Fraction(spans[0][0]) + schedule.phi[index[key].view], key)
                      for key, spans in pending.items() if spans]
            if not starts:
                break
            low = min(start for start, _ in starts)
            high = low + epsilon
            report.slabs += 1
            for key, spans in list(pending.items()):
                instance = index[key]
                phi = schedule.phi[instance.view]
                lo, hi = math.ceil(low - phi), math.ceil(high - phi)
                part = intersect(spans, [(lo, hi)])
                pending[key] = subtract(spans, [(lo, hi)])
                for window in part:
                    self._compute(runner, instance, window)
            produced = runner.flush()
            for stream, spans in produced.items():
                changed[stream].extend(spans)
                if stream not in own:
                    continue
                for reader_instance, item in runner.readers.get(stream, []):
                    key = id(reader_instance)
                    if key not in pending:
                        continue
                    lb, la = runner.window(reader_instance, item.name)
                    touched = intersect(spans, item.life)
                    region = intersect(expand(touched, lookback=lb, lookahead=la), reader_instance.present)
                    if region and (region[0][0] == NEG_INF or region[-1][1] == POS_INF):
                        region = runner.clamp(reader_instance, region, runner.extent_of(reader_instance))
                    region = intersect(runner.bucketed(reader_instance, region), reader_instance.present)
                    pending[key] = merge(pending[key] + region)
        written: dict[str, list[Interval]] = defaultdict(list)
        for instance in members:
            for port in instance.outputs.values():
                written[port.ref_uri].extend(instance.present)
        for name in component:
            self._retract_view(runner, name, structural.get(name, []), written)
        for stream, spans in runner.flush().items():
            changed[stream].extend(spans)


def _names(plan: TemporalPlan | None) -> set[str]:
    if plan is None:
        return set()
    return {definition.view for definition in plan.definitions}


def _plan_difference(old: TemporalPlan, new: TemporalPlan) -> dict[str, list[Interval]]:
    """Per view, where its instances, their presence, or the lives of their
    rows differ between two plans."""
    def shape(plan: TemporalPlan) -> dict[str, dict[tuple, list[Interval]]]:
        out: dict[str, dict[tuple, list[Interval]]] = defaultdict(dict)
        for instance in plan.instances:
            out[instance.view][("present", instance.text, instance.key)] = list(instance.present)
            for row in instance.rows:
                key = ("life", instance.text, instance.key, tuple(sorted(row.columns.items())),
                       tuple(sorted((k, tuple(d.ref_uri for d in v)) for k, v in row.streams.items())))
                out[instance.view][key] = list(row.life)
            out[instance.view][("outputs", instance.text, instance.key,
                                tuple(sorted((p, o.ref_uri) for p, o in instance.outputs.items())),
                                tuple(sorted((n, d.ref_uri) for n, d in instance.named.items())))] = list(instance.present)
        return out
    before, after = shape(old), shape(new)
    regions: dict[str, list[Interval]] = {}
    for view in set(before) | set(after) | _names(old) | _names(new):
        old_view, new_view = before.get(view, {}), after.get(view, {})
        region: list[Interval] = []
        for key in set(old_view) | set(new_view):
            region.extend(symmetric_difference(old_view.get(key, []), new_view.get(key, [])))
        # a definition that changed text changes the view where either holds
        old_defs = {d.text: d.deployed for d in old.definitions if d.view == view}
        new_defs = {d.text: d.deployed for d in new.definitions if d.view == view}
        for text in set(old_defs) | set(new_defs):
            region.extend(symmetric_difference(old_defs.get(text, ()), new_defs.get(text, ())))
        if region:
            regions[view] = merge(region)
    return regions
