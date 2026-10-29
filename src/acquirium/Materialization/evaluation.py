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


class _Reader:
    """Reads and writes the time-series tables on the step's connection."""

    def __init__(self, store: Any, conn: Any) -> None:
        self.store, self.conn = store, conn
        self.postgres = getattr(store, "materialization_backend", None) == "postgres"
        self.revision: int | None = None

    def _t(self, micros: int) -> datetime:
        value = _dt(micros)
        return value if self.postgres else value.replace(tzinfo=None)

    def _q(self, sql: str, params: Iterable[Any] = ()):
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
        if low is None:
            return None
        return (to_micros(low), to_micros(high) + 1)

    def read(self, ranges: Mapping[str, list[Interval]]) -> pa.Table:
        """Rows of each stream inside its ranges, as (ref_uri, time, value, revision)."""
        refs = sorted(ref for ref, spans in ranges.items() if spans)
        empty = pa.table({"ref_uri": pa.array([], pa.string()), "time": pa.array([], pa.timestamp("us", tz="UTC")),
                          "numeric": pa.array([], pa.float64()), "text": pa.array([], pa.string())})
        if not refs:
            return empty
        low = min(spans[0][0] for spans in (ranges[r] for r in refs))
        high = max(spans[-1][1] for spans in (ranges[r] for r in refs))
        marks = ",".join("?" for _ in refs)
        cursor = self._q(
            f"SELECT r.ref_uri, t.ts, t.numeric_value, t.text_value FROM timeseries t "
            f"JOIN ref_ids r ON r.ref_id = t.ref_id WHERE r.ref_uri IN ({marks}) AND NOT t.deleted "
            f"AND t.ts >= ? AND t.ts < ? ORDER BY r.ref_uri, t.ts",
            [*refs, self._t(low), self._t(high)])
        if self.postgres:
            rows = cursor.fetchall()
            table = pa.table({
                "ref_uri": pa.array([r[0] for r in rows], pa.string()),
                "time": pa.array([r[1] for r in rows], pa.timestamp("us", tz="UTC")),
                "numeric": pa.array([r[2] for r in rows], pa.float64()),
                "text": pa.array([r[3] for r in rows], pa.string()),
            })
        else:
            table = cursor.to_arrow_table().rename_columns(["ref_uri", "time", "numeric", "text"])
            table = table.set_column(1, "time", pc.cast(pc.assume_timezone(table["time"], "UTC"), pa.timestamp("us", tz="UTC")))
        if not table.num_rows:
            return empty
        # keep each stream's rows inside its own ranges
        times = pc.cast(table["time"], pa.int64())
        keep = None
        for ref in refs:
            is_ref = pc.equal(table["ref_uri"], ref)
            inside = None
            for start, end in ranges[ref]:
                span = pc.and_(pc.greater_equal(times, _safe(start)), pc.less(times, _safe(end)))
                inside = span if inside is None else pc.or_(inside, span)
            part = pc.and_(is_ref, inside)
            keep = part if keep is None else pc.or_(keep, part)
        return table.filter(keep)

    def output_rows(self, ref: str, window: Interval) -> dict[int, Any]:
        rows = self._q(
            "SELECT t.ts, t.numeric_value, t.text_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
            "WHERE r.ref_uri = ? AND NOT t.deleted AND t.ts >= ? AND t.ts < ?",
            [ref, self._t(window[0]), self._t(window[1])]).fetchall()
        return {to_micros(ts): (text if text is not None else numeric) for ts, numeric, text in rows}

    def _revision(self) -> int:
        if self.revision is None:
            self.revision = self.store._next_revision(self.conn)
        return self.revision

    def replace(self, ref: str, window: Interval, values: Mapping[int, Any], *, port: Any, application: str) -> list[int]:
        """Make ``ref`` hold exactly ``values`` inside ``window``; return the
        times whose value changed."""
        old = self.output_rows(ref, window)
        changed = sorted(t for t in set(old) | set(values) if old.get(t) != values.get(t))
        if not changed:
            return []
        revision = self._revision()
        removed = [t for t in changed if t not in values]
        if removed:
            self._q(
                "UPDATE timeseries SET deleted = TRUE, last_revision = ? WHERE ref_id = "
                "(SELECT ref_id FROM ref_ids WHERE ref_uri = ?) AND ts >= ? AND ts < ? AND NOT deleted",
                [revision, ref, self._t(window[0]), self._t(window[1])])
            written = sorted(values)
        else:
            written = [t for t in changed if t in values]
        if port is not None:
            self._q("INSERT INTO streams (ref_uri, point_uri, source_id, ref_name, value_kind) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT (ref_uri) DO NOTHING",
                    [ref, port.point_uri, f"derived:{application}", port.ref_name, port.spec.value_kind])
        if written:
            numeric = port is None or port.spec.value_kind == "numeric"
            frame = pl.DataFrame({
                "ref_uri": [ref] * len(written),
                "ts": pl.Series([_dt(t) for t in written], dtype=pl.Datetime("us", "UTC")),
                "numeric_value": pl.Series([float(values[t]) if numeric else None for t in written], dtype=pl.Float64),
                "text_value": pl.Series([None if numeric else str(values[t]) for t in written], dtype=pl.String),
            })
            if not self.postgres:
                frame = frame.with_columns(pl.col("ts").dt.replace_time_zone(None))
            self.store._insert_frame(self.conn, frame, revision)
        return changed

    def retract(self, ref: str, window: Interval) -> list[int]:
        return self.replace(ref, window, {}, port=None, application="")

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

    def widest(self, instance: PlannedInstance) -> tuple[int, int]:
        deployment = self.deployment(instance)
        names = {item.name for item in self.inputs(instance)} or {""}
        windows = [deployment.window(name) for name in names]
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
    def compute(self, instance: PlannedInstance, window: Interval) -> dict[str, list[int]]:
        """Recompute ``instance`` over ``window`` (inside its presence) and
        write the results; return the changed times per output stream."""
        changed: dict[str, list[int]] = defaultdict(list)
        if window[0] >= window[1]:
            return changed
        inputs = self.inputs(instance)
        cuts = sorted({p for item in inputs for span in item.life for p in span if window[0] < p < window[1]})
        points = [window[0], *cuts, window[1]]
        for left, right in zip(points, points[1:]):
            for stream, times in self._call(instance, (left, right), inputs).items():
                changed[stream].extend(times)
        return changed

    def _call(self, instance: PlannedInstance, window: Interval, inputs: list[_Input]) -> dict[str, list[int]]:
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
            lb, la = deployment.window(item.name)
            low = NEG_INF if lb == POS_INF else window[0] - lb
            high = window[1] - 1 + la + 1
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
        names = {item.name for item in inputs}
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
        changed: dict[str, list[int]] = {}
        for port_name, port in instance.outputs.items():
            if port_name not in builder.values:
                continue
            out = builder.values[port_name]
            times = pc.cast(out["time"], pa.int64()).to_pylist()
            values = {t: v for t, v in zip(times, out["value"].to_pylist()) if window[0] <= t < window[1]}
            self.report.points_written += len(values)
            moved = self.reader.replace(port.ref_uri, window, values, port=port, application=instance.view)
            if moved:
                changed[port.ref_uri] = moved
                self.report.points_changed += len(moved)
        self.report.timings["write"] += perf_counter() - writing
        return changed

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
            lb, la = deployment.window(item.name)
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
        if first:
            for view in {i.view for i in plan.instances} | set(_names(plan)):
                structural[view] = list(ALL)
            data: dict[str, list[Interval]] = {}
        else:
            data_revision, old_model, old_deployment = (int(v) for v in state)
            if (old_model, old_deployment) != (model_head, deployment_head):
                if old_plan is not None and old_plan[1:] == (old_model, old_deployment):
                    for view, region in _plan_difference(old_plan[0], plan).items():
                        structural[view].extend(region)
                else:
                    region = self.materializer.changed_region(old_model, model_head, old_deployment, deployment_head)
                    for view in {i.view for i in plan.instances} | set(_names(plan)) | set(
                            _names(old_plan[0]) if old_plan else ()):
                        structural[view] = list(region)
            points = reader.changed_points(data_revision, current)
            data = {ref: merge((t, t + 1) for t in times) for ref, times in points.items()}
        for name, start, end in execute(conn, "SELECT name, valid_from, valid_to FROM view_pending").fetchall():
            structural[name].append((int(start), int(end)))
        execute(conn, "DELETE FROM view_pending")
        report.timings["changes"] = perf_counter() - started
        runner = _Runner(plan, reader, self.unit_converter, report)
        changed: dict[str, list[Interval]] = defaultdict(list)
        for ref, spans in data.items():
            changed[ref].extend(spans)
        self._run(plan, runner, structural, changed, report, old_plan[0] if old_plan else None)
        final = reader.current_revision()
        if state is None:
            execute(conn, "INSERT INTO view_engine_state VALUES (1, ?, ?, ?)", [final, model_head, deployment_head])
        else:
            execute(conn, "UPDATE view_engine_state SET data_revision = ?, model_head = ?, deployment_head = ? WHERE id = 1",
                    [final, model_head, deployment_head])
        self._applied = (plan, model_head, deployment_head)
        report.timings["evaluate"] = perf_counter() - started
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
            self._retract_view(runner, name, structural[name], {}, changed)
        for component in schedule.components:
            loop = any(name in schedule.loops for name in component)
            if loop:
                self._run_loop(plan, runner, component, structural, changed, report)
            else:
                (name,) = component
                self._run_view(runner, name, structural.get(name, []), changed, report)

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
        region = runner.clamp(instance, region, runner.extent(instance))
        return intersect(runner.bucketed(instance, region), instance.present)

    def _run_view(self, runner: _Runner, name: str, structural: list[Interval],
                  changed: dict[str, list[Interval]], report: EvaluationReport) -> None:
        written: dict[str, list[Interval]] = defaultdict(list)
        for instance in runner.by_view.get(name, []):
            for port in instance.outputs.values():
                written[port.ref_uri].extend(instance.present)
            region = self._regions(runner, instance, structural, changed)
            for window in region:
                try:
                    for stream, times in runner.compute(instance, window).items():
                        changed[stream].extend(merge((t, t + 1) for t in times))
                except Exception as error:
                    report.errors[instance.id] = f"{type(error).__name__}: {error}"
        self._retract_view(runner, name, structural, written, changed)

    def _retract_view(self, runner: _Runner, name: str, structural: list[Interval],
                      written: Mapping[str, list[Interval]], changed: dict[str, list[Interval]]) -> None:
        """Remove a view's outputs where no present instance writes them, inside
        its structural change."""
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
                times = runner.reader.retract(ref, window)
                if times:
                    changed[ref].extend(merge((t, t + 1) for t in times))

    def _run_loop(self, plan: TemporalPlan, runner: _Runner, component: tuple[str, ...],
                  structural: Mapping[str, list[Interval]], changed: dict[str, list[Interval]],
                  report: EvaluationReport) -> None:
        schedule = plan.schedule
        epsilon = schedule.epsilon
        members = [instance for name in component for instance in runner.by_view.get(name, [])]
        index = {id(instance): instance for instance in members}
        extents = {id(instance): runner.extent(instance) for instance in members}
        pending: dict[int, list[Interval]] = {}
        for instance in members:
            pending[id(instance)] = self._regions(runner, instance, structural.get(instance.view, []), changed)
        own = {port.ref_uri for instance in members for port in instance.outputs.values()}
        while True:
            starts = [(Fraction(spans[0][0]) + schedule.phi[index[key].view], key)
                      for key, spans in pending.items() if spans]
            if not starts:
                break
            low = min(start for start, _ in starts)
            high = low + epsilon
            report.slabs += 1
            produced: dict[str, list[Interval]] = defaultdict(list)
            for key, spans in list(pending.items()):
                instance = index[key]
                phi = schedule.phi[instance.view]
                lo, hi = math.ceil(low - phi), math.ceil(high - phi)
                part = intersect(spans, [(lo, hi)])
                pending[key] = subtract(spans, [(lo, hi)])
                for window in part:
                    try:
                        for stream, times in runner.compute(instance, window).items():
                            produced[stream].extend(merge((t, t + 1) for t in times))
                    except Exception as error:
                        report.errors[instance.id] = f"{type(error).__name__}: {error}"
            for stream, spans in produced.items():
                changed[stream].extend(spans)
                if stream not in own:
                    continue
                for reader_instance, item in runner.readers.get(stream, []):
                    key = id(reader_instance)
                    if key not in pending:
                        continue
                    deployment = runner.deployment(reader_instance)
                    lb, la = deployment.window(item.name)
                    touched = intersect(spans, item.life)
                    region = intersect(expand(touched, lookback=lb, lookahead=la), reader_instance.present)
                    region = runner.clamp(reader_instance, region, extents[key])
                    region = intersect(runner.bucketed(reader_instance, region), reader_instance.present)
                    pending[key] = merge(pending[key] + region)
        written: dict[str, list[Interval]] = defaultdict(list)
        for instance in members:
            for port in instance.outputs.values():
                written[port.ref_uri].extend(instance.present)
        for name in component:
            self._retract_view(runner, name, structural.get(name, []), written, changed)


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
