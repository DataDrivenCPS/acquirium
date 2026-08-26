"""Revision-frontier incremental materialization.

This module is intentionally the whole runtime boundary: declarations compile
to :class:`Binding`, storage constructs coherent :class:`InputBatch` objects,
and the scheduler commits results with the input frontier in one transaction.
It has no queue or lease state; DuckDB is the recovery authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
from time import time
from typing import Any, Iterable, Iterator, Mapping, Protocol
from threading import Lock

import pyarrow as pa
import pyarrow.compute as pc

from acquirium.internals.models import compute_ref_uri

UTC = timezone.utc


def _duration(value: timedelta | str) -> timedelta:
    # Policies are persisted as microseconds, so parse every public spelling
    # here and keep the rest of the scheduler on one comparable type.
    if isinstance(value, timedelta):
        return value
    suffix = value[-2:] if value.endswith("ms") else value[-1:]
    units = {"ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000}
    if suffix not in units:
        raise ValueError("durations must use ms, s, m, or h")
    try: result = timedelta(microseconds=int(float(value[:-len(suffix)]) * units[suffix]))
    except ValueError as error: raise ValueError(f"invalid duration {value!r}") from error
    if result < timedelta(): raise ValueError("durations must not be negative")
    return result


@dataclass(frozen=True)
class OnChange:
    coalesce: timedelta | str = "0ms"
    max_delay: timedelta | str | None = None
    def __post_init__(self):
        object.__setattr__(self, "coalesce", _duration(self.coalesce))
        if self.max_delay is not None:
            object.__setattr__(self, "max_delay", _duration(self.max_delay))


@dataclass(frozen=True)
class Every:
    interval: timedelta | str
    def __post_init__(self): object.__setattr__(self, "interval", _duration(self.interval))


@dataclass(frozen=True)
class Changed: pass


@dataclass(frozen=True)
class AroundChange:
    before: timedelta | str = timedelta()
    after: timedelta | str = timedelta()
    def __post_init__(self):
        object.__setattr__(self, "before", _duration(self.before))
        object.__setattr__(self, "after", _duration(self.after))


@dataclass(frozen=True)
class AllAvailable: pass


@dataclass(frozen=True)
class Current: pass


@dataclass(frozen=True)
class StreamDescriptor:
    ref_uri: str
    point_uri: str | None = None
    label: str | None = None
    value_kind: str = "numeric"
    unit: str | None = None
    quantity_kind: str | None = None
    medium: str | None = None
    substance: str | None = None
    properties: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime
    def __post_init__(self):
        for name in ("start", "end"):
            value = getattr(self, name)
            if value.tzinfo is None: value = value.replace(tzinfo=UTC)
            object.__setattr__(self, name, value.astimezone(UTC))
        if self.end < self.start: raise ValueError("window end precedes start")


def _empty_table() -> pa.Table:
    return pa.table({"ref_uri": pa.array([], pa.string()), "time": pa.array([], pa.timestamp("us", tz="UTC")), "value": pa.array([], pa.float64())})


@dataclass(frozen=True)
class StreamSet:
    alias: str
    window: TimeWindow
    streams: tuple[StreamDescriptor, ...]
    _table: pa.Table = field(default_factory=_empty_table)
    changes: pa.Table = field(default_factory=_empty_table)
    batch_size: int = 65_536

    def batches(self) -> Iterator[pa.RecordBatch]:
        yield from self._table.to_batches(self.batch_size)
    def collect(self) -> pa.Table: return self._table
    def df(self, library: str = "polars") -> Any:
        if library == "polars":
            import polars as pl
            return pl.from_arrow(self._table)
        if library == "pandas": return self._table.to_pandas()
        raise ValueError("library must be 'polars' or 'pandas'")


@dataclass(frozen=True)
class InputBatch:
    binding_signature: str
    graph_revision: int
    from_revision: int
    to_revision: int
    changed_window: TimeWindow
    read_window: TimeWindow
    inputs: Mapping[str, StreamSet]


@dataclass(frozen=True)
class OutputSpec:
    value_kind: str | None = None
    point_uri: str | None = None
    label: str | None = None
    unit: str | None = None
    quantity_kind: str | None = None
    medium: str | None = None
    substance: str | None = None
    data_source: str | None = None
    properties: Mapping[str, tuple[str, ...]] | None = None
    inherit: bool = False
    inherit_properties: tuple[str, ...] = ()
    def __post_init__(self):
        if self.value_kind not in (None, "numeric", "text"):
            raise ValueError("value_kind must be numeric, text, or None")


class _Outputs:
    def stream(self, **kwargs: Any) -> OutputSpec: return OutputSpec(**kwargs)
outputs = _Outputs()


class OutputBuilder:
    """Single-assignment, named output collector for one invocation."""
    def __init__(self, ports: Mapping[str, tuple[str, OutputSpec]]):
        self._ports, self._values = dict(ports), {}
    def __setitem__(self, name: str, value: Any) -> None:
        if name not in self._ports: raise KeyError(f"undeclared output {name!r}")
        if name in self._values: raise ValueError(f"output {name!r} assigned twice")
        self._values[name] = _normalise_output(value, self._ports[name][1])
    @property
    def values(self) -> Mapping[str, pa.Table]: return self._values


def _normalise_output(value: Any, spec: OutputSpec) -> pa.Table:
    # Transformations may use any supported dataframe library, but storage sees
    # one canonical Arrow shape. Validate before casting so a bad output cannot
    # silently change a stream's registered value kind.
    if isinstance(value, pa.RecordBatch): value = pa.Table.from_batches([value])
    elif isinstance(value, (list, tuple)) and all(isinstance(x, pa.RecordBatch) for x in value): value = pa.Table.from_batches(value)
    if not isinstance(value, pa.Table):
        try:
            import polars as pl
            if isinstance(value, pl.DataFrame): value = value.to_arrow()
        except ImportError: pass
    if not isinstance(value, pa.Table):
        try:
            import pandas as pd
            if isinstance(value, pd.DataFrame): value = pa.Table.from_pandas(value, preserve_index=False)
        except ImportError: pass
    if not isinstance(value, pa.Table) or set(value.column_names) != {"time", "value"}:
        raise TypeError("an output must be an Arrow/Polars/pandas table with exactly time and value columns")
    time, values = value["time"], value["value"]
    if time.null_count or not pa.types.is_timestamp(time.type) or time.type.tz is None:
        raise ValueError("output time must be non-null timezone-aware timestamps")
    time = pc.cast(time, pa.timestamp("us", tz="UTC"))
    if values.null_count: raise ValueError("output value must be non-null")
    kind = spec.value_kind
    if kind is None: kind = "numeric" if pa.types.is_integer(values.type) or pa.types.is_floating(values.type) else "text"
    if kind == "numeric" and not (pa.types.is_integer(values.type) or pa.types.is_floating(values.type)):
        raise TypeError("numeric output requires numeric values")
    if kind == "text" and not pa.types.is_string(values.type): raise TypeError("text output requires string values")
    result = pa.table({"time": time, "value": pc.cast(values, pa.float64() if kind == "numeric" else pa.string())})
    # A correction is identified by (stream, time); duplicates would make the
    # publication order-dependent.
    if len(set(result["time"].to_pylist())) != result.num_rows: raise ValueError("output timestamps must be unique")
    return result.sort_by([("time", "ascending")])


def _canonical(value: object) -> str: return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

@dataclass(frozen=True)
class Binding:
    application_name: str
    executable_digest: str
    inputs: Mapping[str, tuple[StreamDescriptor, ...]]
    outputs: Mapping[str, tuple[str, OutputSpec]]
    window: Changed | AroundChange | AllAvailable = field(default_factory=Changed)
    graph_revision: int = 0
    parameters: Mapping[str, Any] = field(default_factory=dict)
    signature: str = field(init=False)
    def __post_init__(self):
        if not self.inputs or not self.outputs: raise ValueError("a binding needs inputs and outputs")
        payload = {"v": 1, "application": self.application_name, "executable": self.executable_digest,
                   "inputs": {k: [x.__dict__ for x in sorted(v, key=lambda x: x.ref_uri)] for k,v in sorted(self.inputs.items())},
                   "outputs": {k: (v[0], v[1].__dict__) for k,v in sorted(self.outputs.items())},
                   "window": {"kind": type(self.window).__name__, **self.window.__dict__}}
        # Keep unconfigured bindings byte-for-byte compatible with their
        # previous identity, while making configured deployments distinct.
        if self.parameters:
            payload["parameters"] = dict(self.parameters)
        object.__setattr__(self, "signature", sha256(_canonical(payload).encode()).hexdigest())

    @classmethod
    def derive_output_ref_name(cls, key: str, inputs: Mapping[str, Iterable[StreamDescriptor]]) -> str:
        # Output identities follow bound inputs, not a deployment instance.
        # Recompiling the same graph therefore reuses the derived stream.
        pairs = sorted((alias, item.ref_uri) for alias, values in inputs.items() for item in values)
        digest = sha256(_canonical([key, pairs]).encode()).hexdigest()
        return f"{key}:{digest}"

    @classmethod
    def derive_output_uri(cls, application_name: str, key: str, inputs: Mapping[str, Iterable[StreamDescriptor]]) -> str:
        """Return the graph-registry URI for one deterministic derived port."""
        return str(compute_ref_uri(
            f"derived:{application_name}", cls.derive_output_ref_name(key, inputs)
        ))

    def output_ref_name(self, key: str) -> str:
        """Return the registry name corresponding to one output port."""
        return self.derive_output_ref_name(key, self.inputs)


class ApplicationGraph:
    """Validated compiled binding DAG, deliberately separate from scheduling."""
    def __init__(self, bindings: Iterable[Binding]):
        self.bindings = tuple(bindings)
        owners: dict[str, str] = {}
        for binding in self.bindings:
            for ref_uri, _ in binding.outputs.values():
                if ref_uri in owners: raise ValueError(f"multiple bindings own {ref_uri!r}")
                owners[ref_uri] = binding.signature
        self.edges = tuple(sorted((owners[d.ref_uri], binding.signature, d.ref_uri)
            for binding in self.bindings for streams in binding.inputs.values() for d in streams
            if d.ref_uri in owners))
        # A binding never reads its own output in the same revision frontier.
        if any(source == target for source, target, _ in self.edges):
            raise ValueError("an application binding cannot consume its own output")
        self._assert_acyclic()
    def _assert_acyclic(self) -> None:
        children: dict[str, set[str]] = {b.signature: set() for b in self.bindings}
        for source, target, _ in self.edges: children[source].add(target)
        visiting, visited = set(), set()
        def visit(node: str) -> None:
            if node in visiting: raise ValueError("application bindings contain a cycle")
            if node not in visited:
                visiting.add(node)
                for child in children[node]: visit(child)
                visiting.remove(node); visited.add(node)
        for node in children: visit(node)
    def topological(self) -> tuple[Binding, ...]:
        by_id = {b.signature: b for b in self.bindings}; incoming = {key: 0 for key in by_id}
        children: dict[str, list[str]] = {key: [] for key in by_id}
        for source, target, _ in self.edges: incoming[target] += 1; children[source].append(target)
        ready = sorted(key for key, count in incoming.items() if not count); order = []
        while ready:
            key = ready.pop(0); order.append(by_id[key])
            for child in sorted(children[key]):
                incoming[child] -= 1
                if not incoming[child]: ready.append(child); ready.sort()
        return tuple(order)

    def layers(self) -> tuple[tuple[Binding, ...], ...]:
        """Return dependency-respecting waves of independently runnable bindings."""
        by_id = {binding.signature: binding for binding in self.bindings}
        incoming = {signature: 0 for signature in by_id}
        children: dict[str, list[str]] = {signature: [] for signature in by_id}
        for source, target, _ in self.edges:
            incoming[target] += 1
            children[source].append(target)
        ready = sorted(signature for signature, count in incoming.items() if count == 0)
        layers = []
        while ready:
            wave = ready
            layers.append(tuple(by_id[signature] for signature in wave))
            ready = []
            for source in wave:
                for target in sorted(children[source]):
                    incoming[target] -= 1
                    if incoming[target] == 0:
                        ready.append(target)
            ready.sort()
        return tuple(layers)


class Transformation:
    """A transformation bound once to the complete semantic query result."""
    name: str | None = None
    binding_mode = "full_query"
    trigger: OnChange | Every = OnChange()
    window: Changed | AroundChange | AllAvailable = Changed()
    outputs: Mapping[str, OutputSpec] = {}
    start: Current | AllAvailable = Current()
    def build_query(self, aq: Any) -> Any: raise NotImplementedError
    def transform(self, inputs: Mapping[str, StreamSet], output: OutputBuilder, context: InputBatch) -> None: raise NotImplementedError


class RowWiseTransformation(Transformation):
    """A transformation bound independently to each semantic query result row."""
    binding_mode = "per_row"


class RevisionStore:
    """Durable revision-frontier persistence shared by supported stores."""
    def __init__(self, store: Any): self.store = store

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
    def initialise(self, binding: Binding, start: Current | AllAvailable) -> int:
        with self.store._lock, self.store._write_conn() as conn:
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE binding_signature=?", [binding.signature]).fetchone()
            if row is not None: return int(row[0])
            current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            # Current opts into future changes; AllAvailable deliberately
            # replays history for a newly seen binding.
            consumed = current if isinstance(start, Current) else 0
            self._execute(conn, "INSERT INTO binding_progress VALUES (?, ?)", [binding.signature, consumed])
            return consumed
    def next_batch(self, binding: Binding) -> InputBatch | None:
        # One read transaction is the snapshot boundary described by proposal.
        conn = self.store._connect()
        try:
            # DuckDB exposes ``begin()`` while psycopg starts an explicit
            # snapshot transaction through SQL.
            if self._postgres:
                conn.execute("BEGIN")
            else:
                conn.begin()
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE binding_signature=?", [binding.signature]).fetchone()
            if row is None: raise KeyError("binding was not initialised")
            previous = int(row[0]); target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            refs = [d.ref_uri for values in binding.inputs.values() for d in values]
            if not refs or target == previous: conn.commit(); return None
            marks = ",".join("?" for _ in refs)
            changed = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.last_revision>? AND t.last_revision<=?", [*refs, previous, target]).fetchone()
            if changed[0] is None:
                conn.commit()
                # Revisions for unrelated streams can be safely skipped.  The
                # compare makes this race-safe with an in-flight invocation.
                with self.store._lock, self.store._write_conn() as writer:
                    self._execute(writer, "UPDATE binding_progress SET consumed_revision=? WHERE binding_signature=? AND consumed_revision=?", [target, binding.signature, previous])
                return None
            changed_window = TimeWindow(changed[0].replace(tzinfo=UTC) if changed[0].tzinfo is None else changed[0], changed[1].replace(tzinfo=UTC) if changed[1].tzinfo is None else changed[1])
            if isinstance(binding.window, AllAvailable):
                extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND NOT t.deleted", refs).fetchone()
                read = TimeWindow(extent[0].replace(tzinfo=UTC) if extent[0].tzinfo is None else extent[0], extent[1].replace(tzinfo=UTC) if extent[1].tzinfo is None else extent[1])
            elif isinstance(binding.window, AroundChange): read = TimeWindow(changed_window.start-binding.window.before, changed_window.end+binding.window.after)
            else: read = changed_window
            inputs = {alias: self._stream_set(conn, alias, descriptors, read, previous, target) for alias, descriptors in binding.inputs.items()}
            conn.commit()
            return InputBatch(binding.signature, binding.graph_revision, previous, target, changed_window, read, inputs)
        except BaseException:
            conn.rollback(); raise
        finally: conn.close()
    def _stream_set(self, conn: Any, alias: str, descriptors: tuple[StreamDescriptor,...], window: TimeWindow, previous: int, target: int) -> StreamSet:
        refs = [x.ref_uri for x in descriptors]
        if not refs: return StreamSet(alias, window, descriptors)
        marks = ",".join("?" for _ in refs)
        query = f"""SELECT {self._ref},t.ts,t.numeric_value,t.text_value,t.last_revision FROM {self._timeseries_source}
                    WHERE {self._ref} IN ({marks}) AND NOT t.deleted AND t.ts>=? AND t.ts<=? ORDER BY {self._ref},t.ts"""
        rows = self._execute(conn, query, [*refs, self._time(window.start), self._time(window.end)]).fetchall()
        numeric = all(row[2] is not None or row[3] is None for row in rows)
        schema = pa.float64() if numeric else pa.string()
        table = pa.table({"ref_uri": pa.array([x[0] for x in rows], pa.string()), "time": pa.array([(x[1].replace(tzinfo=UTC) if x[1].tzinfo is None else x[1].astimezone(UTC)) for x in rows], pa.timestamp("us",tz="UTC")), "value": pa.array([x[2] if numeric else (x[3] if x[3] is not None else str(x[2])) for x in rows], schema)})
        # ``table`` is the complete read window; ``changes`` is only the rows
        # advanced by this batch. Windowed transformations often need both.
        changed = table.filter(pa.array([previous < row[4] <= target for row in rows]))
        return StreamSet(alias, window, descriptors, table, changed)
    def commit(self, binding: Binding, batch: InputBatch, results: Mapping[str, pa.Table]) -> bool:
        return self.commit_wave(((binding, batch, results),)).get(binding.signature, False)

    def commit_wave(self, commits: Iterable[tuple[Binding, InputBatch, Mapping[str, pa.Table]]]) -> Mapping[str, bool]:
        """Commit independent completed work in one revision transaction."""
        completed = tuple(commits)
        if not completed:
            return {}
        with self.store._lock, self.store._write_conn() as conn:
            accepted = []
            for binding, batch, results in completed:
                row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE binding_signature=?", [binding.signature]).fetchone()
                if row is not None and int(row[0]) == batch.from_revision:
                    accepted.append((binding, batch, results))
            # Commit the full wave atomically. A dependent layer can then see
            # either all of its predecessors' outputs or none of them.
            nonempty = [(binding, name, table) for binding, _, results in accepted
                        for name, table in results.items() if table.num_rows]
            if nonempty:
                revision = self.store._next_revision(conn)
                records = []
                for binding, name, table in nonempty:
                    ref, spec = binding.outputs[name]
                    kind = spec.value_kind or ("numeric" if pa.types.is_floating(table["value"].type) or pa.types.is_integer(table["value"].type) else "text")
                    # Framework-owned reference metadata is registered in the
                    # same transaction as the first derived values.  User code
                    # never chooses this identity.
                    self._execute(conn, f"""INSERT INTO streams (ref_uri, point_uri, source_id, ref_name, value_kind)
                        VALUES (?, ?, ?, ?, ?) ON CONFLICT (ref_uri) DO NOTHING""",
                        [ref, spec.point_uri, f"derived:{binding.application_name}", binding.output_ref_name(name), kind])
                    for time, value in zip(table["time"].to_pylist(), table["value"].to_pylist()):
                        records.append((ref, self._time(time), float(value) if kind == "numeric" else None, str(value) if kind == "text" else None, revision))
                if records:
                    timestamp_type = pa.timestamp("us", tz="UTC") if self._postgres else pa.timestamp("us")
                    incoming = pa.table({"ref_uri":[x[0] for x in records], "ts":pa.array([x[1] for x in records], timestamp_type), "numeric_value":[x[2] for x in records], "text_value":[x[3] for x in records]})
                    import polars as pl
                    self.store._insert_frame(conn, pl.from_arrow(incoming), revision)
            for binding, batch, _ in accepted:
                self._execute(conn, "UPDATE binding_progress SET consumed_revision=? WHERE binding_signature=?", [batch.to_revision, binding.signature])
            return {binding.signature: True for binding, _, _ in accepted}


class Executor(Protocol):
    def execute(self, application: Transformation, batch: InputBatch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]: ...


class InProcessExecutor:
    """Deterministic executor useful for tests; it has the same task boundary."""
    def execute(self, application: Transformation, batch: InputBatch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]:
        output = OutputBuilder(ports)
        application.transform(batch.inputs, output, batch)
        return output.values


@dataclass(frozen=True)
class _TaskResult:
    outputs: Mapping[str, pa.Table]
    started_at: float
    finished_at: float


def _ray_transform(application: Transformation, batch: InputBatch,
                   ports: Mapping[str, tuple[str, OutputSpec]]) -> _TaskResult:
    started_at = time()
    outputs = InProcessExecutor().execute(application, batch, ports)
    return _TaskResult(outputs, started_at, time())


class RayExecutor:
    """Disposable, single-node Ray task substrate for sealed Arrow batches."""
    def __init__(self, *, num_cpus: int | None = None) -> None:
        import ray
        self._ray = ray
        self._owns_cluster = not ray.is_initialized()
        if self._owns_cluster:
            options: dict[str, Any] = {"ignore_reinit_error": True, "include_dashboard": False}
            if num_cpus is not None:
                if num_cpus < 1:
                    raise ValueError("Ray worker capacity must be positive")
                options["num_cpus"] = num_cpus
            ray.init(**options)
        self._task = ray.remote(_ray_transform)

    @property
    def worker_capacity(self) -> float:
        """CPU task capacity advertised by the active local Ray cluster."""
        return float(self._ray.cluster_resources().get("CPU", 0))

    def close(self) -> None:
        """Stop the local cluster only when this executor started it."""
        if self._owns_cluster and self._ray.is_initialized():
            self._ray.shutdown()

    def submit(self, application: Transformation, batch: InputBatch,
               ports: Mapping[str, tuple[str, OutputSpec]]) -> Any:
        """Submit work without waiting, returning Ray's dependency token."""
        # Arrow-bearing batches are put once in Ray's object store; retries and
        # object references are intentionally not durable coordination state.
        sealed = self._ray.put(batch)
        return self._task.remote(application, sealed, ports)

    def resolve(self, ticket: Any) -> _TaskResult:
        return self._ray.get(ticket)

    def execute(self, application: Transformation, batch: InputBatch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]:
        return self.resolve(self.submit(application, batch, ports)).outputs


@dataclass
class _Invocation:
    """A claimed durable batch held until its result is committed or discarded."""
    binding: Binding
    batch: InputBatch
    lock: Any


class Scheduler:
    """Coordinates durable batches; independent graph layers may run in parallel."""
    def __init__(self, store: RevisionStore, executor: Executor | None = None):
        self.store, self.executor = store, executor or RayExecutor()
        self._locks: dict[str, Lock] = {}
        self._locks_guard = Lock()
    def run_once(self, binding: Binding, application: Transformation) -> bool:
        invocation = self._prepare(binding, application)
        if invocation is None:
            return False
        try:
            return self._complete(invocation, self.executor.execute(application, invocation.batch, binding.outputs))
        finally:
            invocation.lock.release()

    def _prepare(self, binding: Binding, application: Transformation) -> _Invocation | None:
        with self._locks_guard:
            lock = self._locks.setdefault(binding.signature, Lock())
        # This lock only avoids duplicate local work; the durable frontier
        # comparison remains the correctness guard after a restart.
        if not lock.acquire(blocking=False): return None
        try:
            self.store.initialise(binding, application.start)
            batch = self.store.next_batch(binding)
            if batch is None:
                lock.release()
                return None
            return _Invocation(binding, batch, lock)
        except BaseException:
            lock.release()
            raise

    def _complete(self, invocation: _Invocation, results: Mapping[str, pa.Table]) -> bool:
        return self.store.commit(invocation.binding, invocation.batch, results)

    def run_layer(self, bindings: Iterable[Binding], applications: Mapping[str, Transformation], *, max_workers: int | None = None) -> bool:
        """Run one topological wave and wait for every durable commit.

        The executor work overlaps (Ray tasks in production), while each
        commit remains protected by :class:`RevisionStore`. Callers advance to
        a dependent wave only after this method returns.
        """
        wave = tuple(bindings)
        if not wave:
            return False
        if max_workers is not None and max_workers < 1:
            raise ValueError("max_workers must be positive")
        submit, resolve = getattr(self.executor, "submit", None), getattr(self.executor, "resolve", None)
        if callable(submit) and callable(resolve):
            return self._run_async_layer(wave, applications, submit, resolve)
        workers = min(len(wave), max_workers or len(wave))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="acquirium-materialize") as pool:
            futures = [pool.submit(self.run_once, binding, applications[binding.signature]) for binding in wave]
            results = [future.result() for future in futures]
            return any(results)

    def _run_async_layer(self, wave: tuple[Binding, ...], applications: Mapping[str, Transformation], submit: Any, resolve: Any) -> bool:
        """Submit a whole dependency wave before waiting on any Ray result."""
        pending: list[tuple[_Invocation, Any]] = []
        try:
            for binding in wave:
                application = applications[binding.signature]
                invocation = self._prepare(binding, application)
                if invocation is not None:
                    try:
                        pending.append((invocation, submit(application, invocation.batch, binding.outputs)))
                    except BaseException:
                        invocation.lock.release()
                        raise
            completed, failure = [], None
            for invocation, ticket in pending:
                try:
                    result = resolve(ticket)
                    outputs = result.outputs if isinstance(result, _TaskResult) else result
                    completed.append((invocation.binding, invocation.batch, outputs))
                except BaseException as error:
                    # Resolve every already-submitted task before reporting the
                    # first failure, so no work or binding lock is abandoned.
                    failure = failure or error
            if failure is not None:
                raise failure
            return any(self.store.commit_wave(completed).values())
        finally:
            for invocation, _ in pending:
                if invocation.lock.locked():
                    invocation.lock.release()

    def run_graph_once(self, graph: ApplicationGraph, applications: Mapping[str, Transformation], *, max_workers: int | None = None) -> bool:
        """Run every dependency wave once, committing each wave before the next."""
        ran = False
        for wave in graph.layers():
            ran = self.run_layer(wave, applications, max_workers=max_workers) or ran
        return ran

    def run_until_idle(self, graph: ApplicationGraph, applications: Mapping[str, Transformation], *, max_workers: int | None = None) -> None:
        """Drive a DAG to its latest canonical state without durable queue state."""
        while self.run_graph_once(graph, applications, max_workers=max_workers):
            pass
