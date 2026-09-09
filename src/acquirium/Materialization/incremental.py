"""Revision-frontier incremental materialization.

This module is intentionally the whole runtime boundary: declarations compile
to :class:`Binding`, storage constructs coherent :class:`Batch` objects,
and the scheduler commits results with the input frontier in one transaction.
It has no queue or lease state; DuckDB is the recovery authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, Protocol
from threading import Lock
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc


UTC = timezone.utc


def _duration(value: timedelta | str) -> timedelta:
    # Policies are persisted as microseconds, so parse every public spelling
    # here and keep the rest of the scheduler on one comparable type.
    if isinstance(value, timedelta):
        if value < timedelta():
            raise ValueError("durations must not be negative")
        return value
    suffix = value[-2:] if value.endswith("ms") else value[-1:]
    units = {"ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000,
             "d": 86_400_000_000}
    if suffix not in units:
        raise ValueError("durations must use ms, s, m, h, or d")
    try: result = timedelta(microseconds=int(float(value[:-len(suffix)]) * units[suffix]))
    except ValueError as error: raise ValueError(f"invalid duration {value!r}") from error
    if result < timedelta(): raise ValueError("durations must not be negative")
    return result


def parse_lookback(value: timedelta | str) -> timedelta | None:
    """Parse a lookback: a duration, or ``"all"`` for the whole stored extent.

    ``None`` is the internal spelling of ``"all"``; authors never write it.
    """
    if value == "all" or value is None:
        return None
    return _duration(value)


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
    # The unit converter (or a zero-argument factory for one) is injected by
    # whoever builds the set — RevisionStore in the server — so the dependency
    # is visible in the object graph rather than ambient process state.
    converter: Any = field(default=None, repr=False)

    @property
    def stream(self) -> StreamDescriptor:
        """The one stream bound to this alias — which sensor this call is for.

        A ``per_row`` output binds exactly one stream per alias per call —
        for every alias, even when a query row pairs two — so this is the
        usual way to ask what is being computed: ``inputs["temperature"].stream``
        gives its ``ref_uri``, ``point_uri``, ``label`` and ``unit``.

        A ``named`` output sees the whole query result at once, so its aliases
        can hold many streams and this raises. Use :attr:`streams` there; an
        aggregate is about all of them by definition.
        """
        if len(self.streams) != 1:
            raise ValueError(
                f"alias {self.alias!r} is bound to {len(self.streams)} streams, not one — "
                f"a named output sees every match in one call. Use .streams for all of them, "
                f"or a per_row output to run once per match."
            )
        return self.streams[0]

    def batches(self) -> Iterator[pa.RecordBatch]:
        yield from self._table.to_batches(self.batch_size)
    def collect(self) -> pa.Table: return self._table
    def df(self, library: str = "polars") -> Any:
        if library == "polars":
            import polars as pl
            return pl.from_arrow(self._table)
        if library == "pandas": return self._table.to_pandas()
        raise ValueError("library must be 'polars' or 'pandas'")

    def in_unit(self, unit: str) -> "StreamSet":
        """Return this stream set with every value converted into ``unit``.

        Each stream converts from its own recorded unit, so an alias mixing
        Celsius and Fahrenheit sensors comes out uniform. The result is a
        normal :class:`StreamSet`: every accessor and helper works on it.
        """
        converter = self.converter() if callable(self.converter) else self.converter
        if converter is None:
            raise RuntimeError("this stream set carries no unit converter")
        for descriptor in self.streams:
            if descriptor.unit is None:
                raise ValueError(f"stream {descriptor.ref_uri} has no recorded unit to convert from")
        # QUDT conversions are linear, so two probe conversions per stream
        # yield exact factors; the converter raises on incompatible units.
        shifts = {d.ref_uri: converter.convert(0.0, d.unit, unit) for d in self.streams}
        scales = {d.ref_uri: converter.convert(1.0, d.unit, unit) - shifts[d.ref_uri] for d in self.streams}
        def convert(table: pa.Table) -> pa.Table:
            if not (pa.types.is_floating(table["value"].type) or pa.types.is_integer(table["value"].type)):
                raise TypeError("unit conversion requires numeric values")
            refs = [d.ref_uri for d in self.streams]
            index = pc.index_in(table["ref_uri"], pa.array(refs, pa.string()))
            scale = pc.take(pa.array([scales[r] for r in refs], pa.float64()), index)
            shift = pc.take(pa.array([shifts[r] for r in refs], pa.float64()), index)
            value = pc.add(pc.multiply(pc.cast(table["value"], pa.float64()), scale), shift)
            return table.set_column(table.column_names.index("value"), "value", value)
        from dataclasses import replace as _replace
        streams = tuple(_replace(d, unit=unit) for d in self.streams)
        return StreamSet(self.alias, self.window, streams, convert(self._table), convert(self.changes),
                         self.batch_size, self.converter)


def _rows_to_frame(rows: tuple[Mapping[str, Any], ...]) -> Any:
    """Render match rows as a Polars frame, columns in query-alias order."""
    import polars as pl
    if not rows:
        return pl.DataFrame()
    columns: list[str] = []
    for row in rows:
        columns.extend(key for key in row if key not in columns)
    return pl.DataFrame([{key: row.get(key) for key in columns} for row in rows],
                        schema=columns, orient="row", infer_schema_length=None)


@dataclass(frozen=True)
class InputBatch:
    """What one call to ``transform`` is about — the match, not its data.

    ``result`` is everything ``build_query`` matched, the same table in every
    call, so an app can see the fleet it belongs to and not only its own row.
    ``row`` is the one row a ``per_row`` call is computing. The windows say
    why the call happened and how much was read; the revision fields are
    runtime diagnostics. The data itself, and the streams that produced it,
    arrive in the ``inputs`` argument beside this one.
    """
    binding_signature: str
    graph_revision: int
    from_revision: int
    to_revision: int
    changed_window: TimeWindow
    read_window: TimeWindow
    _row: Mapping[str, Any] | None = None
    _result: tuple[Mapping[str, Any], ...] = ()
    output_window: TimeWindow | None = None
    work_id: str | None = None
    work_cursor: str | None = None
    work_next: str | None = None

    @property
    def result(self) -> Any:
        """Everything ``build_query`` matched, as a Polars dataframe.

        The same table in every call of an app, whatever the output flavor:
        a ``per_row`` call sees the whole fleet it is one of, which is what
        lets it group, rank, or count siblings. Columns follow
        ``Query.metadata()``: an alias holds the matched URI, with
        ``<alias>_ref``, ``<alias>.label`` and ``<alias>.unit`` beside a
        stream-bearing one.

        Use :attr:`row` for the one row this call is computing.
        """
        return _rows_to_frame(self._result)

    @property
    def row(self) -> Mapping[str, Any]:
        """The row this call is computing, for a ``per_row`` output.

        Raises for a ``named`` output: that call is about every matched row
        at once, and :attr:`result` is the whole table.
        """
        if self._row is None:
            raise ValueError(
                "this call covers every matched row, so it has no single row — that is what a "
                "named output sees. Use .result for the whole table, or a per_row output to "
                "run once per row."
            )
        return self._row


@dataclass(frozen=True)
class Batch:
    """One unit of work: the loaded inputs and the context describing them."""
    inputs: Mapping[str, StreamSet]
    context: InputBatch


@dataclass(frozen=True)
class OutputSpec:
    """A derived-stream definition: like a driver's stream registration,
    every field is declared, nothing is inferred from published data."""
    value_kind: str
    point_uri: str | None = None
    label: str | None = None
    unit: str | None = None
    quantity_kind: str | None = None
    medium: str | None = None
    substance: str | None = None
    data_source: str | None = None
    properties: Mapping[str, tuple[str, ...]] | None = None
    # ``stream_name`` makes an output absolute: the derived stream keeps this
    # exact reference name instead of one derived from the bound inputs.
    stream_name: str | None = None
    def __post_init__(self):
        if self.value_kind not in ("numeric", "text"):
            raise ValueError("value_kind must be 'numeric' or 'text'")
        if self.stream_name is not None and not self.stream_name:
            raise ValueError("a named output requires a non-empty stream name")


class OutputPort(NamedTuple):
    """One output's resolved durable identity, decided once at planning time.

    ``ref_name`` is what the stream is called under ``derived:<app>``,
    ``ref_uri`` is the storage key that name hashes to, and ``point_uri`` is
    the graph node carrying the output's metadata — the author's own point
    when they declared one, otherwise a point named after the stream itself.
    Everything downstream reads these fields instead of recomputing them, so
    the name and the URI cannot drift apart.
    """
    ref_uri: str
    ref_name: str
    point_uri: str
    spec: OutputSpec


class _OutputAPI:
    """The two output flavors an app can declare.

    ``per_row`` runs the app once per query-result row and publishes one
    derived stream beside that row's inputs — the right choice when the same
    calculation fans out across many matches. A row is one match, not one
    stream: a query pairing the pressure upstream and downstream of a unit
    gives a call holding both, which publishes a single stream for the pair.

    ``named`` publishes one absolute stream whose identity you choose, from a
    single call over the complete query result, so it can be found by name.
    """
    def per_row(self, **kwargs: Any) -> OutputSpec:
        if "stream_name" in kwargs: raise TypeError("per_row outputs derive their name; use output.named(...)")
        return OutputSpec(**kwargs)
    def named(self, stream_name: str, **kwargs: Any) -> OutputSpec:
        return OutputSpec(stream_name=stream_name, **kwargs)
output = _OutputAPI()


class OutputBuilder:
    """Single-assignment, named output collector for one invocation."""
    def __init__(self, ports: Mapping[str, OutputPort]):
        self._ports, self._values = dict(ports), {}
    def __setitem__(self, name: str, value: Any) -> None:
        if name not in self._ports:
            # Every published stream is declared up front, so an unknown port
            # is a typo or a missing declaration, never a new stream.
            declared = ", ".join(repr(port) for port in sorted(self._ports)) or "none"
            raise KeyError(
                f"output {name!r} is not declared in this app's outputs (declared: {declared})"
            )
        if name in self._values: raise ValueError(f"output {name!r} assigned twice")
        try:
            self._values[name] = _normalise_output(value, self._ports[name].spec)
        except (TypeError, ValueError) as error:
            # Schema violations surface inside transform(); the port name is
            # the author's handle on which assignment broke.
            raise type(error)(f"output {name!r}: {error}") from None
    @property
    def values(self) -> Mapping[str, pa.Table]: return self._values


def _normalise_output(value: Any, spec: OutputSpec) -> pa.Table:
    # Apps may use any supported dataframe library, but storage sees
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
    if kind == "numeric" and not (pa.types.is_integer(values.type) or pa.types.is_floating(values.type)):
        raise TypeError("numeric output requires numeric values")
    # Polars hands back large_string; both are the same values to storage.
    if kind == "text" and not (pa.types.is_string(values.type) or pa.types.is_large_string(values.type)):
        raise TypeError("text output requires string values")
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
    outputs: Mapping[str, OutputPort]
    lookback: timedelta | None = timedelta()   # None reads the whole stored extent
    lookahead: timedelta = timedelta()
    graph_revision: int = 0
    parameters: Mapping[str, Any] = field(default_factory=dict)
    row: Mapping[str, Any] | None = None
    result: tuple[Mapping[str, Any], ...] = ()
    generation: str = ""
    every: timedelta | None = None
    signature: str = field(init=False)
    progress_key: str = field(init=False)
    def __post_init__(self):
        if not self.inputs or not self.outputs: raise ValueError("a binding needs inputs and outputs")
        payload = {"v": 1, "application": self.application_name, "executable": self.executable_digest,
                   "inputs": {k: [x.__dict__ for x in sorted(v, key=lambda x: x.ref_uri)] for k,v in sorted(self.inputs.items())},
                   "outputs": {k: (v.ref_uri, v.spec.__dict__) for k,v in sorted(self.outputs.items())},
                   "lookback": "all" if self.lookback is None else self.lookback.total_seconds(),
                   "lookahead": self.lookahead.total_seconds(),
                   "every": self.every.total_seconds() if self.every else None}
        # Keep unconfigured bindings byte-for-byte compatible with their
        # previous identity, while making configured deployments distinct.
        if self.parameters:
            payload["parameters"] = dict(self.parameters)
        object.__setattr__(self, "signature", sha256(_canonical(payload).encode()).hexdigest())
        # Durable progress deliberately survives code and parameter edits: it is
        # keyed by what the binding reads and writes, not by how it computes.
        # Otherwise editing a comment would reset the frontier and, without
        # backfill, silently skip the rows written in between.
        progress = {"v": 1, "application": self.application_name,
                    "inputs": {k: sorted(x.ref_uri for x in v) for k,v in sorted(self.inputs.items())},
                    "outputs": {k: v.ref_uri for k,v in sorted(self.outputs.items())}}
        object.__setattr__(self, "progress_key", sha256(_canonical(progress).encode()).hexdigest())


class ApplicationGraph:
    """Validated compiled binding DAG, deliberately separate from scheduling."""
    def __init__(self, bindings: Iterable[Binding]):
        self.bindings = tuple(bindings)
        owners: dict[str, str] = {}
        for binding in self.bindings:
            for port in binding.outputs.values():
                if port.ref_uri in owners: raise ValueError(f"multiple bindings own {port.ref_uri!r}")
                owners[port.ref_uri] = binding.signature
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


class App:
    """A calculation over the streams selected by one semantic query.

    The ``outputs`` declaration alone decides how query matches become calls:
    a ``per_row`` output runs ``transform`` once per query-result row and
    derives one stream beside each row's inputs, while a ``named`` output
    runs it once over the complete result and owns one absolute stream. The
    two may be declared together only when the query resolves to a single
    input group, where both describe the same call.

    Every knob is a plain attribute holding a duration string, a bool, or
    ``"all"`` — there are no policy objects to learn:

    - ``lookback`` — how much stored context precedes the new data in each
      call's window (``"all"`` reads the whole stream every time).
    - ``lookahead`` — context after the changed range, for corrections
      that land in the middle of history.
    - ``backfill`` — whether the first run processes already-stored history.
    - ``coalesce`` / ``max_delay`` — wait for a quiet gap in a burst of
      writes before running, capped at ``max_delay``.
    - ``min_interval`` — at most one run per interval.
    """
    name: str | None = None
    every: timedelta | str | None = None
    grouping: str = "per_match"
    lookback: timedelta | str = "0s"
    lookahead: timedelta | str = "0s"
    backfill: bool = False
    coalesce: timedelta | str = "0s"
    max_delay: timedelta | str | None = None
    min_interval: timedelta | str | None = None
    outputs: Mapping[str, OutputSpec] = {}
    def build_query(self, plant: Any) -> Any: raise NotImplementedError
    def transform(self, inputs: Mapping[str, StreamSet], output: OutputBuilder, context: InputBatch) -> None: raise NotImplementedError


class RevisionStore:
    """Durable revision-frontier persistence shared by supported stores."""
    def __init__(self, store: Any, unit_converter: Any = None):
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
            if batch is None and previous != target and any(binding.inputs.values()):
                # Revisions for unrelated streams can be safely skipped.  The
                # compare makes this race-safe with an in-flight invocation.
                with self.store._lock, self.store._write_conn() as writer:
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
        else:
            window = output_window or self._output_window(binding, changed_window)
            read = TimeWindow(window.start - binding.lookback, window.end + binding.lookahead)
        if output_window is not None and binding.lookback is None:
            window = output_window
        inputs = {alias: self._stream_set(conn, alias, descriptors, read, previous, target)
                  for alias, descriptors in binding.inputs.items()}
        return Batch(inputs, InputBatch(binding.signature, binding.graph_revision, previous,
                                        target, changed_window, read, binding.row, binding.result, window))

    @staticmethod
    def _output_window(binding: Binding, changed: TimeWindow) -> TimeWindow:
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
        rows = self._execute(conn, query, [*refs, self._time(window.start), self._time(window.end)]).fetchall()
        numeric = all(row[2] is not None or row[3] is None for row in rows)
        schema = pa.float64() if numeric else pa.string()
        table = pa.table({"ref_uri": pa.array([x[0] for x in rows], pa.string()), "time": pa.array([(x[1].replace(tzinfo=UTC) if x[1].tzinfo is None else x[1].astimezone(UTC)) for x in rows], pa.timestamp("us",tz="UTC")), "value": pa.array([x[2] if numeric else (x[3] if x[3] is not None else str(x[2])) for x in rows], schema)})
        # ``table`` is the complete read window; ``changes`` is only the rows
        # advanced by this batch. Windowed transformations often need both.
        changed = table.filter(pa.array([previous < row[4] <= target for row in rows], type=pa.bool_()))
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


class Executor(Protocol):
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]: ...


class InProcessExecutor:
    """Deterministic executor useful for tests; it has the same task boundary."""
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]:
        output = OutputBuilder(ports)
        application.transform(batch.inputs, output, batch.context)
        return output.values


class Scheduler:
    """A persistent bounded executor, with failures recorded per binding."""
    def __init__(self, store: RevisionStore, executor: Executor | None = None, *, max_workers: int = 2):
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self.store, self.executor = store, executor or InProcessExecutor()
        self.capacity = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="acquirium-materialize")
        self.errors: dict[str, str] = {}
        self._run_lock = Lock()

    def close(self) -> None:
        self._pool.shutdown(wait=True)

    def run_once(self, binding: Binding, application: App) -> bool:
        self.store.initialise(binding, application.backfill)
        batch = self.store.next_batch(binding)
        if batch is None:
            return False
        results = self.executor.execute(application, batch, binding.outputs)
        return self.store.commit(binding, batch, results)

    def run_layer(self, bindings: Iterable[Binding], applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
        # A chunk bounds both loaded batches and pending futures. All successes
        # in it publish together before dependent work can be scheduled.
        capacity = self.capacity if max_workers is None else min(self.capacity, max_workers)
        if capacity < 1:
            raise ValueError("max_workers must be positive")
        wave, ran = tuple(bindings), False
        with self._run_lock:
            for offset in range(0, len(wave), capacity):
                pending = []
                for binding in wave[offset:offset + capacity]:
                    self.errors.pop(binding.signature, None)
                    try:
                        self.store.initialise(binding, applications[binding.signature].backfill)
                        batch = self.store.next_batch(binding)
                        if batch is not None:
                            future = self._pool.submit(self.executor.execute, applications[binding.signature], batch, binding.outputs)
                            pending.append((binding, batch, future))
                    except Exception as error:
                        self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                completed = []
                for binding, batch, future in pending:
                    try:
                        completed.append((binding, batch, future.result()))
                    except Exception as error:
                        self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                ran = any(self.store.commit_wave(completed).values()) or ran
        return ran

    def run_graph_once(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
        ran, blocked = False, set()
        for wave in graph.layers():
            blocked.update(target for source, target, _ in graph.edges if source in blocked)
            ready = [b for b in wave if b.signature not in blocked]
            ran = self.run_layer(ready, applications, max_workers=max_workers) or ran
            blocked.update(b.signature for b in ready if b.signature in self.errors)
        return ran

    def run_until_idle(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> None:
        while self.run_graph_once(graph, applications, max_workers=max_workers):
            pass


def align(inputs: Mapping[str, StreamSet], every: timedelta | str, *, aggregate: str = "mean") -> Any:
    """Resample every input onto one shared clock and return a wide dataframe.

    The result has a ``time`` column plus one column per stream: an alias with
    a single bound stream contributes a column named after the alias, and an
    alias with several contributes ``alias[label-or-ref]`` columns. Buckets a
    stream never reported in hold nulls; combining differently sampled sensors
    is then one join instead of a hand-rolled resample per stream.
    """
    import polars as pl
    step = _duration(every)
    if step <= timedelta(): raise ValueError("align requires a positive bucket size")
    aggregates = {"mean": pl.col("value").mean(), "min": pl.col("value").min(), "max": pl.col("value").max(),
                  "sum": pl.col("value").sum(), "first": pl.col("value").first(), "last": pl.col("value").last(),
                  "median": pl.col("value").median(), "count": pl.col("value").count()}
    if aggregate not in aggregates: raise ValueError(f"aggregate must be one of {sorted(aggregates)}")
    columns: list[pl.DataFrame] = []
    for alias, stream_set in sorted(inputs.items()):
        frame = pl.from_arrow(stream_set.collect())
        labels = {d.ref_uri: d.label or d.ref_uri for d in stream_set.streams}
        for ref, group in sorted(frame.group_by("ref_uri"), key=lambda item: str(item[0][0])):
            name = alias if len(stream_set.streams) <= 1 else f"{alias}[{labels.get(str(ref[0]), str(ref[0]))}]"
            columns.append(group.sort("time")
                .group_by_dynamic("time", every=step).agg(aggregates[aggregate].alias(name)))
    if not columns:
        return pl.DataFrame({"time": pl.Series([], dtype=pl.Datetime("us", "UTC"))})
    result = columns[0]
    for column in columns[1:]:
        result = result.join(column, on="time", how="full", coalesce=True)
    return result.sort("time")
