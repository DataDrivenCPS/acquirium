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
    def __init__(self, ports: Mapping[str, tuple[str, OutputSpec]]):
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
            self._values[name] = _normalise_output(value, self._ports[name][1])
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
    lookback: timedelta | None = timedelta()   # None reads the whole stored extent
    lookahead: timedelta = timedelta()
    graph_revision: int = 0
    parameters: Mapping[str, Any] = field(default_factory=dict)
    row: Mapping[str, Any] | None = None
    result: tuple[Mapping[str, Any], ...] = ()
    signature: str = field(init=False)
    progress_key: str = field(init=False)
    def __post_init__(self):
        if not self.inputs or not self.outputs: raise ValueError("a binding needs inputs and outputs")
        payload = {"v": 1, "application": self.application_name, "executable": self.executable_digest,
                   "inputs": {k: [x.__dict__ for x in sorted(v, key=lambda x: x.ref_uri)] for k,v in sorted(self.inputs.items())},
                   "outputs": {k: (v[0], v[1].__dict__) for k,v in sorted(self.outputs.items())},
                   "lookback": "all" if self.lookback is None else self.lookback.total_seconds(),
                   "lookahead": self.lookahead.total_seconds()}
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
                    "outputs": {k: v[0] for k,v in sorted(self.outputs.items())}}
        object.__setattr__(self, "progress_key", sha256(_canonical(progress).encode()).hexdigest())

    @classmethod
    def derive_output_ref_name(cls, key: str, inputs: Mapping[str, Iterable[StreamDescriptor]],
                               spec: OutputSpec | None = None) -> str:
        # A named output owns its exact reference name. A per-row identity
        # follows the bound inputs, not a deployment instance, so recompiling
        # the same graph reuses the derived stream.
        if spec is not None and spec.stream_name is not None:
            return spec.stream_name
        pairs = sorted((alias, item.ref_uri) for alias, values in inputs.items() for item in values)
        digest = sha256(_canonical([key, pairs]).encode()).hexdigest()
        return f"{key}:{digest}"

    @classmethod
    def derive_output_uri(cls, application_name: str, key: str, inputs: Mapping[str, Iterable[StreamDescriptor]],
                          spec: OutputSpec | None = None) -> str:
        """Return the graph-registry URI for one deterministic derived port."""
        return str(compute_ref_uri(
            f"derived:{application_name}", cls.derive_output_ref_name(key, inputs, spec)
        ))

    def output_ref_name(self, key: str) -> str:
        """Return the registry name corresponding to one output port."""
        return self.derive_output_ref_name(key, self.inputs, self.outputs[key][1])


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
    def initialise(self, binding: Binding, backfill: bool = False) -> int:
        with self.store._lock, self.store._write_conn() as conn:
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is not None: return int(row[0])
            current = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            # Backfill deliberately replays retained history for a newly seen
            # binding; otherwise only future changes are processed.
            consumed = 0 if backfill else current
            self._execute(conn, "INSERT INTO binding_progress VALUES (?, ?)", [binding.progress_key, consumed])
            return consumed
    def next_batch(self, binding: Binding) -> Batch | None:
        # One read transaction is the snapshot boundary described by proposal.
        conn = self.store._connect()
        try:
            # DuckDB exposes ``begin()`` while psycopg starts an explicit
            # snapshot transaction through SQL.
            if self._postgres:
                conn.execute("BEGIN")
            else:
                conn.begin()
            row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            if row is None: raise KeyError("binding was not initialised")
            previous = int(row[0]); target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            batch = self._build_batch(conn, binding, previous, target)
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
            if self._postgres: conn.execute("BEGIN")
            else: conn.begin()
            target = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            batch = self._build_batch(conn, binding, 0, target)
            conn.commit()
            return batch
        except BaseException:
            conn.rollback(); raise
        finally: conn.close()

    def _build_batch(self, conn: Any, binding: Binding, previous: int, target: int) -> Batch | None:
        """Read one coherent batch for the revisions in ``(previous, target]``."""
        refs = [d.ref_uri for values in binding.inputs.values() for d in values]
        if not refs or target == previous: return None
        marks = ",".join("?" for _ in refs)
        changed = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND t.last_revision>? AND t.last_revision<=?", [*refs, previous, target]).fetchone()
        if changed[0] is None: return None
        changed_window = TimeWindow(changed[0].replace(tzinfo=UTC) if changed[0].tzinfo is None else changed[0], changed[1].replace(tzinfo=UTC) if changed[1].tzinfo is None else changed[1])
        if binding.lookback is None:
            extent = self._execute(conn, f"SELECT min(t.ts), max(t.ts) FROM {self._timeseries_source} WHERE {self._ref} IN ({marks}) AND NOT t.deleted", refs).fetchone()
            read = TimeWindow(extent[0].replace(tzinfo=UTC) if extent[0].tzinfo is None else extent[0], extent[1].replace(tzinfo=UTC) if extent[1].tzinfo is None else extent[1])
        else: read = TimeWindow(changed_window.start-binding.lookback, changed_window.end+binding.lookahead)
        inputs = {alias: self._stream_set(conn, alias, descriptors, read, previous, target) for alias, descriptors in binding.inputs.items()}
        return Batch(inputs, InputBatch(binding.signature, binding.graph_revision, previous,
                                       target, changed_window, read, binding.row, binding.result))
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
        changed = table.filter(pa.array([previous < row[4] <= target for row in rows]))
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
                if row is not None and int(row[0]) == batch.context.from_revision:
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
                    kind = spec.value_kind
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
                self._execute(conn, "UPDATE binding_progress SET consumed_revision=? WHERE progress_key=?", [batch.context.to_revision, binding.progress_key])
            return {binding.signature: True for binding, _, _ in accepted}


class Executor(Protocol):
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]: ...


class InProcessExecutor:
    """Deterministic executor useful for tests; it has the same task boundary."""
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]:
        output = OutputBuilder(ports)
        application.transform(batch.inputs, output, batch.context)
        return output.values


@dataclass(frozen=True)
class _TaskResult:
    outputs: Mapping[str, pa.Table]
    started_at: float
    finished_at: float


def _ray_transform(application: App, batch: Batch,
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

    def submit(self, application: App, batch: Batch,
               ports: Mapping[str, tuple[str, OutputSpec]]) -> Any:
        """Submit work without waiting, returning Ray's dependency token."""
        # Arrow-bearing batches are put once in Ray's object store; retries and
        # object references are intentionally not durable coordination state.
        sealed = self._ray.put(batch)
        return self._task.remote(application, sealed, ports)

    def resolve(self, ticket: Any) -> _TaskResult:
        return self._ray.get(ticket)

    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, tuple[str, OutputSpec]]) -> Mapping[str, pa.Table]:
        return self.resolve(self.submit(application, batch, ports)).outputs


@dataclass
class _Invocation:
    """A claimed durable batch held until its result is committed or discarded."""
    binding: Binding
    batch: Batch
    lock: Any


class Scheduler:
    """Coordinates durable batches; independent graph layers may run in parallel."""
    def __init__(self, store: RevisionStore, executor: Executor | None = None):
        self.store, self.executor = store, executor or RayExecutor()
        self._locks: dict[str, Lock] = {}
        self._locks_guard = Lock()
    def run_once(self, binding: Binding, application: App) -> bool:
        invocation = self._prepare(binding, application)
        if invocation is None:
            return False
        try:
            return self._complete(invocation, self.executor.execute(application, invocation.batch, binding.outputs))
        finally:
            invocation.lock.release()

    def _prepare(self, binding: Binding, application: App) -> _Invocation | None:
        with self._locks_guard:
            lock = self._locks.setdefault(binding.progress_key, Lock())
        # This lock only avoids duplicate local work; the durable frontier
        # comparison remains the correctness guard after a restart.
        if not lock.acquire(blocking=False): return None
        try:
            self.store.initialise(binding, application.backfill)
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

    def run_layer(self, bindings: Iterable[Binding], applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
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

    def _run_async_layer(self, wave: tuple[Binding, ...], applications: Mapping[str, App], submit: Any, resolve: Any) -> bool:
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

    def run_graph_once(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
        """Run every dependency wave once, committing each wave before the next."""
        ran = False
        for wave in graph.layers():
            ran = self.run_layer(wave, applications, max_workers=max_workers) or ran
        return ran

    def run_until_idle(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> None:
        """Drive a DAG to its latest canonical state without durable queue state."""
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
