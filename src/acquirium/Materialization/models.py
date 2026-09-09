"""Application declarations, resolved bindings, and dataframe helpers.

OutputSpec and App describe a calculation before its query is resolved. The
planner turns these declarations into Bindings with concrete input references
and OutputPorts. RevisionStore then supplies a Batch: loaded StreamSets plus
an InputBatch describing the interval to replace. OutputBuilder validates the
transform's assignments before they reach storage.

Keep SQL and scheduling out of these types so the same authoring and validation
rules apply to deployed apps, local checks, and standalone runtime users.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from typing import Any, Callable, Iterable, Iterator, Mapping, NamedTuple

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
    if not isinstance(value, str):
        raise TypeError("durations must be strings or timedeltas")
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
    if value == "all":
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
    """Loaded data and descriptors for one query alias in one invocation.

    The Arrow tables belong to the batch and remain usable after its database
    read transaction closes. ``batches()`` only slices that loaded table; it
    does not bound how much data was fetched. ``changes`` contains live changed
    rows, while the complete table supplies context and reflects removals.
    """
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
    every: timedelta | None = None
    _scheduled: bool = field(default=False, repr=False)

    @property
    def stream(self) -> StreamDescriptor:
        """The one stream bound to this alias — which sensor this call is for.

        A ``per_match`` app binds exactly one stream per alias per call —
        for every alias, even when a query row pairs two — so this is the
        usual way to ask what is being computed: ``inputs["temperature"].stream``
        gives its ``ref_uri``, ``point_uri``, ``label`` and ``unit``.

        An ``all_matches`` app sees the whole query result at once, so its aliases
        can hold many streams and this raises. Use :attr:`streams` there; an
        aggregate is about all of them by definition.
        """
        if len(self.streams) != 1:
            raise ValueError(
                f"alias {self.alias!r} is bound to {len(self.streams)} streams, not one — "
                f"all_matches grouping sees every match in one call. Use .streams for all of them, "
                f"or per_match grouping to run once per match."
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
                         self.batch_size, self.converter, self.every, self._scheduled)


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
    ``row`` is the one row a ``per_match`` call is computing. The windows say
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
    output_window: TimeWindow = field(kw_only=True)
    # These fields identify one step of durable, partitioned work. Publication
    # compares both ID and cursor: the input frontier alone stays unchanged
    # between chunks and therefore cannot detect a repeated chunk's result.
    work_id: str | None = None
    work_cursor: str | None = None
    work_next: str | None = None

    @property
    def result(self) -> Any:
        """Everything ``build_query`` matched, as a Polars dataframe.

        The same table in every call of an app, regardless of grouping:
        a ``per_match`` call sees the whole fleet it is one of, which is what
        lets it group, rank, or count siblings. Columns follow
        ``Query.metadata()``: an alias holds the matched URI, with
        ``<alias>_ref``, ``<alias>.label`` and ``<alias>.unit`` beside a
        stream-bearing one.

        Use :attr:`row` for the one row this call is computing.
        """
        return _rows_to_frame(self._result)

    @property
    def row(self) -> Mapping[str, Any]:
        """The row this call is computing, with ``per_match`` grouping.

        An ``all_matches`` call also has a row when its query matches exactly
        one row. Otherwise this raises; :attr:`result` holds the whole table.
        """
        if self._row is None:
            raise ValueError(
                "this call covers every matched row, so it has no single row — that is what "
                "all_matches grouping sees. Use .result for the whole table, or per_match grouping to "
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
    ``ref_uri`` is the canonical stream identity that name hashes to, and
    ``point_uri`` is the graph node carrying the output's metadata — the
    author's own point when they declared one, otherwise a point named after
    the stream itself. The storage backend maps ``ref_uri`` to its internal
    integer key.
    Everything downstream reads these fields instead of recomputing them, so
    the name and the URI cannot drift apart.
    """
    ref_uri: str
    ref_name: str
    point_uri: str
    spec: OutputSpec


class _OutputAPI:
    """Declare a generated stream identity or an explicitly named identity.

    App.grouping chooses which query matches each call receives. Each output
    stream has one owner; multiple per_match bindings cannot share a named one.
    """
    def stream(self, **kwargs: Any) -> OutputSpec:
        if "stream_name" in kwargs: raise TypeError("stream outputs derive their name; use output.named(...)")
        return OutputSpec(**kwargs)
    def named(self, stream_name: str, **kwargs: Any) -> OutputSpec:
        return OutputSpec(stream_name=stream_name, **kwargs)
output = _OutputAPI()


class OutputBuilder:
    """Validate assignments while preserving the author's replacement intent.

    A missing port means leave its stored interval alone. An assigned empty
    table means remove that interval's results. Do not prepopulate ports with
    empty tables or drop empty assignments: either would change publication.
    Window clipping belongs to publication (and check rendering), since this
    collector only knows port schemas, not the invocation's output interval.
    """
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
    if pc.count_distinct(result["time"]).as_py() != result.num_rows: raise ValueError("output timestamps must be unique")
    return result.sort_by([("time", "ascending")])


def _canonical(value: object) -> str: return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

@dataclass(frozen=True)
class Binding:
    """A resolved calculation, with separate identities for separate lifetimes.

    ``signature`` includes code, parameters, input metadata, and window policy.
    It identifies compiled work and diagnostics. ``progress_key`` includes only
    the app and its input/output references, allowing code edits to preserve
    the consumed frontier. Reprocessing old values is an explicit operation.

    ``generation`` is supplied by Materializer and authorizes publication. It
    also captures deployment activation and query context, so an old worker
    cannot publish merely because its durable progress key still exists.
    Graph revision is diagnostic; unrelated graph edits must not reset progress.

    Treat the mappings inside a binding as immutable once it is in a plan:
    workers retain references to a plan after the runtime has replaced it.
    """
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
    custom_window: Callable[[TimeWindow], TimeWindow] | None = field(default=None, repr=False, compare=False)
    signature: str = field(init=False)
    progress_key: str = field(init=False)
    def __post_init__(self):
        if not self.inputs or not self.outputs: raise ValueError("a binding needs inputs and outputs")
        if self.every is not None and self.every <= timedelta():
            raise ValueError("every must be positive")
        payload = {"v": 1, "application": self.application_name, "executable": self.executable_digest,
                   "inputs": {k: [x.__dict__ for x in sorted(v, key=lambda x: x.ref_uri)] for k,v in sorted(self.inputs.items())},
                   "outputs": {k: (v.ref_uri, v.spec.__dict__) for k,v in sorted(self.outputs.items())},
                   "lookback": "all" if self.lookback is None else self.lookback.total_seconds(),
                   "lookahead": self.lookahead.total_seconds(),
                   "every": self.every.total_seconds() if self.every else None}
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
    """Resolve dependencies through output ownership, then validate the graph.

    An edge exists when a binding reads another binding's output reference.
    Explicit single ownership makes replacement unambiguous, and rejecting
    cycles ensures the scheduler can visit producers before consumers. This
    class describes ordering only; readiness, retries, and execution limits
    belong to the runtime and scheduler.
    """
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
        self._layers = self.layers()
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
        if hasattr(self, "_layers"):
            return self._layers
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


_GROUPINGS = ("per_match", "all_matches")


class _AppMeta(type):
    # Validate after construction so custom __init__ methods cannot bypass the
    # check by omitting super().__init__(), and constructor-supplied values are
    # checked as well as class attributes.
    def __call__(cls, *args: Any, **kwargs: Any) -> "App":
        app = super().__call__(*args, **kwargs)
        if app.grouping not in _GROUPINGS:
            raise ValueError(
                f"{cls.__name__}.grouping must be explicitly set to "
                f"'per_match' or 'all_matches'; got {app.grouping!r}"
            )
        return app


class App(metaclass=_AppMeta):
    """A stateless calculation over streams selected by a semantic query.

    Every concrete app must explicitly set grouping to ``"per_match"`` or
    ``"all_matches"``. It selects per-match calls or one aggregate call
    independently of output naming. every declares complete resampling buckets. lookback
    and lookahead describe trailing and leading input dependencies; their
    effects also determine which outputs a correction recomputes.
    backfill processes retained history on first activation. batch_delay and
    min_interval are advanced operational controls: batch_delay collects rapid
    changes before an invocation, while min_interval waits between successful
    invocations of an expensive computation. Their process-local timing state
    is not a failure retry backoff. They do not affect event-time windows.
    """
    name: str | None = None
    every: timedelta | str | None = None
    grouping: str | None = None
    lookback: timedelta | str = "0s"
    lookahead: timedelta | str = "0s"
    backfill: bool = False
    batch_delay: timedelta | str = "0s"
    min_interval: timedelta | str | None = None
    outputs: Mapping[str, OutputSpec] = {}
    def build_query(self, plant: Any) -> Any: raise NotImplementedError
    def transform(self, inputs: Mapping[str, StreamSet], output: OutputBuilder, context: InputBatch) -> None: raise NotImplementedError
    def output_window(self, changed: TimeWindow) -> TimeWindow | None:
        """Declare a custom mapping from changed inputs to affected outputs.

        The planner recognizes this base method and uses lookback/lookahead
        and bucket declarations instead of calling it. An override must return
        a TimeWindow for every call; returning None from an override is invalid.
        Input context is still added around that output interval by RevisionStore.
        """
        return None


def align(inputs: Mapping[str, StreamSet], every: timedelta | str | None = None, *, aggregate: str = "mean") -> Any:
    """Resample every input onto one shared clock and return a wide dataframe.

    The result has a ``time`` column plus one column per stream: an alias with
    a single bound stream contributes a column named after the alias, and an
    alias with several contributes ``alias[label-or-ref]`` columns. Buckets a
    stream never reported in hold nulls; combining differently sampled sensors
    is then one join instead of a hand-rolled resample per stream.
    """
    import polars as pl
    # Resampling needs every input bucket in full. Choosing a different size
    # here after the runtime has loaded its windows could silently aggregate
    # partial buckets and make results depend on ingestion batch boundaries.
    declared = {value.every for value in inputs.values() if value.every is not None}
    if not declared and any(value._scheduled for value in inputs.values()):
        raise ValueError("declare App.every to resample scheduled inputs with complete buckets")
    if every is None:
        if len(declared) != 1:
            raise ValueError("align needs every= or an App.every declaration")
        step = next(iter(declared))
    else:
        step = _duration(every)
        if declared and declared != {step}:
            raise ValueError("align every must match App.every so buckets are read completely")
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
    # Keep timestamps present in any stream. An inner join would discard data
    # when sensors report at different times; callers decide how to handle nulls.
    for column in columns[1:]:
        result = result.join(column, on="time", how="full", coalesce=True)
    return result.sort("time")
