"""Run configured soft sensor actors alongside the Escapement epoch engine."""

from collections import Counter, deque
from collections.abc import Mapping
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from math import isfinite
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic, time_ns as clock_ns
from types import MappingProxyType
from uuid import uuid4

from escapement import Capacity, Closed, NotFound, Refused, Timeout
import polars as pl
import pyarrow as pa

from acquirium.actors import Actor, GraphChange, InputChange, InputSnapshot, TimerFired, Transition, _BoundInput
from acquirium.Client.data_object import _parse_sparql_bindings
from acquirium.Client.explore.attributes import Attr, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import result_datatypes
from acquirium.Client.streams import Stream
from acquirium.app_spec import duration_ns
from acquirium.core import Core, time_ns
from acquirium.internals.models import compute_stream_uri
from acquirium.Server.config import load_app_target
from acquirium.Storage.graph_store import OxigraphGraphStore, sparql_results_to_rows
from acquirium.Storage.values import typed_value_series

log = logging.getLogger(__name__)
SECOND = 1_000_000_000
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _ActorBindings:
    revision: int
    inputs: Mapping[str, _BoundInput]


class _ActorRegistry(Registry):
    def __init__(self, client, known: dict[str, Attr]):
        super().__init__(client)
        self._known = known

    def _discovered(self) -> dict[str, Attr]:
        # A declaration can still refer to a custom attribute after its
        # last value is removed. Keep its predicates available to compile.
        if self._found is None:
            self._known.update(super()._discovered())
        return self._known


class _ActorQueryClient:
    """The read methods the explore builder needs while binding actor inputs."""

    def __init__(self, graph: OxigraphGraphStore):
        self._graph = graph
        self.base_url = f"actor-graph:{uuid4()}"

    def sparql_query(self, query: str, include_dependencies: bool = True, *, wait_for_fresh: bool = False) -> dict:
        result = self._graph.sparql_query_json(
            query, include_dependencies=include_dependencies, wait_for_fresh=wait_for_fresh,
        )
        if result is None:
            raise ValueError("actor input queries must select metadata rows")
        return sparql_results_to_rows(result)

    def graph_version(self) -> int:
        return self._graph.graph_status()["published_version"]

    @staticmethod
    def compact_uri(uri: str) -> str:
        return uri


class ActorSupervisor:
    def __init__(self, core: Core, graph: OxigraphGraphStore | None = None):
        self.core = core
        self.graph = graph
        self._query_client = _ActorQueryClient(graph) if graph is not None else None
        self.stop = Event()
        self.lock = Lock()
        self.workers: dict[str, tuple[Thread, Event]] = {}

    def start_configured(self, client, config):
        entries = config.get("actors", [])
        if not isinstance(entries, list):
            log.warning("Actor configuration must use [[actors]] entries")
            return
        directory = Path(config.get("__config_dir", Path.cwd()))
        for entry in entries:
            if self.stop.is_set():
                return
            if not isinstance(entry, Mapping):
                log.warning("Skipping actor entry that is not a table")
                continue
            spec = entry.get("spec")
            try:
                if not isinstance(spec, str) or not spec:
                    raise ValueError("[[actors]] entries require a spec")
                target = load_app_target(spec, base_dir=directory)
                parameters = {key: value for key, value in entry.items() if key != "spec"}
                configure = getattr(target, "configure", None) if isinstance(target, type) else None
                if callable(configure):
                    actor = configure(client, **parameters)
                else:
                    actor = target(client, parameters) if callable(target) else target
                if not isinstance(actor, Actor):
                    raise ValueError(
                        "actor spec must name an Actor, a class with configure(), "
                        "or a factory returning an Actor"
                    )
                if actor.name in self.workers:
                    raise ValueError(f"duplicate actor name {actor.name!r}")
                client.register_streams([
                    {"source_id": actor.source_id, "stream_name": name, "value_kind": kind}
                    for name, kind in actor.outputs.items()
                ])
                self.start(actor)
            except Exception:
                log.exception("Could not start configured actor %r", spec)

    def start(self, actor: Actor):
        if self.graph is None and any(isinstance(value, Query) for value in actor.inputs.values()):
            raise ValueError("query inputs require an actor supervisor with a graph store")
        with self.lock:
            if self.stop.is_set():
                return
            if actor.name in self.workers:
                raise ValueError(f"duplicate actor name {actor.name!r}")
            wake = Event()
            worker = Thread(target=self._run, args=(actor, wake), name=actor.source_id, daemon=True)
            self.workers[actor.name] = worker, wake
            worker.start()

    def close(self, timeout):
        with self.lock:
            self.stop.set()
            workers = list(self.workers.values())
            for _, wake in workers:
                wake.set()
        deadline = None if timeout is None else monotonic() + timeout
        for worker, _ in workers:
            worker.join(None if deadline is None else max(0, deadline - monotonic()))
            if worker.is_alive():
                log.warning("%s is still finishing its handler", worker.name)

    def _run(self, actor: Actor, wake: Event):
        try:
            handler = actor.handler()
            graph_handler = getattr(handler, "on_graph_change", None)
            if all(isinstance(value, Stream) for value in actor.inputs.values()):
                graph_handler = None
            watch = self.graph.watch_query_revisions(wake) if graph_handler is not None else nullcontext()
            with watch:
                self._run_messages(actor, handler, graph_handler, wake)
        except Closed:
            if not self.stop.is_set():
                log.exception("Engine closed while %s was running", actor.source_id)
        except Exception:
            log.exception("%s stopped", actor.source_id)

    def _input_snapshot(self, actor: Actor, pins: ExitStack, previous: _ActorBindings | None,
                        known_attributes: dict[str, Attr], *, track_changes: bool) -> InputSnapshot:
        if all(isinstance(value, Stream) for value in actor.inputs.values()):
            snapshot = pins.enter_context(self.core.pinned_snapshot())
            return InputSnapshot(snapshot, {
                name: _BoundInput((str(stream),)) for name, stream in actor.inputs.items()
            })

        # Pin data once the inferred graph is fresh. Release the graph lock
        # before calling user code, while keeping the data epoch pinned.
        with self.graph.current_query_view() as revision:
            snapshot = pins.enter_context(self.core.pinned_snapshot())
            if previous is not None and previous.revision == revision:
                return InputSnapshot(snapshot, previous.inputs, revision)
            registry = _ActorRegistry(self._query_client, known_attributes)
            inputs = {}
            for name, declaration in actor.inputs.items():
                if isinstance(declaration, Stream):
                    inputs[name] = _BoundInput((str(declaration),))
                    continue
                # A declaration may already have a client-side result cached.
                query = Query(client=self._query_client, query_graph=declaration.query_graph,
                              cache={"_registry": registry})
                matches, *_ = _parse_sparql_bindings(query, include_dependencies=True)
                streams = tuple(sorted({stream for _, _, stream in matches}))
                key = _match_key(query.execute()) if track_changes else None
                inputs[name] = _BoundInput(streams, query.metadata(include_internals=True), key)
        return InputSnapshot(snapshot, inputs, revision)

    def _run_messages(self, actor: Actor, handler, graph_handler, wake: Event):
        checkpoint = f"urn:acquirium:actor-state:{actor.name}"
        # Pending requests from the previous server can contain the last checkpoint.
        if not self._wait_for_publication(self.core.engine.status()["accepted"]):
            return
        with self.core.pinned_snapshot() as snapshot:
            saved = _load_checkpoint(snapshot, checkpoint)
        del snapshot
        if saved is None:
            saved = _checkpoint(handler.initial_state(), {})

        periods = {name: duration_ns(interval, "timer interval")
                   for name, interval in actor.timers.items()}
        now = clock_ns()
        deadlines = {name: (now // period + 1) * period for name, period in periods.items()}
        due = deque()
        previous = None
        timer_turn = False
        known_attributes: dict[str, Attr] = {}
        if graph_handler is not None:
            wake.set()  # Establish the initial set of bindings before waiting for changes.
        while not self.stop.is_set():
            if not due:
                wake.wait(max(0, (min(deadlines.values()) - clock_ns()) / SECOND))
                if self.stop.is_set():
                    return
                now = clock_ns()
                # When an actor falls behind, run the newest due tick for each timer.
                due.extend(sorted((now // periods[name] * periods[name], name)
                                  for name, deadline in deadlines.items() if deadline <= now))
                for scheduled, name in due:
                    deadlines[name] = scheduled + periods[name]

            with ExitStack() as pins:
                try:
                    if timer_turn:
                        # Give a due timer a turn with the bindings just delivered.
                        # Keep any wakeup for a newer graph revision pending.
                        timer_turn = False
                        snapshot = InputSnapshot(
                            pins.enter_context(self.core.pinned_snapshot()), previous.inputs, previous.revision,
                        )
                    else:
                        wake.clear()
                        snapshot = self._input_snapshot(
                            actor, pins, previous, known_attributes, track_changes=graph_handler is not None,
                        )
                except Closed:
                    raise
                except Exception:
                    log.exception("%s failed to resolve its input snapshot", actor.source_id)
                    if due:
                        due.popleft()
                    continue

                change = message = None
                description = "message dispatch"
                try:
                    if snapshot.graph_revision is not None:
                        if graph_handler is not None:
                            change = _graph_change(previous, snapshot)
                        previous = _ActorBindings(snapshot.graph_revision, snapshot._inputs)
                    state = json.loads(saved)
                    if change is not None:
                        timer_turn = bool(due)
                        callback, message = graph_handler, change
                        # Timer request IDs end in a numeric scheduled time.
                        request_id = f"graph-change:{change.new_revision}:transition"
                        description = f"graph change {change.old_revision} to {change.new_revision}"
                    elif due:
                        scheduled, name = due.popleft()
                        if scheduled <= state["timers"].get(name, -1):
                            continue
                        scheduled_at = EPOCH + timedelta(microseconds=scheduled // 1000)
                        callback = handler.on_timer
                        message = TimerFired(name, scheduled_at, actor.timers[name], snapshot)
                        state["timers"][name] = scheduled
                        request_id = f"{name}:{scheduled}"
                        description = f"timer {name} at {scheduled_at}"
                    else:
                        continue

                    handler.state = state["state"]
                    transition = callback(message)
                    next_state, frame = _prepare(actor, transition, state["timers"], checkpoint)
                except Closed:
                    raise
                except Exception:
                    log.exception("%s failed on %s", actor.source_id, description)
                    continue
                finally:
                    del snapshot, change, message

            if self._commit(actor, request_id, frame):
                saved = next_state

    def _commit(self, actor, request_id, frame):
        # Output rows and the checkpoint are one Escapement request.
        while not self.stop.is_set():
            try:
                receipt = self.core.ingest(actor.source_id, request_id, frame).receipt
            except Capacity:
                self.stop.wait(0.2)
            except (ValueError, Refused):
                log.exception("%s could not submit transition %s", actor.source_id, request_id)
                return False
            else:
                return self._wait_for_publication(receipt.sequence)
        return False

    def _wait_for_publication(self, sequence: int) -> bool:
        while not self.stop.is_set():
            try:
                self.core.wait_for(sequence, timeout=1)
                return True
            except Timeout:
                continue
        return False


def _match_key(result):
    # SPARQL may return rows in a different order for the same matches.
    # Include RDF datatypes so changes hidden by lexical equality still count.
    rows = Counter(tuple(zip(row, types)) for row, types in zip(result["rows"], result_datatypes(result)))
    return tuple(result["columns"]), rows


def _graph_change(previous: _ActorBindings | None, snapshot: InputSnapshot) -> GraphChange | None:
    revision = snapshot.graph_revision
    if previous is None or revision is None or previous.revision == revision:
        return None
    affected = {}
    for name, current in snapshot._inputs.items():
        old = previous.inputs[name]
        if current.metadata is None or (old.streams == current.streams and old.match_key == current.match_key):
            continue
        affected[name] = InputChange(
            added_streams=tuple(sorted(set(current.streams) - set(old.streams))),
            removed_streams=tuple(sorted(set(old.streams) - set(current.streams))),
            old_metadata=old.metadata.clone(),
            new_metadata=current.metadata.clone(),
        )
    if not affected:
        return None
    return GraphChange(previous.revision, revision, MappingProxyType(affected), snapshot)


def _checkpoint(state, timers):
    return json.dumps({"state": state, "timers": timers}, allow_nan=False)


def _load_checkpoint(snapshot, stream):
    try:
        batches = snapshot.read([stream], limit=1)
    except NotFound:
        return None
    with pa.RecordBatchReader.from_stream(batches) as reader:
        values = reader.read_all()["text"]
        return values[0].as_py() if len(values) else None


def _prepare(actor, transition, timers, checkpoint):
    if not isinstance(transition, Transition):
        raise TypeError("actor handlers must return a Transition")
    saved = _checkpoint(transition.state, timers)
    streams, times, values, kinds = [checkpoint], [0], [saved], ["text"]
    for port, readings in transition.writes.items():
        kind = actor.outputs[port]
        stream = str(compute_stream_uri(actor.source_id, port))
        for timestamp, value in readings:
            ts = time_ns(timestamp)
            if ts is None:
                raise ValueError("actor outputs require a timestamp")
            if kind == "numeric" and (type(value) not in (int, float) or not isfinite(value)):
                raise ValueError(f"actor output {port!r} requires a finite number")
            if kind == "text" and not isinstance(value, str):
                raise ValueError(f"actor output {port!r} requires text")
            streams.append(stream)
            times.append(ts)
            values.append(value)
            kinds.append(kind)
    return saved, pl.DataFrame({
        "stream_uri": streams, "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
        "value": typed_value_series(values), "value_kind": kinds,
    })
