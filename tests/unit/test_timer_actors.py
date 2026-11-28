"""Timer handlers, snapshot isolation, and actor recovery."""

from contextlib import closing
from datetime import timedelta
import json
from queue import Queue
from threading import Event
from time import monotonic, sleep

import pandas as pd
import polars as pl
import pytest
from escapement import Timeout

import acquirium as aq
from acquirium.Server.actors import ActorSupervisor
from acquirium.core import Core
from acquirium.internals.internals_namespaces import ACQUIRIUM_ATTR_NS, S223
from acquirium.internals.models import MAX_REQUEST_ROWS, compute_stream_uri
from tests.epoch_helpers import epoch_server, readings


@pytest.mark.parametrize("interval", [pd.Timedelta("1ns"), pd.Timedelta("1001ns")],
                         ids=["smaller-than-a-microsecond", "fractional-microseconds"])
def test_actor_rejects_timers_the_scheduler_cannot_represent(interval):
    class Handler:
        def initial_state(self):
            return None

        def on_timer(self, tick):
            return aq.Transition(None, {})

    with pytest.raises(ValueError, match="microsecond precision"):
        aq.Actor("too-fast", Handler, {}, {}, {"sample": interval})


def test_rejected_transition_keeps_the_actor_running(tmp_path):
    observed = Queue()

    class Writer:
        def initial_state(self):
            self.attempts = 0
            return 0

        def on_timer(self, tick):
            self.attempts += 1
            observed.put(self.state)
            if self.attempts == 1:
                # The saved state adds one row, putting this request over the limit.
                return aq.Transition(100, {"out": [(i, 100) for i in range(MAX_REQUEST_ROWS)]})
            return aq.Transition(self.state + 1, {"out": [(tick.scheduled_at, self.state + 1)]})

    actor = aq.Actor(
        name="oversized", handler=Writer, inputs={}, outputs={"out": "numeric"},
        timers={"sample": timedelta(milliseconds=250)},
    )
    with closing(Core(tmp_path / "actor.duckdb")) as core:
        workers = ActorSupervisor(core)
        workers.start(actor)
        try:
            # The rejected tick leaves state at zero. The next tick commits one.
            assert [observed.get(timeout=10) for _ in range(3)] == [0, 0, 1]
            output = str(compute_stream_uri(actor.source_id, "out"))
            assert readings(core.engine.snapshot(), output, end=MAX_REQUEST_ROWS) == []
        finally:
            workers.close(10)


def test_timer_uses_a_pinned_snapshot_and_resumes_committed_state(tmp_path, monkeypatch):
    started = Event()
    release = Event()
    initializations = []

    class Counter:
        def initial_state(self):
            initializations.append(True)
            return 0

        def on_timer(self, tick):
            # Let the test correct the reading after this tick pins its snapshot.
            started.set()
            assert release.wait(15)
            observed = tick.snapshot.read("input")["value"].item()
            count = self.state + 1
            return aq.Transition(count, {
                "seen": [(tick.scheduled_at, observed)],
                "count": [(tick.scheduled_at, count)],
            })

    source = aq.Stream.from_source("test", "actor-input")
    actor = aq.Actor(
        name="counter", handler=Counter, inputs={"input": source},
        outputs={"seen": "numeric", "count": "numeric"},
        timers={"sample": pd.Timedelta("250ms")},
    )
    count_stream = str(compute_stream_uri(actor.source_id, "count"))
    seen_stream = str(compute_stream_uri(actor.source_id, "seen"))
    checkpoint = "urn:acquirium:actor-state:counter"

    def write(core, value):
        """Publish a value at the same input timestamp on every call."""
        frame = pl.DataFrame({"stream_uri": [str(source)], "ts": [0], "value": [value]})
        frame = frame.with_columns(pl.col("ts").cast(pl.Datetime("ns", "UTC")))
        core.wait(core.ingest("test", str(value), frame).receipt)

    def wait_for_count(core, minimum):
        """Read committed counter values once enough timer ticks have published."""
        deadline = monotonic() + 15
        while monotonic() < deadline:
            counts = readings(core.engine.snapshot(), count_stream)
            if len(counts) >= minimum:
                return counts
            sleep(0.02)
        raise AssertionError("timer did not publish enough output")

    path = tmp_path / "actor.duckdb"
    with closing(Core(path, history_epochs=1)) as core:
        # Seed the input and start a timer that publishes the value it sees and its count.
        write(core, 2)
        input_epoch = core.engine.status()["published"]
        workers = ActorSupervisor(core)
        workers.start(actor)
        try:
            assert started.wait(15)
            write(core, 4)  # Publish while the first handler is blocked.
            assert not readings(core.engine.snapshot(), count_stream)
            # Retention can advance to the pinned epoch while the handler is paused.
            deadline = monotonic() + 5
            while core.engine.status()["retention"] < input_epoch:
                assert monotonic() < deadline, "retention did not advance"
                sleep(0.02)
            assert core.engine.status()["retention"] == input_epoch
            release.set()
            wait_for_count(core, 2)
        finally:
            release.set()
            workers.close(15)
        # The first handler saw 2, the next saw the correction, and the saved
        # checkpoint contains the same count as the last committed output.
        snapshot = core.wait_for(core.engine.status()["accepted"])
        assert readings(snapshot, seen_stream)[:2] == [2, 4]
        last_count = readings(snapshot, count_stream)[-1]
        assert json.loads(readings(snapshot, checkpoint)[0])["state"] == last_count
        del snapshot

    with closing(Core(path)) as core:
        # Reopen the same store and make the first checkpoint wait time out.
        recovery_sequence = core.engine.status()["accepted"]
        wait_for = core.wait_for
        recovery_attempts = []

        def delayed_publication(sequence, timeout):
            if sequence == recovery_sequence:
                recovery_attempts.append(sequence)
                if len(recovery_attempts) == 1:
                    raise Timeout("checkpoint is still publishing")
            return wait_for(sequence, timeout=timeout)

        monkeypatch.setattr(core, "wait_for", delayed_publication)
        workers = ActorSupervisor(core)
        workers.start(actor)
        try:
            # Recovery retries the wait and the next tick continues the saved count.
            counts = wait_for_count(core, int(last_count) + 1)
            assert counts[int(last_count)] == last_count + 1
            assert initializations == [True]
            assert len(recovery_attempts) == 2
        finally:
            workers.close(15)


def test_timer_queries_keep_the_initial_graph_bindings_metadata_and_readings(tmp_path):
    started, release = Event(), Event()
    observed = Queue()

    class ReadProbes:
        def initial_state(self):
            return None

        def on_timer(self, tick):
            # Build a lazy query now and collect its rows after the test edits the inputs.
            query = tick.snapshot.query()
            started.set()
            assert release.wait(15)
            direct = tick.snapshot.read("tank_probes", start=1000, end=2000)
            assert tick.snapshot.read("missing").schema == direct.schema
            assert tick.snapshot.query("missing").dataframe().is_empty()
            observed.put((
                tick.snapshot.graph_revision,
                tick.snapshot.epoch,
                tick.snapshot.metadata("tank_probes"),
                direct,
                query.dataframe(start=1000, end=2000),
            ))
            return aq.Transition(None)

    def write(manager, probe, request_id, value):
        """Publish a reading in the test window and a sentinel at its excluded end."""
        manager.core.wait(manager.insert_timeseries(
            source_id="probes", stream_name=probe, request_id=request_id,
            rows=[(1000, value), (2000, 99)],
        ).receipt)

    def assign(manager, moving_tank, calibration_version):
        """Replace the tank assignments and optional calibration metadata."""
        calibration = (
            f"<urn:probe:stable> <{ACQUIRIUM_ATTR_NS}calibration_version> {calibration_version} ."
            if calibration_version is not None else ""
        )
        manager.insert_graph(f"""
            <urn:tank:{moving_tank}> <{S223.hasProperty}> <urn:probe:moving> .
            <urn:tank:A> <{S223.hasProperty}> <urn:probe:stable>, <urn:probe:unwritten> .
            {calibration}
        """, source_id="probe-assignment", replace=True)

    with epoch_server(tmp_path) as (manager, _, client):
        # A query for tank A initially finds a moving probe, a stable probe,
        # and a registered probe with no readings yet.
        client.register_streams([
            dict(source_id="probes", stream_name=probe, value_kind="numeric", property_uri=f"urn:probe:{probe}")
            for probe in ("moving", "stable", "unwritten")
        ])
        assign(manager, "A", 1)
        write(manager, "moving", "moving-original", 10)
        write(manager, "stable", "stable-original", 20)
        moving = str(compute_stream_uri("probes", "moving"))
        stable = str(compute_stream_uri("probes", "stable"))
        query = client.query().entity(uri="urn:tank:A", alias="tank").measurement(
            alias="probe", include_connection_points=False,
        ).include("calibration_version", of="probe")
        assert query.metadata().height == 3  # Prime the declaration's client cache.
        actor = aq.Actor(
            name="probe-reader", handler=ReadProbes,
            inputs={
                "tank_probes": query,
                "reference": aq.Stream(stable),
                "missing": client.query().measurement(uri="urn:probe:missing"),
            },
            outputs={},
            timers={"sample": timedelta(milliseconds=250)},
        )
        workers = ActorSupervisor(manager.core, manager.graph_store)
        workers.start(actor)
        try:
            assert started.wait(15)
            # Move the first probe away, delete the calibration attribute, and
            # correct the stable reading while the first timer is paused.
            assign(manager, "B", None)
            write(manager, "stable", "stable-correction", 21)
            release.set()
            first, second = observed.get(timeout=15), observed.get(timeout=15)
        finally:
            release.set()
            workers.close(15)

        # Both read paths keep the first tick's view and see the edits on the
        # next tick. The deleted optional attribute is still a readable column.
        first_revision, first_epoch, first_meta, first_read, first_query = first
        second_revision, second_epoch, second_meta, second_read, second_query = second
        assert second_revision > first_revision
        assert second_epoch > first_epoch
        assert first_meta.height == 3
        assert second_meta.height == 2
        assert first_meta["probe.calibration_version"].drop_nulls().to_list() == [1]
        assert second_meta["probe.calibration_version"].is_null().all()
        assert dict(first_read.select("stream_uri", "value").iter_rows()) == {moving: 10, stable: 20}
        assert dict(second_read.select("stream_uri", "value").iter_rows()) == {stable: 21}
        for direct, frame, reference_value in [(first_read, first_query, 20), (second_read, second_query, 21)]:
            assert frame.filter(pl.col("input") == "tank_probes").drop("input").equals(direct)
            assert frame.filter(pl.col("input") == "reference")["value"].to_list() == [reference_value]


def test_actor_handles_relevant_graph_changes_before_using_new_bindings(tmp_path, monkeypatch):
    clock_ready, timer_started, release = Event(), Event(), Event()
    release_change = Event()
    observations = Queue()
    hour = 3_600_000_000_000
    now = 0

    def clock_ns():
        # Let the scheduler sample zero for its first deadline before we advance it.
        sampled = now
        clock_ready.set()
        return sampled

    monkeypatch.setattr("acquirium.Server.actors.clock_ns", clock_ns)

    class WatchProbes:
        def initial_state(self):
            return 0

        def on_timer(self, tick):
            timer_started.set()
            assert release.wait(15)
            value = tick.snapshot.read("tank_probe")["value"].item()
            observations.put(("timer", tick.snapshot.graph_revision, value, self.state))
            return aq.Transition(self.state)

        def on_graph_change(self, change: aq.GraphChange):
            assert change.snapshot.graph_revision == change.new_revision
            assert tuple(change.inputs) == ("tank_probe",)
            probe = change.inputs["tank_probe"]
            observations.put((
                "change", change.old_revision, change.new_revision, probe.added_streams, probe.removed_streams,
                probe.old_metadata["probe.calibration_version"].item(),
                probe.new_metadata["probe.calibration_version"].item(),
                change.snapshot.read("tank_probe")["value"].item(), self.state,
            ))
            if probe.added_streams:
                assert release_change.wait(15)
            return aq.Transition(self.state + 1, {"changed": [(change.new_revision, self.state + 1)]})

    with epoch_server(tmp_path) as (manager, _, client):
        # Both probes have readings. The graph decides which one the actor sees.
        client.register_streams([
            dict(source_id="probes", stream_name=probe, property_uri=f"urn:probe:{probe}", value_kind="numeric")
            for probe in ("first", "second")
        ])
        for probe, value in (("first", 10), ("second", 20)):
            manager.core.wait(manager.insert_timeseries(
                source_id="probes", stream_name=probe, rows=[(1000, value)], request_id=probe,
            ).receipt)

        def assign(probe, version):
            """Assign the tank's probe and return the updated graph revision."""
            calibration = (
                f"<urn:probe:{probe}> <{ACQUIRIUM_ATTR_NS}calibration_version> {version} ."
                if version is not None else ""
            )
            manager.insert_graph(f"""
                <urn:tank:A> <{S223.hasProperty}> <urn:probe:{probe}> .
                {calibration}
            """, source_id="probe-assignment", replace=True)
            return manager.graph_store.graph_status()["source_version"]

        def unrelated(value):
            """Publish a graph edit outside the actor's input query."""
            manager.insert_graph(
                f"<urn:unrelated> <urn:label> {value} .", source_id="unrelated", replace=True,
            )
            return manager.graph_store.graph_status()["source_version"]

        assign("first", 1)
        query = client.query().entity(uri="urn:tank:A", alias="tank").measurement(
            alias="probe", include_connection_points=False,
        ).include("calibration_version", of="probe")
        actor = aq.Actor(
            name="watch-probes", handler=WatchProbes,
            inputs={"tank_probe": query, "other": client.query().measurement(uri="urn:probe:missing")},
            outputs={"changed": "numeric"}, timers={"sample": timedelta(hours=1)},
        )
        first = str(compute_stream_uri("probes", "first"))
        second = str(compute_stream_uri("probes", "second"))
        workers = ActorSupervisor(manager.core, manager.graph_store)
        workers.start(actor)
        _, wake = workers.workers[actor.name]

        def advance_clock(timestamp):
            """Wake the scheduler after moving the test clock to a timer deadline."""
            nonlocal now
            now = timestamp
            wake.set()

        try:
            assert clock_ready.wait(15)
            initial_revision = unrelated(1)
            advance_clock(hour)
            assert timer_started.wait(15)
            # Move the probe while the first timer is still reading its original view.
            moved_revision = assign("second", 2)
            assert observations.empty()
            now = 2 * hour  # A timer is due when the graph change is dispatched.
            release.set()
            assert observations.get(timeout=15) == ("timer", initial_revision, 10, 0)
            assert observations.get(timeout=15) == (
                "change", initial_revision, moved_revision, (second,), (first,), 1, 2, 20, 0,
            )
            # Change calibration during the callback. The due timer gets the
            # bindings just delivered before the next callback sees the new metadata.
            metadata_revision = assign("second", 3)
            release_change.set()
            assert observations.get(timeout=15) == ("timer", moved_revision, 20, 1)
            assert observations.get(timeout=15) == (
                "change", moved_revision, metadata_revision, (), (), 2, 3, 20, 1,
            )

            # The unrelated edit advances the graph revision without a callback.
            unrelated_revision = unrelated(2)
            advance_clock(3 * hour)
            assert observations.get(timeout=15) == ("timer", unrelated_revision, 20, 2)

            # The fake clock stays frozen, so publication must wake the actor.
            removed_revision = assign("second", None)
            assert observations.get(timeout=15) == (
                "change", unrelated_revision, removed_revision, (), (), 3, None, 20, 2,
            )
            final_revision = unrelated(3)
            advance_clock(4 * hour)
            assert observations.get(timeout=15) == ("timer", final_revision, 20, 3)
        finally:
            release.set()
            release_change.set()
            workers.close(15)

        output = str(compute_stream_uri(actor.source_id, "changed"))
        # Only the three relevant changes published an output.
        assert readings(manager.core.engine.snapshot(), output) == [1, 2, 3]
