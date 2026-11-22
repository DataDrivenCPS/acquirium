"""Live ingest through metadata-driven plan changes, seals, and a restart.

A writer keeps submitting readings, late corrections, and deletes while
metadata edits move streams between groups, add groups, and remove them. Each
edit makes Acquirium compile and activate a new plan. Readers check that every
snapshot they see is coherent: each operator's output is the pointwise mean of
the inputs that snapshot's own plan lists, and outputs of groups the plan does
not hold are empty. History is checked again epoch by epoch, before and after a
restart.
"""

from __future__ import annotations

import functools
import math
import threading
import time

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from fastapi.testclient import TestClient

from acquirium import App, Output, Selection
from acquirium.apps import output_reference
from acquirium.core import Core
from acquirium.internals.internals_namespaces import OUTPUT_GROUP, PRODUCED_BY
from acquirium.internals.models import compute_ref_uri
from acquirium.Server import manager as manager_module
from acquirium.Server.app import app as api
from acquirium.Server.manager import Manager

SOURCE = "live"
STREAMS = "abc"
REFS = {name: str(compute_ref_uri(SOURCE, name)) for name in STREAMS}
FEEDS = "urn:live#feeds"
GROUPS = ("urn:live:g1", "urn:live:g2")
MEAN, TOTAL = "live-mean", "live-total"
OUTPUTS = {output_reference(MEAN, group, "mean"): group for group in GROUPS}
TOTAL_OUTPUT = output_reference(TOTAL, TOTAL, "mean")
# Three minutes apart, so the run spans more than one sealed UTC day.
BASE, STEP = 1_767_225_600 * 10**9, 180 * 10**9
LIMIT = 1_000_000

# Which streams feed which group. Moves, additions and removals of groups.
MEMBERSHIPS = [
    {"g1": "ab"},
    {"g1": "bc"},
    {"g1": "a", "g2": "bc"},
    {"g2": "abc"},
    {"g1": "ac", "g2": "b"},
    {"g1": "abc"},
]


def membership_turtle(membership):
    return "\n".join(
        f"<{REFS[stream]}> <{FEEDS}> <urn:live:{group}> ."
        for group, streams in membership.items()
        for stream in streams
    )


def apps(kind="mean"):
    """The group app, computing ``kind`` per group, and the app combining
    its groups' outputs."""
    function = {"kind": kind, "port": "mean"}
    if kind == "mean":
        function["per_stream"] = False
    mean = App(
        MEAN,
        {"input": Selection(f"SELECT ?stream ?group WHERE {{ ?stream <{FEEDS}> ?group }}")},
        {"mean": Output()},
        function,
    )
    total = App(
        TOTAL,
        {"input": Selection.outputs_of(mean, "mean", combine=True)},
        {"mean": Output()},
        {"kind": "mean", "port": "mean", "per_stream": False},
    )
    return mean, total


REDUCE = {"mean": lambda values: sum(values) / len(values), "max": max}


def operators(plan):
    return [
        op for op in (plan or {}).get("operators", [])
        if op["id"].split(":", 1)[0] in (MEAN, TOTAL)
    ]


def plan_groups(plan):
    return {
        OUTPUTS[stream]
        for op in operators(plan)
        for stream in op["outputs"].values()
        if stream in OUTPUTS
    }


def incoherence(plan, read):
    """What makes one epoch's rows disagree with its plan, or None.

    ``read(stream)`` returns ``{event_time_ns: value}`` at that epoch. Each
    operator reduces its inputs' readings at each time with its function."""
    live = set()
    for op in operators(plan):
        rows = [read(item["stream"]) for item in op["inputs"]]
        times = set().union(*rows)
        expected = {
            ts: REDUCE[op["function"]["kind"]](values)
            for ts in times
            if (values := [row[ts] for row in rows if ts in row])
        }
        for stream in op["outputs"].values():
            live.add(stream)
            actual = read(stream)
            if actual.keys() != expected.keys():
                return (
                    f"{op['id']}: output times differ; missing "
                    f"{sorted(expected.keys() - actual.keys())[:5]}, extra "
                    f"{sorted(actual.keys() - expected.keys())[:5]}"
                )
            for ts, value in expected.items():
                if not math.isclose(actual[ts], value, rel_tol=1e-12):
                    return f"{op['id']}: output at {ts} is {actual[ts]}, inputs give {value}"
    for stream in [*OUTPUTS, TOTAL_OUTPUT]:
        if stream not in live and read(stream):
            return f"{stream} is outside the plan but has {len(read(stream))} rows"
    return None


def rows(batch):
    return dict(zip(batch["event_time_ns"].to_pylist(), batch["value"].to_pylist()))


def check_snapshot(snapshot):
    cache = {}

    def read(stream):
        if stream not in cache:
            cache[stream] = rows(snapshot.query_arrow(stream, limit=LIMIT))
        return cache[stream]

    return incoherence(snapshot.plan, read)


def check_epoch(engine, epoch):
    cache = {}

    def read(stream):
        if stream not in cache:
            cache[stream] = rows(engine.query_arrow(stream, epoch=epoch, limit=LIMIT))
        return cache[stream]

    return incoherence(engine.plan_at(epoch), read)


def check_http(http, manager):
    """Read like a client: find the app's groups in the graph, capture an
    epoch, then read every stream at it.

    When the graph did not change in between, the epoch's plan must hold
    exactly the groups the graph named. Returns the problem, if any, and the
    plan read with whether that comparison applied."""
    before = manager.graph_store.graph_status()["source_version"]
    groups = metadata_groups(manager)
    snapshot = http.get("/engine/snapshot").json()
    epoch, plan = snapshot["epoch"], snapshot["plan"]
    compared = before == manager.graph_store.graph_status()["source_version"]
    if compared and groups != plan_groups(plan):
        return f"graph names {sorted(groups)}, epoch {epoch} plans {sorted(plan_groups(plan))}", None
    cache = {}

    def read(stream):
        if stream not in cache:
            response = http.get("/timeseries", params={"uri": stream, "epoch": epoch})
            assert response.status_code == 200, response.text
            table = ipc.open_stream(response.content).read_all()
            times = table["ts"].cast(pa.int64()).to_pylist()
            cache[stream] = dict(zip(times, table["value"].to_pylist()))
        return cache[stream]

    return incoherence(plan, read), (plan and plan["id"], compared)


def metadata_groups(manager):
    result = manager.graph_store.sparql_query(
        f'SELECT ?group WHERE {{ ?point <{PRODUCED_BY}> "{MEAN}" ; <{OUTPUT_GROUP}> ?group }}',
        wait_for_fresh=True,
    )
    return {str(row[0]) for row in result.get("rows", [])}


class Writer(threading.Thread):
    """Submits one request per tick: new readings for every stream, and now
    and then a late correction or a delete of an older reading."""

    def __init__(self, manager, start, oracle):
        super().__init__(daemon=True)
        self.manager, self.tick, self.oracle = manager, start, oracle
        self.stop = threading.Event()
        self.error = None
        self.receipt = None

    def run(self):
        try:
            while not self.stop.is_set():
                i = self.tick
                ts = BASE + i * STEP
                batch = {name: [(ts, float((i * 7 + k * 13) % 101))] for k, name in enumerate(STREAMS)}
                if i % 4 == 3:
                    batch["b"].append((ts - 2 * STEP, float(500 + i)))
                for name, items in batch.items():
                    for at, value in items:
                        self.oracle[(name, at)] = value
                self.receipt = self.manager.insert_timeseries_batch(
                    SOURCE, batch, request_id=f"put-{i}"
                )
                if i % 9 == 8:
                    old = BASE + (i - 3) * STEP
                    self.receipt = self.manager.core.ingest(
                        SOURCE, f"delete-{i}", delete_frame(REFS["c"], old)
                    )
                    self.oracle.pop(("c", old), None)
                self.tick += 1
                time.sleep(0.002)
        except BaseException as error:  # pragma: no cover - reported by the test
            self.error = error


def delete_frame(ref, ts):
    import polars as pl

    return pl.DataFrame(
        {
            "ref_uri": [ref],
            "ts": pl.Series([ts], dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
            "operation": ["delete"],
            "value": pl.Series([None], dtype=pl.Object),
        }
    )


class Reader(threading.Thread):
    def __init__(self, check):
        super().__init__(daemon=True)
        self.check = check
        self.stop = threading.Event()
        self.problems, self.seen, self.checks = [], set(), 0
        self.error = None

    def run(self):
        try:
            while not self.stop.is_set():
                problem, seen = self.check()
                self.checks += 1
                self.seen.add(seen)
                if problem:
                    self.problems.append(problem)
        except BaseException as error:  # pragma: no cover - reported by the test
            self.error = error


def open_manager(directory):
    manager = Manager(data_dir=directory, exact_only=True)
    manager.recover_apps()
    api.state.manager = manager
    # Small pages make one read span several engine queries at its epoch.
    api.state.read_batch_size = 97
    return manager, TestClient(api)


def membership(manager, groups):
    """A step that installs ``groups`` and checks that, once the edit
    returns, the metadata graph and the published plan agree."""

    def step():
        manager.insert_graph(membership_turtle(groups), source_id="membership", replace=True)
        plan = manager.core.engine.snapshot().plan
        assert metadata_groups(manager) == plan_groups(plan) == {
            f"urn:live:{group}" for group in groups
        }

    return step


def live_round(manager, http, steps, *, start, oracle, seal=True):
    """Write continuously while each step changes the plan in turn, sealing
    after every other step."""
    writer = Writer(manager, start, oracle)

    def snapshot_check():
        snapshot = manager.core.engine.snapshot()
        return check_snapshot(snapshot), (snapshot.plan or {}).get("id")

    readers = [Reader(snapshot_check), Reader(lambda: check_http(http, manager))]
    for thread in [writer, *readers]:
        thread.start()
    try:
        for index, step in enumerate(steps):
            time.sleep(0.15)
            step()
            if seal and index % 2:
                manager.core.engine.seal()
        time.sleep(0.15)
    finally:
        for thread in [writer, *readers]:
            thread.stop.set()
            thread.join(30)
    for thread in [writer, *readers]:
        assert thread.error is None, thread.error
    manager.core.wait(writer.receipt)
    for reader in readers:
        assert reader.problems == [], reader.problems[:3]
        assert reader.checks > 0
    return writer, readers


def assert_current(manager, oracle):
    snapshot = manager.core.engine.snapshot()
    assert check_snapshot(snapshot) is None
    for name in STREAMS:
        expected = {ts: value for (stream, ts), value in oracle.items() if stream == name}
        assert rows(snapshot.query_arrow(REFS[name], limit=LIMIT)) == expected
    return snapshot


def assert_history(manager, epochs):
    problems = {epoch: check_epoch(manager.core.engine, epoch) for epoch in epochs}
    assert {epoch: problem for epoch, problem in problems.items() if problem} == {}


def staged_activations(manager):
    return manager.core.processor.status()["stats"]["staged_activations"]


@pytest.mark.integration
@pytest.mark.parametrize("staged", [False, True], ids=["regular", "staged-rebuilds"])
def test_live_ingest_through_plan_changes_stays_coherent_across_restart(
    tmp_path, monkeypatch, staged
):
    # File-backed DuckDB stages rebuilds by default; compare with it off.
    if not staged:
        monkeypatch.setattr(manager_module, "Core", functools.partial(Core, stage_rebuilds=False))
    oracle = {}
    manager, http = open_manager(tmp_path)
    try:
        manager.insert_graph(
            "\n".join(
                f'<{ref}> <urn:acquirium#sourceId> "{SOURCE}" ; <urn:acquirium#refName> "{name}" .'
                for name, ref in REFS.items()
            ),
            source_id=SOURCE,
        )
        manager.insert_graph(membership_turtle(MEMBERSHIPS[0]), source_id="membership", replace=True)
        mean, total = apps()
        manager.deploy_app(mean)
        manager.deploy_app(total)
        first = manager.core.engine.snapshot().epoch

        # Metadata edits change existing operators' inputs, which Escapement
        # repairs incrementally; groups they add are new operators.
        writer, readers = live_round(
            manager, http, [membership(manager, groups) for groups in MEMBERSHIPS[1:]],
            start=0, oracle=oracle,
        )
        assert len(readers[0].seen) >= 3
        assert any(compared for _, compared in readers[1].seen)

        # Redefining the group app replaces its outputs, staged when enabled,
        # while the app reading them stays deployed.
        staged_before = staged_activations(manager)
        writer, _ = live_round(
            manager, http,
            [
                lambda: manager.deploy_app(apps("max")[0]),
                lambda: manager.deploy_app(apps("mean")[0]),
            ],
            start=writer.tick, oracle=oracle,
        )
        redefinitions = staged_activations(manager) - staged_before
        assert redefinitions >= 2 if staged else redefinitions == 0
        before = assert_current(manager, oracle)
        assert_history(manager, range(first, before.epoch + 1))
        tick = writer.tick
    finally:
        http.close()
        manager.close()

    manager, http = open_manager(tmp_path)
    try:
        snapshot = manager.core.engine.snapshot()
        # Recovery republishes nothing and keeps the exact plan and rows.
        assert snapshot.epoch == before.epoch
        assert snapshot.plan == before.plan
        assert_current(manager, oracle)
        assert_history(manager, range(first, snapshot.epoch + 1))

        live_round(
            manager, http,
            [membership(manager, MEMBERSHIPS[2]), membership(manager, MEMBERSHIPS[0])],
            start=tick, oracle=oracle,
        )
        after = assert_current(manager, oracle)
        assert_history(manager, range(snapshot.epoch, after.epoch + 1))
    finally:
        http.close()
        manager.close()
