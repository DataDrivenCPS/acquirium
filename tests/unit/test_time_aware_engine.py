"""The time-aware engine end to end: inbox, steps, deployments over time,
clipping, removal, late data, chains, loops, and rejected writes.

Each test runs a real DuckDB store and a versioned graph store with no
ontologies. Data goes through the inbox; the engine is not deferred, so every
submission runs a step and a read after a write sees the write.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from rdflib import Graph, URIRef

import acquirium.Storage.graph_store as graph_store_module
from acquirium.internals.intervals import NEG_INF, POS_INF, from_micros, to_micros
from acquirium.Materialization.admission import AdmissionError
from acquirium.Materialization.models import App, output
from acquirium.Materialization.planner import Deployment
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI
from acquirium.Storage.model_history import ModelHistory
from acquirium.Storage.metadata_outbox import execute
from acquirium.Storage.publication.types import MUTATION_SCHEMA

UTC = timezone.utc
MIN = 60 * 1_000_000
DAY = 86_400 * 1_000_000
BASE = datetime(2026, 1, 1, tzinfo=UTC)
NOW = BASE + timedelta(days=100)
REF = "https://brickschema.org/schema/Brick/ref#hasExternalReference"
QK = "http://qudt.org/schema/qudt/hasQuantityKind"
TEMPERATURE = "http://qudt.org/vocab/quantitykind/Temperature"


class FakeOntoEnv:
    def __init__(self, *args, **kwargs):
        self.add = MagicMock()
        self.close = MagicMock()

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls()

    def import_dependencies(self, graph):
        return None


def _open_store(tmp_path):
    """DuckDB by default; ENGINE_TEST_BACKEND=timescale runs the same tests on
    the database at ACQUIRIUM_TEST_PG_DSN, emptied first."""
    if os.getenv("ENGINE_TEST_BACKEND") == "timescale":
        from acquirium.Storage.timescale_store import TimescaleStore
        return TimescaleStore(dsn=os.environ["ACQUIRIUM_TEST_PG_DSN"], recreate=True)
    return DuckDBStore(tmp_path / "ts.duckdb")


class World:
    def __init__(self, tmp_path, monkeypatch):
        now = to_micros(NOW)
        for module in (graph_store_module,):
            monkeypatch.setattr(module, "now_micros", lambda: now)
        import acquirium.Materialization.runtime as runtime_module
        import acquirium.Materialization.deployments as deployments_module
        monkeypatch.setattr(runtime_module, "now_micros", lambda: now)
        monkeypatch.setattr(deployments_module, "now_micros", lambda: now)
        monkeypatch.setattr(graph_store_module, "OntoEnv", FakeOntoEnv)
        monkeypatch.setattr(graph_store_module, "load_bundled_graph", lambda fname, canonical: Graph())
        monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_start_query_rebuild_locked", lambda self: None)
        self.store = _open_store(tmp_path)
        self.graphs = graph_store_module.OxigraphGraphStore(
            store_path=tmp_path / "graph", env_root=tmp_path / "env", history=ModelHistory(self.store))
        self.views = Materializer(self.store, self.graphs)

    def close(self):
        self.graphs.close()
        self.store.close()

    # ---- model ----
    def model(self, text: str, **interval):
        bounds = {key: from_micros(to_micros(value)) if isinstance(value, datetime) else value
                  for key, value in interval.items()}
        self.graphs.insert_graph(text, format="nt", graph_uri=URIRef(PLANT_GRAPH_URI), **bounds)
        self.views.model_changed()
        self.views.step()

    def unmodel(self, text: str, **interval):
        self.graphs.sparql_update(f"DELETE DATA {{ {text} }}", graph_uri=URIRef(PLANT_GRAPH_URI), **interval)
        self.views.model_changed()
        self.views.step()

    # ---- data ----
    def write(self, ref: str, samples):
        table = pa.table({
            "operation": ["upsert"] * len(samples), "ref_uri": [ref] * len(samples),
            "ts": pa.array([t for t, _ in samples], pa.timestamp("us", tz="UTC")),
            "numeric_value": pa.array([float(v) for _, v in samples], pa.float64()),
            "text_value": pa.array([None] * len(samples), pa.string()),
        }, schema=MUTATION_SCHEMA)
        self.views.engine.submit("readings", table)

    def read(self, ref: str) -> list[tuple[datetime, float]]:
        with self.store._own_conn() as conn:
            rows = execute(conn,
                "SELECT t.ts, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                "WHERE r.ref_uri = ? AND NOT t.deleted ORDER BY t.ts", [ref]).fetchall()
        return [(ts.replace(tzinfo=UTC), value) for ts, value in rows]

    def deploy(self, app, start=NEG_INF):
        self.views.deploy(Deployment.from_class(app), start=start if isinstance(start, int) else to_micros(start))
        self.views.step()

    def outputs(self, view: str) -> dict[str, list]:
        plan, *_ = self.views.current_plan()
        found = {}
        for instance in plan.instances:
            if instance.view == view:
                for port in instance.outputs.values():
                    found[instance.key if instance.key != "*" else port.ref_name] = self.read(port.ref_uri)
        return found


@pytest.fixture
def world(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    yield w
    w.close()


def point(n: int) -> str:
    return f"<urn:ex/p{n}> <{REF}> <urn:ex/r{n}> .\n<urn:ex/p{n}> <{QK}> <{TEMPERATURE}> ."


def at(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


# -------------------- apps --------------------
class Double(App):
    name = "double"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        table = inputs["t"].collect()
        output["out"] = pa.table({"time": table["time"], "value": pa.compute.multiply(table["value"], 2.0)})


class RollingSum(App):
    """Sum over the last 10 minutes, at every input sample."""
    name = "rolling-sum"
    grouping = "per_match"
    lookback = "10m"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        import polars as pl
        frame = inputs["t"].df().sort("time")
        window = context.output_window
        rows = []
        for t in frame["time"].to_list():
            if window.start <= t <= window.end:
                total = frame.filter((pl.col("time") > t - timedelta(minutes=10)) & (pl.col("time") <= t))["value"].sum()
                rows.append((t, float(total)))
        output["out"] = pa.table({"time": pa.array([r[0] for r in rows], pa.timestamp("us", tz="UTC")),
                                  "value": pa.array([r[1] for r in rows], pa.float64())})


class TenMinuteMean(App):
    """The mean of each ten-minute bucket, written at the bucket's start."""
    name = "ten-minute-mean"
    grouping = "per_match"
    every = "10m"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        import polars as pl
        frame = inputs["t"].df().sort("time")
        if frame.is_empty():
            output["out"] = pa.table({"time": pa.array([], pa.timestamp("us", tz="UTC")), "value": pa.array([], pa.float64())})
            return
        means = frame.group_by_dynamic("time", every="10m").agg(pl.col("value").mean())
        window = context.output_window
        output["out"] = means.filter((pl.col("time") >= window.start) & (pl.col("time") <= window.end)).to_arrow()


class SumOfDoubles(App):
    name = "sum-of-doubles"
    grouping = "all_matches"
    outputs = {"total": output.named("total-of-doubles", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="d", app="double")
    def transform(self, inputs, output, context):
        import polars as pl
        frame = inputs["d"].df()
        if frame.is_empty():
            output["total"] = pa.table({"time": pa.array([], pa.timestamp("us", tz="UTC")), "value": pa.array([], pa.float64())})
            return
        total = frame.group_by("time").agg(pl.col("value").sum()).sort("time")
        output["total"] = total.to_arrow()


class Ewma(App):
    """y(t) = 0.5 x(t) + 0.5 y(t - 1 min): reads its own named output."""
    name = "ewma"
    grouping = "all_matches"
    named_inputs = {"x": "urn:ex/r1", "previous": "ewma-r1"}
    windows = {"x": ("0s", "0s"), "previous": ("-1m", "-1m")}
    outputs = {"y": output.named("ewma-r1", value_kind="numeric")}
    def transform(self, inputs, output, context):
        x = dict(zip(inputs["x"].collect()["time"].to_pylist(), inputs["x"].collect()["value"].to_pylist()))
        prev = dict(zip(inputs["previous"].collect()["time"].to_pylist(), inputs["previous"].collect()["value"].to_pylist()))
        rows = []
        for t, value in sorted(x.items()):
            if context.output_window.start <= t <= context.output_window.end:
                before = prev.get(t - timedelta(minutes=1), 0.0)
                rows.append((t, 0.5 * value + 0.5 * before))
        output["y"] = pa.table({"time": pa.array([r[0] for r in rows], pa.timestamp("us", tz="UTC")),
                                "value": pa.array([r[1] for r in rows], pa.float64())})


class Instant(App):
    """Reads its own named output with no delay: never admitted."""
    name = "instant"
    grouping = "all_matches"
    named_inputs = {"previous": "instant-out"}
    windows = {"previous": ("0s", "0s")}
    outputs = {"y": output.named("instant-out", value_kind="numeric")}
    def transform(self, inputs, output, context): pass


def ewma_reference(samples):
    out, prev = [], {}
    for t, value in sorted(samples):
        y = 0.5 * value + 0.5 * prev.get(t - timedelta(minutes=1), 0.0)
        prev[t] = y
        out.append((t, y))
    return out


# -------------------- tests --------------------
def test_a_step_applies_the_inbox_and_computes_in_one_transaction(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(0), 1.0), (at(1), 2.0)])
    world.deploy(Double)
    (values,) = world.outputs("double").values()
    assert values == [(at(0), 2.0), (at(1), 4.0)]
    world.write("urn:ex/r1", [(at(2), 5.0)])
    (values,) = world.outputs("double").values()
    assert values[-1] == (at(2), 10.0)


def test_a_deployment_writes_only_from_its_start_but_reads_before_it(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(0, 30, 5)])
    world.deploy(RollingSum, start=at(12))
    (values,) = world.outputs("rolling-sum").values()
    # outputs from minute 12 on; the one at 15 sums 10 and 15 (read before the start)
    assert [t for t, _ in values] == [at(15), at(20), at(25)]
    assert values[0][1] == 2.0


def test_reads_are_clipped_to_the_run_of_the_match(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(0, 30, 2)])
    world.deploy(RollingSum)
    # the point leaves the model over [10, 14): a gap in its life, two runs
    world.unmodel(point(1), effective_from=from_micros(to_micros(at(10))), effective_until=from_micros(to_micros(at(14))))
    (values,) = world.outputs("rolling-sum").values()
    by_time = dict(values)
    assert at(10) not in by_time and at(12) not in by_time
    # at minute 16 the 10-minute window would reach back to 8, but the run
    # started at 14: only 14 and 16 are read
    assert by_time[at(16)] == 2.0
    assert by_time[at(8)] == 5.0


def test_removal_retracts_outputs_from_its_time_and_purge_removes_all(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(10)])
    world.deploy(Double)
    world.views.remove("double", at=to_micros(at(5)))
    world.views.step()
    with world.store._own_conn() as conn:
        rows = execute(conn, "SELECT t.ts FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                            "JOIN streams s ON s.ref_uri = r.ref_uri WHERE s.source_id = 'derived:double' "
                            "AND NOT t.deleted ORDER BY t.ts").fetchall()
    assert [ts.replace(tzinfo=UTC) for (ts,) in rows] == [at(m) for m in range(5)]
    world.views.remove("double", purge=True)
    world.views.step()
    with world.store._own_conn() as conn:
        left = execute(conn, "SELECT count(*) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                            "JOIN streams s ON s.ref_uri = r.ref_uri WHERE s.source_id = 'derived:double' "
                            "AND NOT t.deleted").fetchone()[0]
    assert left == 0


def test_a_late_sample_recomputes_only_the_windows_it_touches(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(0, 60, 5)])
    world.deploy(RollingSum)
    calls_before = world.views.evaluator.last_report.calls
    world.write("urn:ex/r1", [(at(31), 100.0)])
    report = world.views.evaluator.last_report
    (values,) = world.outputs("rolling-sum").values()
    by_time = dict(values)
    # the 10-minute window at 31 holds 25, 30 and 31
    assert by_time[at(31)] == 102.0 and by_time[at(35)] == 102.0 and by_time[at(40)] == 102.0
    assert by_time[at(45)] == 2.0
    assert report.points_changed == 3  # 31 is new, 35 and 40 changed, nothing else
    assert calls_before >= 1


def test_a_chain_through_generated_outputs(world):
    world.model(point(1) + "\n" + point(2))
    world.write("urn:ex/r1", [(at(0), 1.0), (at(1), 2.0)])
    world.write("urn:ex/r2", [(at(0), 10.0)])
    world.deploy(Double)
    world.deploy(SumOfDoubles)
    (total,) = world.outputs("sum-of-doubles").values()
    assert total == [(at(0), 22.0), (at(1), 4.0)]
    world.write("urn:ex/r2", [(at(1), 1.0)])
    (total,) = world.outputs("sum-of-doubles").values()
    assert total == [(at(0), 22.0), (at(1), 6.0)]


def test_a_view_reading_its_own_output_with_a_delay(world):
    samples = [(at(m), float(m % 7)) for m in range(20)]
    world.write("urn:ex/r1", samples)
    world.deploy(Ewma, start=at(0))
    (values,) = world.outputs("ewma").values()
    assert values == ewma_reference(samples)
    # a late sample changes the loop from that sample on
    world.write("urn:ex/r1", [(at(3), 50.0)])
    changed = dict(samples) | {at(3): 50.0}
    (values,) = world.outputs("ewma").values()
    assert values == ewma_reference(sorted(changed.items()))


def test_a_loop_without_delay_is_rejected(world):
    with pytest.raises(AdmissionError, match="does not move into the past"):
        world.views.deploy(Deployment.from_class(Instant), start=to_micros(at(0)))
    assert world.views._log.current() == {}


def test_a_loop_asking_for_all_history_starts_at_its_earliest_input(world):
    world.write("urn:ex/r1", [(at(5), 1.0), (at(6), 1.0)])
    world.views.deploy(Deployment.from_class(Ewma), start=NEG_INF)
    (change,) = world.views._log.changes()
    assert change.start == to_micros(at(5))


def test_a_model_write_that_breaks_admission_is_undone(world):
    # Double writes a generated output per sensor; a per_match view with a
    # named output may have only one row at a time.
    class OneNamed(App):
        name = "one-named"
        grouping = "per_match"
        outputs = {"out": output.named("the-one", value_kind="numeric")}
        def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
        def transform(self, inputs, output, context): pass
    OneNamed.__module__ = __name__
    OneNamed.__qualname__ = "OneNamed"
    globals()["OneNamed"] = OneNamed
    world.model(point(1))
    world.deploy(OneNamed)
    head = world.graphs.history.head()
    with pytest.raises(AdmissionError, match="more than one instance"):
        world.graphs.insert_graph(point(2), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
        world.views.model_changed()
    assert world.graphs.history.head() == head
    assert len(world.graphs.model_at(to_micros(NOW))) == 2  # only point 1's two triples


def test_a_step_that_fails_halfway_leaves_nothing_and_the_next_one_redoes_it(world, monkeypatch):
    import acquirium.Materialization.evaluation as evaluation
    world.model(point(1))
    world.deploy(Double)
    original = evaluation._Reader.flush

    def broken(self):
        raise RuntimeError("the database went away")

    monkeypatch.setattr(evaluation._Reader, "flush", broken)
    with pytest.raises(RuntimeError):
        world.write("urn:ex/r1", [(at(0), 1.0)])
    assert world.read("urn:ex/r1") == []          # the raw rows did not commit either
    assert world.views.engine.pending() == 1       # the entry is still in the inbox
    monkeypatch.setattr(evaluation._Reader, "flush", original)
    world.views.step()
    assert world.views.engine.pending() == 0
    (values,) = world.outputs("double").values()
    assert values == [(at(0), 2.0)]


# -------------------- check and lineage --------------------
class Undeclared(Double):
    name = "undeclared"
    def transform(self, inputs, output, context):
        output["typo"] = inputs["t"].collect().select(["time", "value"])


class Tagged(App):
    name = "tagged"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric", data_source="lab", label=None)}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context): pass


def test_check_computes_without_writing_anything(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), float(m)) for m in range(5)])
    with world.store._own_conn() as conn:
        revision = execute(conn, "SELECT current_revision FROM system_state").fetchone()[0]
    result = world.views.check(Deployment.from_class(Double), limit=2)
    (binding,) = result["bindings"]
    assert binding["error"] is None
    assert binding["inputs"] == {"t": [{"ref_uri": "urn:ex/r1", "label": None, "unit": None}]}
    out = binding["outputs"]["out"]
    assert out["rows"] == 5 and out["truncated"] and [v["value"] for v in out["values"]] == [0.0, 2.0]
    with world.store._own_conn() as conn:
        assert execute(conn, "SELECT count(*) FROM streams WHERE source_id LIKE ?", ["derived:%"]).fetchone()[0] == 0
        assert execute(conn, "SELECT current_revision FROM system_state").fetchone()[0] == revision
    assert world.views._log.changes() == []


def test_check_reports_a_failing_or_undeclared_output_as_a_result(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(0), 1.0)])
    (binding,) = world.views.check(Deployment.from_class(Undeclared))["bindings"]
    assert "not declared" in binding["error"] and "typo" in binding["error"]


def test_the_outputs_present_now_are_published_into_acquiriums_graph(world):
    from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI
    from acquirium.internals.internals_namespaces import DATA_SOURCE, HAS_EXTERNAL_REFERENCE, PRODUCED_BY, PRODUCES
    from rdflib import Literal, RDFS
    world.model(point(1))
    world.deploy(Tagged)
    graph = world.graphs.source_dataset.graph(URIRef(ACQUIRIUM_GRAPH_URI))
    (port,) = world.views.current_plan()[0].instances[0].outputs.values()
    point_uri, ref = URIRef(port.point_uri), URIRef(port.ref_uri)
    assert (point_uri, HAS_EXTERNAL_REFERENCE, ref) in graph
    assert (point_uri, PRODUCED_BY, Literal("tagged")) in graph
    assert (point_uri, DATA_SOURCE, Literal("lab")) in graph
    assert any(graph.triples((None, PRODUCES, ref)))
    assert str(next(graph.objects(point_uri, RDFS.label))).startswith("tagged[out]")
    # planning never reads this graph: the model has no derived points
    assert not any(world.graphs.model_at(to_micros(NOW)).triples((point_uri, None, None)))


# -------------------- a loop across two views --------------------
def _named_series(inputs, name):
    table = inputs[name].collect()
    return dict(zip(table["time"].to_pylist(), table["value"].to_pylist()))


def _series(rows):
    return pa.table({"time": pa.array([r[0] for r in rows], pa.timestamp("us", tz="UTC")),
                     "value": pa.array([r[1] for r in rows], pa.float64())})


class Estimator(App):
    """e(t) = x(t) + 0.1 c(t - 2 min)."""
    name = "estimator"
    grouping = "all_matches"
    named_inputs = {"x": "urn:ex/r1", "c": "controller-out"}
    windows = {"x": ("0s", "0s"), "c": ("-2m", "-2m")}
    outputs = {"e": output.named("estimator-out", value_kind="numeric")}
    def transform(self, inputs, output, context):
        x, c = _named_series(inputs, "x"), _named_series(inputs, "c")
        window = context.output_window
        output["e"] = _series([(t, v + 0.1 * c.get(t - timedelta(minutes=2), 0.0))
                               for t, v in sorted(x.items()) if window.start <= t <= window.end])


class Controller(App):
    """c(t) = mean of e over [t, t + 1 min], at every e: reads ahead."""
    name = "controller"
    grouping = "all_matches"
    named_inputs = {"e": "estimator-out"}
    windows = {"e": ("0s", "1m")}
    outputs = {"c": output.named("controller-out", value_kind="numeric")}
    def transform(self, inputs, output, context):
        e = _named_series(inputs, "e")
        window = context.output_window
        rows = []
        for t in sorted(e):
            if window.start <= t <= window.end:
                ahead = [v for s, v in e.items() if t <= s <= t + timedelta(minutes=1)]
                rows.append((t, sum(ahead) / len(ahead)))
        output["c"] = _series(rows)


def loop_reference(x: dict) -> tuple[dict, dict]:
    """e and c in time order: e(t) needs c(t - 2 min); c(s) needs e on
    [s, s + 1 min], known once t passes s + 1 min."""
    e, c = {}, {}
    times = sorted(x)
    for t in times:
        e[t] = x[t] + 0.1 * c.get(t - timedelta(minutes=2), 0.0)
        for s in [s for s in times if s not in c and s + timedelta(minutes=1) <= t]:
            ahead = [e[v] for v in times if s <= v <= s + timedelta(minutes=1)]
            c[s] = sum(ahead) / len(ahead)
    for s in times:
        if s not in c:
            ahead = [e[v] for v in times if s <= v <= s + timedelta(minutes=1)]
            c[s] = sum(ahead) / len(ahead)
    return e, c


def test_a_loop_across_two_views_with_lookahead(world):
    x = {at(m): float((m * 3) % 5) for m in range(12)}
    world.write("urn:ex/r1", sorted(x.items()))
    world.deploy(Estimator, start=at(0))
    world.deploy(Controller, start=at(0))
    plan = world.views.current_plan()[0]
    assert plan.schedule.loops == {"estimator", "controller"}
    assert plan.schedule.epsilon == 30 * 1_000_000  # (-2 min + 1 min) over two edges
    want_e, want_c = loop_reference(x)
    (e,) = world.outputs("estimator").values()
    (c,) = world.outputs("controller").values()
    assert e == sorted(want_e.items()) and c == sorted(want_c.items())
    # a late sample changes the loop from there on
    x[at(4)] = 40.0
    world.write("urn:ex/r1", [(at(4), 40.0)])
    want_e, want_c = loop_reference(x)
    (e,) = world.outputs("estimator").values()
    (c,) = world.outputs("controller").values()
    assert e == sorted(want_e.items()) and c == sorted(want_c.items())


# -------------------- batching --------------------
def _ops(world):
    world.model(point(1) + "\n" + point(2))
    world.deploy(Double)
    world.deploy(RollingSum)
    world.deploy(SumOfDoubles)
    world.deploy(TenMinuteMean, start=at(5))
    world.write("urn:ex/r1", [(at(m), float(m % 4)) for m in range(0, 40, 3)])
    world.write("urn:ex/r2", [(at(m), float(m % 5)) for m in range(1, 40, 4)])
    world.unmodel(point(2), effective_from=from_micros(to_micros(at(10))), effective_until=from_micros(to_micros(at(20))))
    world.write("urn:ex/r1", [(at(7), 9.0), (at(31), 1.0)])
    world.views.remove("rolling-sum", at=to_micros(at(30)))
    world.views.step()


def _all_outputs(world):
    with world.store._own_conn() as conn:
        rows = execute(conn, "SELECT s.ref_uri, t.ts, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                             "JOIN streams s ON s.ref_uri = r.ref_uri WHERE s.source_id LIKE ? AND NOT t.deleted", ["derived:%"]).fetchall()
    return sorted((ref, ts.replace(tzinfo=UTC), value) for ref, ts, value in rows)


def test_outputs_do_not_depend_on_how_writes_are_batched_into_steps(tmp_path, monkeypatch):
    eager = World(tmp_path / "eager", monkeypatch)
    lazy = World(tmp_path / "lazy", monkeypatch)
    try:
        _ops(eager)
        lazy.views.engine.deferred = True
        lazy.views.step = lambda: None  # every write waits in the inbox
        _ops(lazy)
        type(lazy.views).step(lazy.views)
        assert lazy.views.engine.pending() == 0
        assert _all_outputs(lazy) == _all_outputs(eager)
        assert _all_outputs(eager)
    finally:
        eager.close()
        lazy.close()


# -------------------- redefinition and a write that closes a loop --------------------
class Scale(App):
    name = "scale"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def __init__(self, factor: float = 2.0):
        self.factor = factor
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        table = inputs["t"].collect()
        output["out"] = pa.table({"time": table["time"], "value": pa.compute.multiply(table["value"], self.factor)})


def test_a_redefinition_from_a_time_keeps_the_outputs_before_it(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(10)])
    world.views.deploy(Deployment.from_class(Scale, parameters={"factor": 2.0}), start=NEG_INF)
    world.views.deploy(Deployment.from_class(Scale, parameters={"factor": 3.0}), start=to_micros(at(5)))
    world.views.step()
    plan = world.views.current_plan()[0]
    (ref,) = {port.ref_uri for i in plan.instances for port in i.outputs.values()}
    assert world.read(ref) == [(at(m), 2.0 if m < 5 else 3.0) for m in range(10)]


class ReadsByName(App):
    """Reads the other view's named output at the same time."""
    name = "reads-by-name"
    grouping = "all_matches"
    named_inputs = {"b": "b-out"}
    windows = {"b": ("0s", "0s")}
    outputs = {"a": output.named("a-out", value_kind="numeric", point_uri="urn:ex/p9")}
    def transform(self, inputs, output, context): pass


class SelectsPoint(App):
    """Selects temperature points; the other view's output becomes one when
    the model says its point measures temperature."""
    name = "selects-point"
    grouping = "all_matches"
    outputs = {"b": output.named("b-out", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context): pass


def test_a_model_write_that_closes_a_loop_without_delay_is_undone(world):
    world.model(point(1))
    world.deploy(ReadsByName, start=at(0))
    world.deploy(SelectsPoint, start=at(0))
    head = world.graphs.history.head()
    # p9 is reads-by-name's output point; saying it measures temperature makes
    # selects-point read it at the same time it writes what reads-by-name reads
    world.graphs.insert_graph(f"<urn:ex/p9> <{QK}> <{TEMPERATURE}> .", format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    with pytest.raises(AdmissionError, match="does not move into the past"):
        world.views.model_changed()
    assert world.graphs.history.head() == head
    assert not any(world.graphs.source_dataset.graph(URIRef(PLANT_GRAPH_URI)).triples((URIRef("urn:ex/p9"), None, None)))
    world.views.step()  # the engine carries on with the admitted model
    assert not world.views.failures()


# -------------------- clipping of named inputs and of all-matches rows --------------------
class SumWithReference(App):
    """Per sensor: its value plus the sum of a fixed reference stream over
    the last 10 minutes, read by name."""
    name = "sum-with-reference"
    grouping = "per_match"
    named_inputs = {"ref": "urn:ex/reference"}
    windows = {"ref": ("-10m", "0s")}
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        t, ref = _named_series(inputs, "t"), _named_series(inputs, "ref")
        window = context.output_window
        output["out"] = _series([(s, v + sum(r for u, r in ref.items() if s - timedelta(minutes=10) <= u <= s))
                                 for s, v in sorted(t.items()) if window.start <= s <= window.end])


class RollingTotal(App):
    """All sensors together: the sum of every sample in the last 10 minutes."""
    name = "rolling-total"
    grouping = "all_matches"
    lookback = "10m"
    every = None
    outputs = {"total": output.named("rolling-total", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        table = inputs["t"].collect()
        samples = list(zip(table["time"].to_pylist(), table["value"].to_pylist()))
        window = context.output_window
        times = sorted({t for t, _ in samples if window.start <= t <= window.end})
        output["total"] = _series([(t, sum(v for u, v in samples if t - timedelta(minutes=10) < u <= t)) for t in times])


def test_a_per_match_named_input_is_clipped_by_the_run_of_its_row(world):
    world.model(point(1))
    world.write("urn:ex/reference", [(at(m), 1.0) for m in range(20)])
    world.write("urn:ex/r1", [(at(m), 0.0) for m in range(20)])
    world.deploy(SumWithReference)
    # p1 leaves the model over [5, 8): after it comes back, the reference is
    # read only from 8 on
    world.unmodel(point(1), effective_from=from_micros(to_micros(at(5))), effective_until=from_micros(to_micros(at(8))))
    (values,) = world.outputs("sum-with-reference").values()
    by_time = dict(values)
    assert by_time[at(4)] == 5.0         # reference at 0..4
    assert at(6) not in by_time
    assert by_time[at(9)] == 2.0         # reference at 8 and 9 only, not 0..4
    assert by_time[at(19)] == 11.0       # 9..19 inside the run


def test_an_all_matches_view_clips_each_row_by_its_own_run(world):
    world.model(point(1) + "\n" + point(2))
    world.write("urn:ex/r1", [(at(m), 1.0) for m in range(20)])
    world.write("urn:ex/r2", [(at(m), 10.0) for m in range(20)])
    world.deploy(RollingTotal)
    world.unmodel(point(2), effective_from=from_micros(to_micros(at(5))), effective_until=from_micros(to_micros(at(8))))
    (values,) = world.outputs("rolling-total").values()
    by_time = dict(values)
    # at 9: r1 over (-1, 9] is 10 samples; r2 only from its new run, 8 and 9
    assert by_time[at(9)] == 10 * 1.0 + 2 * 10.0
    # inside p2's gap only r1 counts
    assert by_time[at(6)] == 7 * 1.0


class Label(App):
    """A text output: "high" or "low" at every sample."""
    name = "label"
    grouping = "per_match"
    outputs = {"state": output.stream(value_kind="text")}
    def build_query(self, plant): return plant.query().measurement(alias="t", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context):
        table = inputs["t"].collect()
        output["state"] = pa.table({"time": table["time"],
                                    "value": pa.array(["high" if v > 5 else "low" for v in table["value"].to_pylist()], pa.string())})


def test_text_outputs_are_written_and_corrected(world):
    world.model(point(1))
    world.write("urn:ex/r1", [(at(m), float(m)) for m in range(10)])
    world.deploy(Label)
    plan = world.views.current_plan()[0]
    (ref,) = {port.ref_uri for i in plan.instances for port in i.outputs.values()}
    with world.store._own_conn() as conn:
        rows = execute(conn, "SELECT t.ts, t.text_value, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                             "WHERE r.ref_uri = ? AND NOT t.deleted ORDER BY t.ts", [ref]).fetchall()
    assert [(text, numeric) for _, text, numeric in rows] == [("low", None)] * 6 + [("high", None)] * 4
    world.write("urn:ex/r1", [(at(2), 9.0)])     # a late correction flips one label
    with world.store._own_conn() as conn:
        rows = execute(conn, "SELECT t.text_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id "
                             "WHERE r.ref_uri = ? AND NOT t.deleted ORDER BY t.ts", [ref]).fetchall()
    assert [text for (text,) in rows] == ["low", "low", "high", "low", "low", "low", "high", "high", "high", "high"]
    assert world.views.evaluator.last_report.points_changed == 1
