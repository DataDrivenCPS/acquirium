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
        assert execute(conn, "SELECT count(*) FROM streams WHERE source_id LIKE 'derived:%'").fetchone()[0] == 0
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
