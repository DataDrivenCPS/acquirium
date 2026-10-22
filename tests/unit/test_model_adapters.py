"""Explicit adapter declarations, local readers, and batched recording."""
from datetime import datetime, timedelta, timezone
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient
from httpx import HTTPStatusError

import acquirium as aq
from acquirium.Client.client import AcquiriumClient
from acquirium.Experiments import ExperimentStore, Study, StudyService, _stream_name
from acquirium.Storage.duckdb_store import DuckDBStore

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
PAYLOAD = {"a.b": {"arbitrary key": [None, {}, [], {"https://example.org/key": "value"}]},
           "role": "user value", "unit": None, "": True, "日本語": {"x": np.int64(3)},
           "note": "https://example.org/a contains a note"}


@pytest.fixture
def model_api(tmp_path, monkeypatch):
    store = DuckDBStore(tmp_path / "models.duckdb")
    ledger = ExperimentStore.standalone(store, tmp_path / "artifacts")
    module = import_module("acquirium.Server.app")
    monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=ledger), raising=False)
    client = AcquiriumClient()
    client._http.close()
    client._http = TestClient(module.app)
    client.insert_timeseries_batch = Mock(return_value={"ok": True})
    client.record_experiment_batch = Mock(wraps=client.record_experiment_batch)
    client.resolve = Mock(side_effect=lambda value, **kwargs: value)
    client.sync_experiment_metadata = Mock(return_value={"ok": True})
    ac = SimpleNamespace(client=client, register_streams=Mock())
    study = Study(ac, ledger.define("models"))
    try:
        yield SimpleNamespace(study=study, ac=ac, client=client, ledger=ledger)
    finally:
        client._http.close()
        store.close()


def fake_adapter():
    state = SimpleNamespace(values=dict(cost=np.float64(4), status="optimal", config=PAYLOAD,
        table=pl.DataFrame({"x": [1, 2]}), level=np.float64(3), flow={("p1",): 5, ("p2",): 7}), reads=[])
    def read(label):
        state.reads.append(label)
        return state.values[label]
    adapter = aq.adapters.define()
    adapter.scalar("cost", read=lambda: read("cost"), unit="urn:unit:USD", metadata={"stage": "final"})
    adapter.text("status", read=lambda: read("status"))
    adapter.input.json("config", read=lambda: read("config"), metadata=PAYLOAD)
    adapter.table("table", read=lambda: read("table"))
    adapter.timeseries("level", read=lambda: read("level"))
    adapter.add(aq.Component("flow", "timeseries", unit="urn:unit:L-PER-SEC", keys=("pump",),
        observed={("p1",): "urn:plant:p1", ("p2",): "urn:plant:p2"}), read=lambda: read("flow"))
    return adapter, state


def test_namespaces_declaration_filters_and_idempotency(model_api):
    study = model_api.study
    adapter, state = fake_adapter()
    variables = study.declare_from(adapter, include=["c*", "f*", "level"], exclude="config")
    assert list(variables) == ["cost", "level", "flow"]
    assert state.reads == []
    again = study.declare_from(adapter)
    assert again["cost"] is variables["cost"]
    assert list(again.input) == ["config"]
    assert set(again.output) == {"cost", "status", "table", "level", "flow"}
    assert again["config"].role == "input"
    assert again["cost"].metadata == {"stage": "final"}
    assert again["cost"].unit == "urn:unit:USD"
    conflict = aq.adapters.define()
    conflict.scalar("cost", read=lambda: 9, unit="urn:unit:EUR", metadata={"stage": "final"})
    with pytest.raises(HTTPStatusError) as caught:
        study.declare_from(conflict)
    assert "declared differently" in caught.value.response.json()["detail"]
    with pytest.raises(TypeError, match="role"):
        study.declare_from(adapter, role="input")
    with pytest.raises(ValueError, match="no adapter components"):
        study.declare_from(adapter, include="missing*")
    with pytest.raises(ValueError, match="empty"):
        study.declare_from(aq.adapters.define())


def test_record_values_sample_and_role_views(model_api):
    study, client, ac = model_api.study, model_api.client, model_api.ac
    adapter, state = fake_adapter()
    variables = study.declare_from(adapter)
    with pytest.raises(RuntimeError, match="start"):
        variables.sample(at=T0)
    run = study.start()
    variables.input.record_values()
    assert state.reads == ["config"]
    state.reads.clear()
    assert len(variables.sample(at=T0)) == 3
    assert state.reads == ["level", "flow"]
    assert client.record_experiment_batch.call_count == 2
    entries = client.record_experiment_batch.call_args.args[1]
    assert [entry["form"] for entry in entries] == ["samples"] * 3
    assert entries[1]["key"] == ["p1"]
    assert entries[2]["samples"] == [[T0.isoformat(), 7]]
    ac.register_streams.assert_not_called()
    client.insert_timeseries_batch.assert_not_called()
    assert [obs.key for obs in run.observations(variables["flow"]).all()] == [("p1",), ("p2",)]
    variables.sample(at=T0 + timedelta(hours=1))
    assert client.record_experiment_batch.call_count == 3
    assert len(variables.output.record_values()) == 3
    assert len(run.observations(variables["config"]).all()) == 1
    assert run.observations(variables["cost"]).latest().value == 4
    assert run.observations(variables["table"]).latest().dataframe().equals(state.values["table"])
    assert len(run.observations(variables["level"]).all()) == 2
    run.finish()
    study.start()
    variables.sample(at=T0)
    assert client.record_experiment_batch.call_count == 5
    assert not hasattr(variables, "record_all")


def test_json_values_and_arbitrary_nested_metadata_survive_reopening(model_api):
    adapter, state = fake_adapter()
    variables = model_api.study.declare_from(adapter)
    run = model_api.study.start()
    variables.record_values()
    expected = aq.ExperimentValues.to_json(PAYLOAD)
    assert variables["config"].metadata == expected
    assert run.observations(variables["config"]).latest().value == expected
    reopened = StudyService(model_api.ac).get("models")
    assert reopened.variables["config"].metadata == expected
    assert reopened.variables["config"].role == "input"
    assert reopened.variables["config"].unit is None
    assert reopened.variables.frame().filter(pl.col("label") == "config")["metadata"].item() == expected


def test_preview_is_local_and_matches_recording(model_api):
    adapter, state = fake_adapter()
    assert adapter.frame().height == 6
    assert state.reads == []
    preview = adapter.preview()
    assert preview["cost"] == 4
    assert preview["config"]["a.b"]["arbitrary key"][:3] == [None, {}, []]
    model_api.client.insert_timeseries_batch.assert_not_called()
    model_api.client.record_experiment_batch.assert_not_called()
    model_api.ac.register_streams.assert_not_called()
    variables = model_api.study.declare_from(adapter)
    assert variables.output.preview()["level"] == preview["level"]


def test_model_time_is_forwarded_unchanged_and_timestamp_only_labels_sample(model_api):
    adapter = aq.adapters.define()
    indexed = Mock(side_effect=lambda t: {0: 5, None: 6}[t])
    current = Mock(return_value=7)
    adapter.timeseries("indexed", read_at=indexed)
    adapter.timeseries("current", read=current)
    variables = model_api.study.declare_from(adapter)
    run = model_api.study.start()
    with pytest.raises(ValueError, match="indexed.*requires model_time") as caught:
        variables.sample(at=T0)
    assert caught.value.__cause__ is not None
    model_api.ac.register_streams.assert_not_called()
    variables.sample(at=T0 + timedelta(days=100), model_time=0)
    variables.sample(at=T0, model_time=None)
    assert [call.args[0] for call in indexed.call_args_list] == [0, None]
    assert all(not call.args for call in current.call_args_list)
    assert run.observations(variables["indexed"]).all()[0].range_start == T0 + timedelta(days=100)
    assert variables.preview(model_time=0) == {"indexed": 5, "current": 7}


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), "bad", [1],
                                 aq.Stream("urn:external").between(T0, T0)])
def test_invalid_sample_records_nothing(model_api, bad):
    adapter = aq.adapters.define()
    adapter.timeseries("good", read=lambda: 1)
    adapter.timeseries("bad", read=lambda: bad)
    variables = model_api.study.declare_from(adapter)
    run = model_api.study.start()
    with pytest.raises(ValueError, match="bad"):
        variables.sample(at=T0)
    model_api.ac.register_streams.assert_not_called()
    model_api.client.insert_timeseries_batch.assert_not_called()
    model_api.client.record_experiment_batch.assert_not_called()
    assert run.observations().all() == []


def test_reader_exception_includes_source_and_context(model_api):
    adapter = aq.adapters.define()
    def read(t):
        raise KeyError(t)
    adapter.add(aq.Component("flow", "timeseries", source_path="block.flow"), read_at=read)
    variables = model_api.study.declare_from(adapter)
    model_api.study.start()
    with pytest.raises(ValueError, match="flow.*block.flow.*model_time=2") as caught:
        variables.sample(at=T0, model_time=2)
    assert isinstance(caught.value.__cause__, KeyError)
    model_api.ac.register_streams.assert_not_called()


def test_frozen_bindings_and_fresh_models_reuse_declarations(model_api):
    adapter, state = fake_adapter()
    variables = model_api.study.declare_from(adapter)
    adapter.scalar("later", read=lambda: 9)
    assert "later" not in variables
    descriptor = adapter.components()[0]
    descriptor.metadata["stage"] = "changed"
    state.values["cost"] = 8
    model_api.study.start()
    variables.record_values()
    assert variables["cost"].observations().latest().value == 8
    assert variables["cost"].metadata == {"stage": "final"}
    replacement, fresh = fake_adapter()
    fresh.values["cost"] = 99
    new_variables = model_api.study.declare_from(replacement)
    assert new_variables["cost"] is variables["cost"]
    new_variables.record_values()
    assert variables["cost"].observations().latest().value == 99


def test_builder_validation_and_explicit_json_metadata():
    adapter = aq.adapters.define()
    for kwargs in ({}, {"read": lambda: 1, "read_at": lambda t: 1}):
        with pytest.raises(ValueError, match="exactly one"):
            adapter.timeseries("bad", **kwargs)
    with pytest.raises(TypeError, match="callable"):
        adapter.scalar("bad", read=3)
    with pytest.raises(TypeError, match="metadata"):
        adapter.json("bad", read=lambda: {}, metadata=[])
    with pytest.raises(TypeError):
        adapter.scalar("bad", read=lambda: 1, pump="p1")
    adapter.input.scalar("cost", read=lambda: 1)
    with pytest.raises(ValueError, match="duplicate"):
        adapter.output.scalar("cost", read=lambda: 2)
    with pytest.raises(ValueError, match="NaN"):
        adapter.json("bad", read=lambda: {}, metadata={"x": float("nan")})


@pytest.mark.parametrize("failure", ["registration", "insertion", "observation", "outbox"])
def test_transport_retries_and_atomic_observation_batches(model_api, monkeypatch, failure):
    variables = model_api.study.declare_from(fake_adapter()[0])
    run = model_api.study.start()
    ledger = model_api.ledger
    ledger.sync_metadata_graph()
    execute = ledger._execute
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise RuntimeError("temporary")
        if failure == "registration":
            patch.setattr(ledger.store, "_ensure_stream_refs", fail)
        elif failure == "insertion":
            patch.setattr(ledger.store, "_insert_frame", fail)
        elif failure == "outbox":
            patch.setattr(ledger.metadata, "enqueue", fail)
        else:
            def insert(conn, statement, parameters=None):
                if statement.startswith("INSERT INTO experiment_observations"):
                    raise RuntimeError("temporary")
                return execute(conn, statement, parameters)
            patch.setattr(ledger, "_execute", insert)
        with pytest.raises(RuntimeError, match="temporary"):
            variables.sample(at=T0)
    assert run.observations().all() == []
    with ledger.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM timeseries").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM streams").fetchone()[0] == 0
    variables.sample(at=T0)
    assert len(run.observations().all()) == 3
    assert model_api.client.record_experiment_batch.call_count == 2


def test_pyomo_explicit_sources_units_and_mutable_values(model_api):
    pyo = pytest.importorskip("pyomo.environ")
    pytest.importorskip("pint")
    model = pyo.ConcreteModel()
    model.time = pyo.Set(initialize=[0, 2])
    model.pump = pyo.Set(initialize=["p1", "p2"])
    model.tariff = pyo.Param(initialize=2, mutable=True)
    model.cost = pyo.Var(initialize=4, units=pyo.units.kW, doc="Power objective")
    model.pressure = pyo.Var(initialize=5, units=pyo.units.Pa)
    model.flow = pyo.Var(model.pump, model.time, initialize=lambda m, p, t: t + (1 if p == "p1" else 2), units=pyo.units.L / pyo.units.s)
    adapter = aq.adapters.pyomo(model)
    adapter.input.scalar("tariff", model.tariff)
    descriptor = adapter.scalar("cost", model.cost, metadata=PAYLOAD)
    descriptor.metadata["changed"] = "detached descriptor"
    adapter.timeseries("pressure", model.pressure)
    for pump in model.pump:
        adapter.timeseries(f"flow/{pump}", model.flow, read_at=lambda t, pump=pump: model.flow[pump, t])
    assert adapter.frame().filter(pl.col("label") == "cost")["description"].item() == "Power objective"
    preview = adapter.preview(model_time=2)
    assert preview["flow/p1"] == 3 and preview["flow/p2"] == 4
    variables = model_api.study.declare_from(adapter)
    assert variables["cost"].source_path == "cost"
    assert variables["cost"].unit == "kW"
    assert "changed" not in variables["cost"].metadata
    run = model_api.study.start()
    variables.input.record_values()
    model.cost.set_value(7)
    model.pressure.set_value(12)
    variables.sample(at=T0, model_time=2)
    variables.output.record_values()
    assert run.observations(variables["cost"]).latest().value == 7
    assert run.observations(variables["pressure"]).latest().range_start == T0
    with pytest.raises(ValueError, match="explicit read"):
        adapter.scalar("bad", model.flow)
    with pytest.raises(TypeError, match="callable"):
        adapter.scalar("bad reader", model.cost, read=3)
    with pytest.raises(TypeError, match="callable"):
        adapter.timeseries("bad indexed reader", model.flow, read_at=3)
    with pytest.raises(TypeError, match="start"):
        aq.adapters.pyomo(model, start=T0)


def test_pyomo_unit_assertions_fail_before_declaration(model_api):
    pyo = pytest.importorskip("pyomo.environ")
    pytest.importorskip("pint")
    model = pyo.ConcreteModel()
    model.flow = pyo.Var(initialize=1, units=pyo.units.m**3 / pyo.units.hour)
    adapter = aq.adapters.pyomo(model)
    adapter.timeseries("flow", model.flow, unit="L-PER-SEC")
    model_api.client.get_conversion_factors = Mock(return_value=dict(compatible=True,
        from_multiplier=1/3600, to_multiplier=.001, from_offset=0, to_offset=0))
    with pytest.raises(ValueError, match="requires conversion"):
        model_api.study.declare_from(adapter)
    assert len(model_api.study.variables) == 0
    model_api.client.get_conversion_factors.return_value = dict(compatible=True,
        from_multiplier=1, to_multiplier=1, from_offset=0, to_offset=0)
    variables = model_api.study.declare_from(adapter)
    assert variables["flow"].unit == "L-PER-SEC"


def test_hand_declared_keyed_series_remain_supported(model_api):
    flow = model_api.study.output("flow").timeseries(keys=("pump", "zone"), observed="urn:plant:flow")
    run = model_api.study.start()
    streams = flow.record({("p1", "a,b"): [(T0, np.float32(5))], (1, "x"): [(T0, 2)], ("1", "x"): [(T0, 3)]})
    assert set(streams) == {("p1", "a,b"), (1, "x"), ("1", "x")}
    from acquirium.internals.models import compute_ref_uri
    refs = [stream.ref_uri for stream in streams.values()]
    assert refs == [str(compute_ref_uri(f"experiment/{run.run_id}", _stream_name(flow.variable_id, key))) for key in (("p1", "a,b"), (1, "x"), ("1", "x"))]
    assert len(run.observations().all()) == 3
    for value in ({"p1": [(T0, 5)]}, {("p1",): [(T0, 5)]}, {("p1", None): [(T0, 5)]}):
        with pytest.raises(ValueError, match="key"):
            flow.record(value)


def test_keyed_registration_distinguishes_numeric_types(model_api):
    flow = model_api.study.output("flow").timeseries(keys=("pump",))
    model_api.study.start()
    for key in (1, True, 1.0, 0.0, -0.0):
        flow.record({(key,): [(T0, 5)]})
    assert model_api.client.record_experiment_batch.call_count == 5
    with model_api.ledger.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM streams").fetchone()[0] == 5


def test_importing_core_does_not_import_pyomo():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, "-c", "import acquirium, sys; assert 'pyomo.environ' not in sys.modules"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_declaration_does_not_evaluate_uninitialized_pyomo_values(model_api):
    pyo = pytest.importorskip("pyomo.environ")
    model = pyo.ConcreteModel()
    model.x = pyo.Var()
    adapter = aq.adapters.pyomo(model)
    adapter.scalar("x", model.x)
    assert adapter.frame().height == 1
    variables = model_api.study.declare_from(adapter)
    run = model_api.study.start()
    with pytest.raises(ValueError, match="could not read 'x'.*source='x'"):
        variables.record_values()
    assert run.observations().all() == []


def test_legacy_metadata_named_like_an_adapter_field_is_not_reinterpreted(model_api):
    variable = model_api.study.output("legacy").json(adapter_metadata="ordinary text")
    assert variable.metadata == {"adapter_metadata": "ordinary text"}


def test_table_recording_serializes_once_and_preview_preserves_frame(model_api, monkeypatch):
    import acquirium.ExperimentValues as values
    adapter = aq.adapters.define()
    frame = pl.DataFrame({"cost": [1, 2]})
    adapter.table("table", read=lambda: frame)
    assert adapter.preview()["table"] is frame
    variables = model_api.study.declare_from(adapter)
    model_api.study.start()
    serialize = Mock(wraps=values.parquet)
    monkeypatch.setattr(values, "parquet", serialize)
    variables.record_values()
    assert serialize.call_count == 1
    assert variables["table"].observations().latest().dataframe().equals(frame)
    assert model_api.client.record_experiment_batch.call_count == 1


def test_custom_adapters_pass_through_shared_validation(model_api):
    adapter = SimpleNamespace(components=lambda: [aq.Component("bad", "scalar", role="annotation")])
    with pytest.raises(ValueError, match="inputs or outputs"):
        model_api.study.declare_from(adapter)
    assert model_api.ledger.variables(model_api.study.study_id) == []


def test_hand_and_model_declarations_share_metadata_and_query_paths(model_api):
    study = model_api.study
    first = study.output("cost").scalar(unit="urn:unit:USD", objective="minimize",
        metadata={"a.b": [None, {}, []]})
    adapter = aq.adapters.define()
    adapter.scalar("cost", read=lambda: 1, unit="urn:unit:USD",
        metadata={"objective": "minimize", "a.b": [None, {}, []]})
    assert study.declare_from(adapter)["cost"] is first
    assert first.metadata == {"objective": "minimize", "a.b": [None, {}, []]}
    assert first.unit == "urn:unit:USD"
    assert "unit" not in first.metadata
    # A separate namespaced value cannot silently replace a descriptive value.
    with pytest.raises(ValueError, match="objective"):
        study.output("conflict").scalar(objective="minimize", metadata={"objective": "maximize"})
