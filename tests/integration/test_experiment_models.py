"""Whole-model recording through a real local HTTP server."""
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from requests import HTTPError

import acquirium as aq


@pytest.mark.integration
def test_model_sweep_roundtrip_request_budget_and_keyed_deletion(tmp_path, monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    from types import SimpleNamespace
    from acquirium.ExperimentValues import to_json
    state = SimpleNamespace(scenario=0, step=0)
    payload = {"a.b": {"arbitrary key": [None, {}, [], {"https://example.org/key": "value"}]},
               "role": "user value", "unit": None, "": True,
               "note": "https://example.org/a contains a note"}
    adapter = aq.adapters.define()
    adapter.scalar("cost", read=lambda: 100 + state.scenario)
    adapter.text("status", read=lambda: "ok")
    adapter.input.json("config", read=lambda: payload, metadata=payload)
    adapter.table("capacity", read=lambda: pl.DataFrame({"pump": ["p1", "p2"], "value": [10, 20]}))
    adapter.timeseries("pressure", read=lambda: state.step, metadata={"location": "tank", **payload})
    adapter.add(aq.Component("flow", "timeseries", unit="L-PER-SEC", keys=("pump",),
        quantity_kind="http://qudt.org/vocab/quantitykind/VolumeFlowRate",
        observed={("p1",): "urn:plant:p1-flow"}),
        read_at=lambda t: {("p1",): state.scenario + t, ("p2",): 10 + state.scenario + t})

    ac = aq.init(data_dir=tmp_path / "local-server")
    try:
        study = ac.study.define("model sweep")
        variables = study.declare_from(adapter)
        runs = []
        for scenario in range(3):
            state.scenario = scenario
            run = study.start({"scenario": scenario})
            variables.input.record_values()
            for step in range(3):
                state.step = step
                calls = []
                request = ac.client._http.request
                with monkeypatch.context() as patch:
                    def track(method, url, **kwargs):
                        calls.append((method, url))
                        return request(method, url, **kwargs)
                    patch.setattr(ac.client._http, "request", track)
                    variables.sample(at=start + timedelta(hours=step), model_time=step)
                assert len(calls) == (3 if step == 0 else 2), calls
                assert calls[-1][1].endswith("/observations/batch")
            calls = []
            request = ac.client._http.request
            with monkeypatch.context() as patch:
                patch.setattr(ac.client._http, "request", track)
                variables.output.record_values()
            assert len(calls) == 1  # scalar, text, JSON, and Parquet together
            run.finish()
            runs.append(run)
        assert sorted(study.experiments.scalars(variables["cost"])["cost"]) == [100, 101, 102]
        query = ac.query().measurement(variable_id=variables["flow"].variable_id, pump="p1")
        assert query.dataframe(shape="narrow").height == 9
        objects = query.include("pump", "observed").metadata_objects()
        assert len(objects) == 3
        assert all(item["data"]["pump"] == "p1" and item["data"]["observed"] == "urn:plant:p1-flow" for item in objects)
        reopened = ac.study.get("model sweep")
        assert reopened.variables["config"].metadata == to_json(payload)
        assert runs[0].observations(variables["config"]).latest().value == to_json(payload)
        assert reopened.variables["config"].role == "input"
        assert ac.query().measurement(**{"metadata.location": "tank"}).dataframe(shape="narrow").height == 9
        assert ac.query().measurement(**{"metadata.note": payload["note"]}).dataframe(shape="narrow").height == 9
        assert ac.query().measurement(quantity_kind="http://qudt.org/vocab/quantitykind/VolumeFlowRate").dataframe(shape="narrow").height == 18
        assert runs[0].observations(variables["capacity"]).latest().dataframe().height == 2
        assert {obs.key for obs in runs[0].observations(variables["flow"]).all()} == {("p1",), ("p2",)}
        assert runs[0].delete()["samples"] == 9
        assert ac.query().measurement(variable_id=variables["flow"].variable_id, pump="p1").dataframe(shape="narrow").height == 6

        # A rejected observation batch leaves samples under the run's source;
        # deleting that run must sweep these unreferenced streams as well.
        orphan = study.start()
        observe = ac.client.observe_experiment_batch
        with monkeypatch.context() as patch:
            def reject(run_id, observations):
                return observe(run_id, [*observations, {"variable_id": variables["cost"].variable_id, "value": "bad"}])
            patch.setattr(ac.client, "observe_experiment_batch", reject)
            with pytest.raises(HTTPError):
                variables.sample(at=start, model_time=0)
        assert orphan.observations().all() == []
        orphan.finish()
        assert orphan.delete()["samples"] == 3
        aq.shutdown()
        ac = aq.init(data_dir=tmp_path / "local-server")
        assert ac.study.get("model sweep").variables["config"].metadata == to_json(payload)
    finally:
        aq.shutdown()
