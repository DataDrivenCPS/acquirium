"""Whole-model recording through a real local HTTP server."""
from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl
import pytest
import acquirium as aq

pytestmark = pytest.mark.usefixtures("separate_server")


@pytest.mark.integration
def test_model_sweep_records_batches_and_deletes_one_runs_samples(tmp_path, monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    scenario = 0
    payload = {"a.b": {"arbitrary key": [None, {}, []]}, "role": "user value", "unit": None,
               "": True, "日本語": {"x": np.int64(3)}, "note": "https://example.org/a contains a note"}
    adapter = aq.adapters.define()
    adapter.scalar("cost", read=lambda: 100 + scenario)
    adapter.input.json("config", read=lambda: payload, metadata=payload)
    adapter.table("capacity", read=lambda: pl.DataFrame({"pump": ["p1", "p2"], "value": [10, 20]}))
    adapter.timeseries("pressure", read=lambda: 2, metadata={"location": "tank", **payload})
    flow_kind = "http://qudt.org/vocab/quantitykind/VolumeFlowRate"
    adapter.add(aq.Component("flow", "timeseries", unit="L-PER-SEC", keys=("pump",),
        quantity_kind=flow_kind, observed={("p1",): "urn:plant:p1-flow"}),
        read_at=lambda t: {("p1",): scenario + t, ("p2",): 10 + scenario + t})

    ac = aq.init(data_dir=tmp_path / "local-server")
    try:
        study = ac.study.define("model sweep")
        calls = []
        request = ac.client._http.request

        def track(method, url, **kwargs):
            calls.append((method, url))
            return request(method, url, **kwargs)

        def one_request(path, operation, *args, **kwargs):
            """Verify that a complete declaration or recording uses one HTTP request."""
            calls.clear()
            result = operation(*args, **kwargs)
            [(method, url)] = calls
            assert method == "POST" and url.endswith(path)
            return result

        monkeypatch.setattr(ac.client._http, "request", track)
        variables = one_request("/variables/batch", study.declare_from, adapter)
        runs = []
        for scenario in range(2):
            run = study.start({"scenario": scenario})
            one_request("/record", variables.input.record_values)
            for step in range(2):
                one_request("/record", variables.sample, at=start + timedelta(hours=step), model_time=step)
            one_request("/record", variables.output.record_values)
            run.finish()
            runs.append(run)

        # The keyed samples are visible to the ordinary stream query.
        assert sorted(study.experiments.scalars(variables["cost"])["cost"]) == [100, 101]
        flow = variables["flow"]
        query = ac.query().measurement(variable_id=flow.variable_id, pump="p1")
        assert sorted(query.dataframe(shape="narrow")["value_numeric"]) == [0.0, 1.0, 1.0, 2.0]
        objects = query.include("pump", "observed").metadata_objects()
        assert len(objects) == len(runs)
        assert all(item["data"]["pump"] == "p1" and item["data"]["observed"] == "urn:plant:p1-flow" for item in objects)
        assert ac.query().measurement(quantity_kind=flow_kind).dataframe(shape="narrow").height == 8
        for fields in ({"metadata.location": "tank"}, {"metadata.note": payload["note"]}):
            assert ac.query().measurement(**fields).dataframe(shape="narrow").height == 4
        assert runs[0].observations("capacity").latest().dataframe().height == 2
        assert {obs.key for obs in runs[0].observations(flow).all()} == {("p1",), ("p2",)}

        reopened = ac.study.get("model sweep")
        expected = {**payload, "日本語": {"x": 3}}
        config = reopened.variables["config"]
        assert config.metadata == expected and config.role == "input" and config.unit is None
        assert reopened.variables.frame().filter(pl.col("label") == "config")["metadata"].item() == expected
        assert runs[0].observations("config").latest().value == expected

        # Deleting one run leaves the second run's samples visible.
        assert runs[0].delete()["samples"] == 6
        remaining = ac.query().measurement(variable_id=flow.variable_id, pump="p1")
        assert remaining.dataframe(shape="narrow")["value_numeric"].to_list() == [1.0, 2.0]
    finally:
        aq.shutdown()


@pytest.mark.integration
def test_experiment_stream_identity_survives_label_collisions_and_resume(tmp_path):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ac = aq.init(data_dir=tmp_path / "identity-server")
    try:
        study = ac.study.define("stream identities")
        keyed = study.output("flow").timeseries(keys=("pump",))
        literal = study.output("flow[p1]").timeseries()
        run = study.start()
        # Simulate an observation written before variable-based storage names.
        source = f"experiment/{run.run_id}"
        old_at = start - timedelta(hours=1)
        ac.register_streams([{"source_id": source, "stream_name": literal.label, "value_kind": "numeric"}])
        ac.insert_timeseries(source, literal.label, [(old_at, 5)])
        legacy_stream = ac.stream(source, literal.label)
        literal.record(legacy_stream.between(old_at, old_at))
        keyed_stream = keyed.record({("p1",): [(start, 1)]})[("p1",)]
        literal_stream = literal.record([(start, 10)])
        assert literal_stream.stream_uri != legacy_stream.stream_uri
        assert keyed_stream.stream_uri != literal_stream.stream_uri
        assert keyed_stream.dataframe()["value"].to_list() == [1.]
        assert literal_stream.dataframe()["value"].to_list() == [10.]
        # Reopen the study and resume: identities remain stable without caches.
        reopened = ac.study.get(study.name)
        resumed = reopened.resume(run.run_id)
        assert resumed.observations("flow[p1]").all()[0].dataframe()["value"].to_list() == [5.]
        end = start + timedelta(hours=1)
        # A stream reads once its own recording publishes, so read the later ones.
        keyed_again = reopened.output["flow"].record({("p1",): [(end, 2)]})[("p1",)]
        literal_again = reopened.output["flow[p1]"].add([(end, 20)])
        assert keyed_again.stream_uri == keyed_stream.stream_uri
        assert literal_again.stream_uri == literal_stream.stream_uri
        assert keyed_again.dataframe()["value"].to_list() == [1., 2.]
        assert literal_again.dataframe()["value"].to_list() == [10., 20.]
        observations = resumed.observations("flow[p1]").all()
        assert [item.sequence for item in observations] == [1, 3, 5]
        assert [item.range_start for item in observations] == [old_at, start, end]

        # JSON key encoding distinguishes types, signed zero, and delimiters.
        keyed = reopened.output["flow"]
        refs = []
        for key in (1, 1.0, True, "1", 0.0, -0.0, "p1,p2", "p1][p2", "", "日本語"):
            refs.append(keyed.record({(key,): [(start, 3)]})[(key,)].stream_uri)
        assert len(set(refs)) == len(refs)
        assert [item.stream_uri for item in resumed.observations(keyed).all()][-len(refs):] == refs
        resumed.finish()

        # A new run owns a different stream for the same variable.
        second = reopened.start()
        later = reopened.output["flow[p1]"].record([(start, 50)])
        assert later.stream_uri != literal_stream.stream_uri
        assert second.observations("flow[p1]").latest().sequence == 1
        assert later.dataframe()["value"].to_list() == [50.]
        second.finish()
    finally:
        aq.shutdown()


@pytest.mark.integration
def test_corrected_source_reads_current_reference_saved_epoch_and_owned_copy(tmp_path):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ac = aq.init(data_dir=tmp_path / "epoch-server")
    try:
        ac.register_streams([{"source_id": "site", "stream_name": "flow", "value_kind": "numeric"}])
        ac.insert_timeseries("site", "flow", [(start, 5)])
        study = ac.study.define("as recorded")
        flow = study.input("flow").timeseries()
        run = study.start()
        site = ac.stream("site", "flow")
        window = site.between(start, start)

        # Record a reference to the site stream and a copy owned by this run.
        flow.record(window)
        copied = flow.record(window.dataframe())
        run.finish()
        reference, copy = run.observations("flow").all()
        assert reference.interval == window and reference.stream == site
        assert copy.stream == copied and copy.stream_uri != site.stream_uri

        # Only the current reference changes when a correction arrives.
        ac.insert_timeseries("site", "flow", [(start, 7)])
        assert reference.epoch is not None
        assert reference.dataframe()["value"].to_list() == [7.]
        assert reference.dataframe(epoch=reference.epoch)["value"].to_list() == [5.]
        assert copy.dataframe()["value"].to_list() == [5.]

        # A later run can refer to the owned copy without copying the rows again.
        later = study.start()
        flow.record(copy.interval)
        reused = later.observations(flow).latest()
        assert reused.stream_uri == copied.stream_uri
        assert reused.dataframe()["value"].to_list() == [5.]
        later.finish()
    finally:
        aq.shutdown()
