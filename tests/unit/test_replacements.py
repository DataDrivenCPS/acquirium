"""Replacement identity, output ownership, and reads across plan changes."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from escapement import NotFound

import acquirium as aq
from acquirium import replacements
from acquirium.apps import output_reference
from acquirium.functions import _epoch_outputs
from acquirium.internals.models import compute_stream_uri
from tests.epoch_helpers import epoch_server

NTU = "http://qudt.org/vocab/unit/NTU"
TURBIDITY = "http://qudt.org/vocab/quantitykind/Turbidity"
MINUTE = 60_000_000_000


DEG_C = "http://qudt.org/vocab/unit/DEG_C"
DEG_F = "http://qudt.org/vocab/unit/DEG_F"


def selection(ref, *, group="urn:replacement:tank", parameters=None, as_of=None, unit=None, **kwargs):
    """Select one stored stream under the test tank's group."""
    bind = f" BIND(<{unit}> AS ?unit)" if unit else ""
    return aq.Selection(
        f"SELECT ?stream ?group ?unit WHERE {{ BIND(<{ref}> AS ?stream) BIND(<{group}> AS ?group){bind} }}",
        parameters={group: parameters} if parameters else None, as_of=as_of, **kwargs,
    )


def declaration(name, ref, function, **kwargs):
    """Declare an app whose output replaces the selected source measurement."""
    return aq.App(name, {"raw": selection(ref)}, {"out": aq.ReplacementOutput()}, function, **kwargs)


def test_declaration_roundtrip():
    app = declaration("clean", "urn:raw", aq.Native.mean("out"))
    assert app.to_dict()["outputs"] == {"out": {"replaces": "raw", "unit": None, "label": None}}
    assert aq.App.from_dict(app.to_dict()).to_dict() == app.to_dict()
    ordinary = replace(app, outputs={"out": aq.Output()})
    assert "replaces" not in ordinary.to_dict()["outputs"]["out"]
    with pytest.raises(ValueError, match="unknown input 'missing'"):
        replace(app, outputs={"out": aq.ReplacementOutput(of="missing")}).to_dict()
    # With several inputs, a replacement must say which one it continues.
    two = replace(app, inputs={**app.inputs, "reference": selection("urn:other")})
    with pytest.raises(ValueError, match="must name the input it replaces"):
        two.to_dict()
    assert replace(two, outputs={"out": aq.ReplacementOutput(of="raw")}).to_dict()["outputs"]["out"]["replaces"] == "raw"


@pytest.mark.parametrize("inputs", [
    pytest.param(["raw"], id="list-instead-of-mapping"),
    pytest.param({"": selection("urn:raw")}, id="empty-input-name"),
    pytest.param({"raw": []}, id="empty-selection-list"),
    pytest.param({"raw": ["query"]}, id="string-instead-of-selection"),
])
def test_inputs_map_names_to_selections(inputs):
    with pytest.raises(ValueError, match="input"):
        aq.App("bad", inputs, {"out": aq.Output()}, aq.Native.mean("out")).to_dict()


def test_several_selections_feed_one_input_and_roundtrip():
    app = aq.App("pair", {"raw": [selection("urn:a"), selection("urn:b")], "gain": selection("urn:g")},
                 {"out": aq.Output()}, aq.Native.mean("out"))
    data = app.to_dict()
    assert [item["input"] for item in data["inputs"]] == ["raw", "raw", "gain"]
    loaded = aq.App.from_dict(data)
    assert isinstance(loaded.inputs["raw"], list) and isinstance(loaded.inputs["gain"], aq.Selection)
    assert loaded.to_dict() == data


def test_selection_views_roundtrip_and_default_by_selection_kind():
    app = declaration("clean", "urn:raw", aq.Native.mean("out"))
    assert "view" not in app.to_dict()["inputs"][0]
    raw = replace(app, inputs={"raw": replace(app.inputs["raw"], view="raw")})
    assert aq.App.from_dict(raw.to_dict()).inputs["raw"].view == "raw"
    with pytest.raises(ValueError, match="view"):
        replace(app, inputs={"raw": replace(app.inputs["raw"], view="typo")}).to_dict()
    reader = aq.App("reader", {"turbidity": selection("urn:raw")}, {"out": aq.Output()},
                    aq.Native.mean("out"))
    both = replace(reader, inputs={**reader.inputs, "stage": aq.Selection.outputs_of(app, "out")})
    declarations = {"clean": app.to_dict(), "reader": both.to_dict()}
    assert replacements.input_views(declarations) == {
        ("clean", "raw"): "stored", ("reader", "turbidity"): "current", ("reader", "stage"): "stored",
    }
    claimed = replace(app, inputs={"raw": replace(app.inputs["raw"], view="current")})
    with pytest.raises(ValueError, match="binds the stream it selects"):
        replacements.input_views({"clean": claimed.to_dict()})
    mixed = replace(reader, inputs={"turbidity": [
        reader.inputs["turbidity"], replace(reader.inputs["turbidity"], view="raw")]})
    with pytest.raises(ValueError, match="different views"):
        replacements.input_views({"reader": mixed.to_dict()})


def test_plan_resolution_and_corrupt_cycles():
    plan = {"instances": [
        {"outputs": {"out": "urn:clean", "diagnostic": "urn:diagnostic"},
         "annotations": {"replaces": {"out": "urn:raw"}}},
        {"outputs": {"out": "urn:filled"}, "annotations": {"replaces": {"out": "urn:clean"}}},
    ]}
    refs = ["urn:raw", "urn:clean", "urn:filled", "urn:diagnostic"]
    assert list(replacements.resolve(plan, refs).values()) == ["urn:filled"] * 3 + ["urn:diagnostic"]
    assert list(replacements.resolve(plan, refs, view="raw").values()) == ["urn:raw"] * 3 + ["urn:diagnostic"]
    assert replacements.resolve(plan, refs, view="stored") == dict(zip(refs, refs))
    with pytest.raises(ValueError, match="view"):
        replacements.resolve(plan, [], view="typo")
    plan["instances"][0]["annotations"]["replaces"]["out"] = "urn:filled"
    with pytest.raises(ValueError, match="cycle"):
        replacements.resolve(plan, ["urn:filled"])


@pytest.mark.parametrize("library", ["pandas", "polars"])
@pytest.mark.parametrize("batch", [False, True])
def test_adapter_selects_only_replacement_times(library, batch):
    times = [0, 1_000, 2_000]
    values = pl.DataFrame({
        "time": pl.Series(times).cast(pl.Datetime("ns", "UTC")),
        "value": [1., 2., 3.],
    })
    selected = [0, 2_000] if batch else [1_000]
    anchors = pl.Series(selected).cast(pl.Datetime("ns", "UTC")).to_list()
    if library == "pandas":
        values = values.to_pandas(use_pyarrow_extension_array=True)
    outputs = _epoch_outputs({"clean": values, "diagnostic": values}, ["clean", "diagnostic"], ["clean"], anchors)
    assert outputs["clean"]["time"].dt.epoch("ns").to_list() == selected
    assert outputs["diagnostic"]["time"].dt.epoch("ns").to_list() == times


@pytest.fixture(scope="module")
def runtime(tmp_path_factory):
    root = tmp_path_factory.mktemp("replacements")
    module = root / "functions.py"
    module.write_text('''import polars as pl
from acquirium.functions import emit

def double(frame: pl.DataFrame, context):
    if context["parameters"].get("fail"):
        raise ValueError("broken replacement")
    if "output_time" in frame.columns:
        frame = frame.filter(pl.col("time") == pl.col("output_time"))
    return {"out": frame.select("time", pl.col("value") * 2).drop_nulls("value")}

def empty(frame: pl.DataFrame, context):
    return {"out": frame.select("time", "value").head(0)}

def passthrough(frame: pl.DataFrame, context):
    return {"out": frame.select("time", "value", "text")}
''')
    with epoch_server(root, read_batch_size=1) as server:
        yield (*server, module)


@pytest.fixture
def system(runtime, request):
    """Register a separate turbidity probe for this test on the shared server."""
    manager, http, client, module = runtime
    source = request.node.name.replace("[", "-").replace("]", "")
    point = f"urn:replacement:{source}"
    client.register_streams([dict(source_id=source, stream_name="ntu", property_uri=point,
                                  unit=NTU, quantity_kind=TURBIDITY)])
    ref = str(compute_stream_uri(source, "ntu"))
    try:
        yield manager, http, client, module, source, point, ref
    finally:
        manager.apps._install({})


def put(system, rows, request="data"):
    """Write turbidity readings and return the published snapshot."""
    manager, _, _, _, source, _, _ = system
    return manager.core.wait(manager.insert_timeseries(
        source_id=source, stream_name="ntu", rows=rows, request_id=request).receipt)


def values(client, ref, **kwargs):
    """Read the value column for a reference at the requested view or epoch."""
    return client.client.timeseries_df(ref, **kwargs)["value"].to_list()


def test_replacement_views_and_consumers_follow_plan_changes(system):
    from acquirium.internals.internals_namespaces import S223

    manager, http, client, module, _, point, ref = system
    # Seed raw readings, then deploy two successive calculations that each double the values.
    initial_snapshot = put(system, [(0, 1.), (1_000, 2.)])
    initial = initial_snapshot.epoch
    function = aq.PythonFunction.from_file(module, "double")
    first = declaration("first", ref, function)
    manager.deploy_app(first)
    first_epoch = manager.core.engine.snapshot().epoch
    clean = output_reference("first", "urn:replacement:tank", "out")
    second = declaration("second", clean, function)
    # Deliberately select the physical stage without upstream metadata hints.
    manager.deploy_app(second)
    head = output_reference("second", "urn:replacement:tank", "out")
    # Current, raw, and stored reads expose different stages of the same measurement.
    assert values(client, point) == [4., 8.]
    assert values(client, ref, view="raw") == [1., 2.]
    assert values(client, clean, view="stored") == [2., 4.]
    assert values(client, head, view="raw") == [1., 2.]
    # Historical reads use the stage that was active at the requested epoch.
    assert values(client, ref, epoch=initial) == [1., 2.]
    assert values(client, ref, epoch=initial, after=manager.core.engine.snapshot(initial).applied) == [1., 2.]
    with pytest.raises(ValueError, match="precedes"):
        manager.timeseries_batch(ref, epoch=initial - 1, after=manager.core.engine.snapshot(initial).applied)
    assert values(client, ref, epoch=first_epoch) == [2., 4.]
    assert client.client.resolve_storage_keys([point, ref]) == {point: head, ref: head}
    assert client.client.resolve_storage_keys([head], view="raw") == {head: ref}
    # Extents, existence checks, and explicit view resolution agree with value reads.
    assert client.client.timeseries_info_batch([ref])[ref].row_count == 2
    assert client.client.timeseries_exists_batch([ref], view="raw") == {ref}
    resolved = client.client.stream_views([ref, clean])
    assert resolved["epoch"] == manager.core.engine.snapshot().epoch
    assert resolved["streams"] == {
        ref: {"current": head, "raw": ref, "unit": NTU},
        clean: {"current": head, "raw": ref, "unit": NTU},
    }
    assert client.client.stream_views([ref], epoch=first_epoch)["streams"][ref]["current"] == clean
    assert http.get("/timeseries", params={"uri": ref, "view": "typo"}).status_code == 422

    # One query can read the same registered measurement in two views.
    tank = "urn:replacement:test-tank"
    client.insert_graph(f"<{tank}> <{S223.hasProperty}> <{point}> .", source_id="test-tank")
    data = (
        client.query().entity(uri=tank, alias="tank")
        .measurement(frm="tank", alias="current", quantity_kind=TURBIDITY)
        .measurement(frm="tank", alias="stored", stream=ref)
    ).data()
    assert {(binding.alias, binding.view) for binding in data.bindings} == {
        ("current", "current"), ("stored", "stored"),
    }
    assert data.total_rows == 4
    frame = data.dataframe(shape="narrow", compact=False).sort("time")
    assert frame.filter(pl.col("data_alias") == "current")["value_numeric"].to_list() == [4., 8.]
    assert frame.filter(pl.col("data_alias") == "stored")["value_numeric"].to_list() == [1., 2.]

    # A calculation matching all three stages follows the identity once.
    follower = reader(ref, clean, head)
    manager.deploy_app(follower)
    assert bound(manager, "reader") == [head]
    assert manager.apps.inspect("reader")["matches"] == {"turbidity": 1}
    out = output_reference("reader", "urn:replacement:tank", "out")
    assert values(client, out) == [4., 8.]

    # The iterator captures both the resolved head and its epoch before deployment.
    pages = manager.timeseries_batch(ref, batch_size=1)
    first_page = next(pages)
    manager.remove_app("second")
    assert first_page["value"].to_pylist() == [4.]
    assert [v for page in pages for v in page["value"].to_pylist()] == [8.]
    assert values(client, ref) == [2., 4.]
    assert bound(manager, "reader") == [clean]
    assert values(client, out) == [2., 4.]
    manager.deploy_app(second)
    assert bound(manager, "reader") == [head]
    assert values(client, out) == [4., 8.]
    # The first stage is still required by the second stage's physical input.
    with pytest.raises(ValueError, match="removed output"):
        manager.remove_app("first")
    # Rebinding the consumer and removing the old stage can be one deployment.
    old_version = manager.apps.inspect("second")["operators"][0]["version"]
    manager.apps._install({
        "second": declaration("second", ref, function).to_dict(), "reader": follower.to_dict(),
    })
    assert manager.apps.inspect("second")["operators"][0]["version"] != old_version
    assert values(client, ref) == [2., 4.]
    assert bound(manager, "reader") == [head]
    assert values(client, out) == [2., 4.]

    # Explicit input views can still bind the original samples or exact stage.
    manager.deploy_app(reader(ref, head, view="raw"))
    assert bound(manager, "reader") == [ref]
    manager.deploy_app(reader(ref, view="stored"))
    assert bound(manager, "reader") == [ref]
    assert values(client, out) == [1., 2.]
    with pytest.raises(NotFound, match="not published"):
        manager.resolve_storage_keys([ref], epoch=manager.core.engine.snapshot().epoch + 1)


def reader(*refs, view=None, group="urn:replacement:tank"):
    """An ordinary app whose one query matches several stages of one identity."""
    rows = " ".join(f"<{ref}>" for ref in refs)
    query = f"SELECT ?stream ?group WHERE {{ VALUES ?stream {{ {rows} }} BIND(<{group}> AS ?group) }}"
    return aq.App("reader", {"turbidity": aq.Selection(query, view=view)},
                  {"out": aq.Output()}, aq.Native.mean("out"))


def bound(manager, name):
    """List the physical stream references bound to an app's operators."""
    return [item["stream"] for operator in manager.apps.inspect(name)["operators"]
            for item in operator["inputs"]]


def test_a_replacing_app_cannot_follow_its_own_output(system):
    manager, _, _, module, _, _, ref = system
    first = declaration("first", ref, aq.PythonFunction.from_file(module, "double"))
    follower = replace(first, inputs={**first.inputs, "reference": selection(ref)},
                       outputs={"out": aq.ReplacementOutput(of="raw")})
    # The reference defaults to the current head and would make this app read its own output.
    with pytest.raises(ValueError, match="follows its own output"):
        manager.deploy_app(follower)
    looking_back = replace(follower, inputs={**first.inputs, "reference": selection(ref, view="raw")})
    manager.deploy_app(looking_back)


def test_competing_claims_metadata_and_group_cardinality(system):
    manager, _, _, module, _, _, ref = system
    app = declaration("first", ref, aq.PythonFunction.from_file(module, "double"))
    manager.deploy_app(app)
    before = manager.core.engine.snapshot().plan()
    # All of these declarations are rejected without disturbing the published plan.
    with pytest.raises(ValueError, match="competing replacements"):
        manager.deploy_app(replace(app, name="sibling"))
    with pytest.raises(ValueError, match="not compatible with its input's unit"):
        manager.deploy_app(replace(app, outputs={"out": aq.ReplacementOutput(unit=DEG_C)}))
    with pytest.raises(ValueError, match="drive execution"):
        manager.deploy_app(replace(app, inputs={"raw": selection(ref, as_of=pd.Timedelta("1m"))}))
    with pytest.raises(ValueError, match="exactly one"):
        manager.deploy_app(replace(app, inputs={"raw": [selection(ref), selection("urn:another")]}))
    assert manager.core.engine.snapshot().plan() == before
    # Repeated identical query rows still identify one physical input.
    manager.deploy_app(replace(app, inputs={"raw": [selection(ref), selection(ref)]}))
    from rdflib import URIRef
    manager.deploy_app(replace(app, outputs={"out": aq.ReplacementOutput(unit=URIRef(NTU))}))


def test_replacements_inherit_metadata_and_may_change_to_a_compatible_unit(system, tmp_path, monkeypatch):
    from unittest.mock import Mock

    from acquirium.internals.internals_namespaces import HAS_QUANTITY_KIND, HAS_UNIT
    from rdflib import URIRef

    manager, _, client, _, source, _, _ = system
    snapshot = Mock(wraps=client.client.engine_snapshot)
    monkeypatch.setattr(client.client, "engine_snapshot", snapshot)
    point = f"urn:replacement:{source}:temperature"
    client.register_streams([dict(source_id=source, stream_name="temperature", property_uri=point,
                                  unit=DEG_F, quantity_kind="http://qudt.org/vocab/quantitykind/Temperature")])
    ref = str(compute_stream_uri(source, "temperature"))
    manager.core.wait(manager.insert_timeseries(source_id=source, stream_name="temperature",
                                               rows=[(0, 68.), (1_000, 212.)], request_id="temperature").receipt)
    before = client.query().measurement(alias="t", uri=point).data()
    module = tmp_path / "celsius.py"
    module.write_text(
        "import polars as pl\n"
        "def celsius(frame: pl.DataFrame, context):\n"
        "    return {'out': frame.select('time', (pl.col('value') - 32) * 5 / 9)}\n"
    )
    to_celsius = aq.App("celsius", {"raw": selection(ref)}, {"out": aq.ReplacementOutput(unit=DEG_C)},
                        aq.PythonFunction.from_file(module, "celsius"))
    manager.deploy_app(to_celsius)
    clean = output_reference("celsius", "urn:replacement:tank", "out")
    graph = manager.graph_store.data_graph("derived:celsius")
    assert (URIRef(f"{clean}__point"), HAS_UNIT, URIRef(DEG_C)) in graph
    assert (URIRef(f"{clean}__point"), HAS_QUANTITY_KIND,
            URIRef("http://qudt.org/vocab/quantitykind/Temperature")) in graph
    # Without on_group, the replacement is not a property of the group.
    assert not list(graph.triples((URIRef("urn:replacement:tank"), None, None)))
    # A normal read returns the final stage, in its unit.
    assert values(client, point) == pytest.approx([20., 100.])
    data = client.query().measurement(alias="t", uri=point).data()
    assert data.bindings[0].property_unit == DEG_C
    assert data.bindings[0].stream_unit == DEG_C
    assert data["t"]["value"].to_list() == pytest.approx([20., 100.])
    # An already-resolved object retains the pre-deployment snapshot.
    assert before.units() == {"t": [DEG_F]}
    assert before["t"]["value"].to_list() == [68., 212.]
    # Even when only the replacement matches, raw values use the base unit.
    raw = client.query().measurement(alias="t", uri=f"{clean}__point").data(view="raw")
    assert raw.units() == {"t": [DEG_F]}
    assert raw["t"]["value"].to_list() == [68., 212.]
    snapshot.assert_not_called()
    # A consumer that selected the Fahrenheit stream converts from the stage it binds.
    normalize = aq.PythonFunction.from_file(
        Path(__file__).resolve().parents[2] / "scripts/apps/temperature_normalization.py", "normalize")
    consumer = aq.App("fahrenheit", {"temperature": selection(ref, unit=DEG_F, target_unit=DEG_F)},
                      {"temperature": aq.Output(unit=DEG_F)}, normalize)
    manager.deploy_app(consumer)
    assert bound(manager, "fahrenheit") == [clean]
    assert values(client, output_reference("fahrenheit", "urn:replacement:tank", "temperature")) == (
        pytest.approx([68., 212.]))
    # A later stage inherits the changed unit, and may not change it to an incompatible one.
    second = aq.App("second", {"raw": selection(clean)}, {"out": aq.ReplacementOutput()},
                    aq.PythonFunction.from_file(module.parent / "celsius.py", "celsius"))
    with pytest.raises(ValueError, match="not compatible"):
        manager.deploy_app(replace(second, outputs={"out": aq.ReplacementOutput(unit=NTU)}))
    assert replacements.units(manager.core.engine.snapshot().plan()) == {clean: DEG_C}


def test_failed_new_head_stale_values_and_successful_empty_result(system):
    manager, _, client, module, _, _, ref = system
    put(system, [(0, 1.)])
    app = declaration("clean", ref, aq.PythonFunction.from_file(module, "double"))
    broken = replace(app, inputs={"raw": selection(ref, parameters={"fail": True})})
    # A head that has never completed has no current value. Raw reads still work.
    assert manager.deploy_app(broken)["status"] == "failed"
    assert values(client, ref) == []
    assert values(client, ref, view="raw") == [1.]
    assert client.client.timeseries_exists_batch([ref]) == set()
    assert client.client.timeseries_exists_batch([ref], view="raw") == {ref}
    manager.deploy_app(app)
    assert values(client, ref) == [2.]
    # After a successful calculation, a later failure leaves the last completed value visible.
    assert manager.deploy_app(broken)["stale"] is True
    put(system, [(0, 3.)], "new-raw")
    assert values(client, ref) == [2.]
    assert values(client, ref, view="raw") == [3.]
    # A successful empty calculation retracts the stale output.
    empty = replace(app, function=aq.PythonFunction.from_file(module, "empty"))
    assert manager.deploy_app(empty)["status"] == "active"
    assert values(client, ref) == []
    assert client.client.timeseries_info_batch([ref])[ref].row_count == 0


def test_whole_series_cleaner_corrections_and_deletions(system):
    from examples.turbidity import calculations

    manager, _, client, _, source, _, ref = system
    app = declaration("minute-clean", ref, aq.PythonFunction.from_file(calculations.__file__, "clean"),
                      window=aq.Rolling(lookback=pd.Timedelta("2m"), step=pd.Timedelta("1m"), lookahead=pd.Timedelta("2m")))
    app = replace(app, outputs={"turbidity": replace(app.outputs["out"])},
                  inputs={"raw": selection(ref, parameters={"threshold": 1.0})})
    put(system, [(0, .2), (30_000_000_000, .4), (2 * MINUTE, .6)])
    manager.deploy_app(app)
    assert values(client, ref) == pytest.approx([.3, .45, .6])
    # A late correction changes the nearby smoothed values. Deleting their
    # source readings retracts those outputs while leaving the last minute.
    put(system, [(30_000_000_000, .6)], "correction")
    assert values(client, ref) == pytest.approx([.4, .5, .6])
    manager.core.wait(manager.delete_timeseries(source, "ntu", [0, 30_000_000_000], request_id="deleted").receipt)
    assert values(client, ref) == [.6]
    manager.core.wait(manager.core.clear(source, "all-deleted", [ref]))
    assert values(client, ref) == []


def test_removed_text_replacement_keeps_its_historical_read_kind(system):
    manager, _, client, module, source, _, _ = system
    client.register_streams([dict(source_id=source, stream_name="state", value_kind="text")])
    ref = str(compute_stream_uri(source, "state"))
    manager.core.wait(manager.insert_timeseries(source_id=source, stream_name="state",
                                               rows=[(0, "running")], request_id="state").receipt)
    app = aq.App("state-clean", {"raw": selection(ref)}, {"out": aq.ReplacementOutput()},
                 aq.PythonFunction.from_file(module, "passthrough"))
    manager.deploy_app(app)
    epoch = manager.core.engine.snapshot().epoch
    assert values(client, ref) == ["running"]
    # An old epoch still knows that the removed stage published text.
    manager.remove_app(app.name)
    assert values(client, ref, epoch=epoch) == ["running"]


@pytest.mark.parametrize("mode", ["default", "batch", "sequential"])
def test_replacement_windows_preserve_existing_execution_modes(system, mode):
    manager, _, client, module, source, _, ref = system
    app = declaration("window-clean", ref, aq.PythonFunction.from_file(
        module, "double", batch=mode == "batch", sequential=mode == "sequential",
    ), window=(aq.FixedBucket(pd.Timedelta("1us")) if mode == "sequential"
               else aq.Rolling(lookback=pd.Timedelta("1us"), step=pd.Timedelta("1us"),
                               lookahead=pd.Timedelta("1us"))))
    put(system, [(0, 1.), (1_000, 2.), (2_000, 3.)])
    assert manager.deploy_app(app)["status"] == "active"
    assert values(client, ref) == [2., 4., 6.]
    put(system, [(1_000, 5.)], "corrected")
    assert values(client, ref) == [2., 10., 6.]
    manager.core.wait(manager.core.clear(source, "clear", [ref]))
    assert values(client, ref) == []
