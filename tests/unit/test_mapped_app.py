from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
from rdflib import URIRef

from acquirium import MappedApp, MappedStream, OutputTemplate
from acquirium.Apps.execution import prepare_app_debug
from acquirium.Client.acquirium import Acquirium
from acquirium.Apps.mapped import mapped_output_identity
from acquirium.Apps.runner import AppRunner
from acquirium.internals.internals_namespaces import IS_CALCULATED_FROM
from acquirium.internals.models import AppContext, AppSpec


class FakeData:
    def __init__(self):
        self.bindings = [
            SimpleNamespace(
                alias="sensor",
                point_uri="urn:test:sensor:a",
                ref_uri="urn:test:ref:a",
                property_unit="http://qudt.org/vocab/unit/DEG_C",
                ref_unit=None,
            ),
            SimpleNamespace(
                alias="sensor",
                point_uri="urn:test:sensor:b",
                ref_uri="urn:test:ref:b",
                property_unit="http://qudt.org/vocab/unit/DEG_C",
                ref_unit=None,
            ),
        ]
        ts = [
            datetime(2026, 1, 1, hour=i, tzinfo=timezone.utc)
            for i in range(3)
        ]
        self.frames = {
            "urn:test:sensor:a": pl.DataFrame({"time": ts, "value": [1.0, 3.0, 5.0]}),
            "urn:test:sensor:b": pl.DataFrame({"time": ts, "value": [2.0, 4.0, 6.0]}),
        }

    def iter(self, alias):
        assert alias == "sensor"
        yield from sorted(self.frames.items())


class FakeQuery:
    def __init__(self):
        self.calls = []

    def data(self, **kwargs):
        self.calls.append(kwargs)
        return FakeData()

    def resolved_nodes(self, **kwargs):
        return ["urn:test:sensor:a", "urn:test:sensor:b"]

    def to_dict(self):
        return {"kind": "fake"}


class AverageApp(MappedApp):
    name = "average"
    input_alias = "sensor"
    output = OutputTemplate(name="mean", unit="same_as_input")

    def __init__(self, query=None):
        self.fake_query = query or FakeQuery()

    def build_query(self, aq):
        return self.fake_query

    def transform(self, stream: MappedStream, ctx: AppContext):
        return stream.values.with_columns(
            pl.col("value").rolling_mean(window_size=2, min_samples=1)
        )


def context(query):
    return AppContext(
        app_id="average",
        started_at=datetime.now(timezone.utc),
        start=None,
        end=None,
        query=query,
        queries={"default": query},
        params={},
    )


def test_mapped_identity_is_stable_and_input_specific():
    first = mapped_output_identity("average", "mean", "urn:test:sensor:a")
    assert first == mapped_output_identity("average", "mean", "urn:test:sensor:a")
    assert first != mapped_output_identity("average", "mean", "urn:test:sensor:b")
    assert first[0].startswith("mean/")
    assert first[1].startswith("urn:acquirium:derived:average:mean:")


def test_resolve_output_specs_copies_unit_and_direct_lineage():
    app = AverageApp()
    specs = app.resolve_output_specs({"default": app.fake_query})

    assert len(specs) == 2
    assert {spec.depends_on[0] for spec in specs} == {
        "urn:test:sensor:a", "urn:test:sensor:b"
    }
    assert {spec.unit for spec in specs} == {
        "http://qudt.org/vocab/unit/DEG_C"
    }
    assert all(spec.ref_name.startswith("mean/") for spec in specs)


def test_resolve_mappings_reports_input_and_output_refs():
    app = AverageApp()
    mappings = app.resolve_mappings({"default": app.fake_query})

    assert mappings[0].to_dict() == {
        "input_point_uri": "urn:test:sensor:a",
        "input_ref_uri": "urn:test:ref:a",
        "output_point_uri": mapped_output_identity(
            "average", "mean", "urn:test:sensor:a"
        )[1],
        "output_ref_name": mapped_output_identity(
            "average", "mean", "urn:test:sensor:a"
        )[0],
    }


def test_run_maps_each_input_to_its_declared_output():
    app = AverageApp()
    outputs = app.run(context(app.fake_query))
    specs = app.resolve_output_specs({"default": app.fake_query})

    assert len(outputs) == 2
    assert {out.payload["point_uri"] for out in outputs} == {
        spec.point_uri for spec in specs
    }
    assert {out.payload["ref_name"] for out in outputs} == {
        spec.ref_name for spec in specs
    }
    assert [value for _, value in outputs[0].payload["rows"]] == [1.0, 2.0, 4.0]


def test_streams_exposes_exact_transform_inputs():
    app = AverageApp()
    streams = app.streams(context(app.fake_query))

    assert [stream.input_point_uri for stream in streams] == [
        "urn:test:sensor:a", "urn:test:sensor:b"
    ]
    assert streams[0].values["value"].to_list() == [1.0, 3.0, 5.0]


def test_debug_session_calls_transform_on_first_or_selected_stream():
    app = AverageApp()
    session = prepare_app_debug(app, object())

    assert session.transform()["value"].to_list() == [1.0, 2.0, 4.0]
    assert session.transform(session.streams[1])["value"].to_list() == [2.0, 3.0, 5.0]


def test_registration_graph_records_per_output_lineage(tmp_path):
    app = AverageApp()
    specs = app.resolve_output_specs({"default": app.fake_query})
    spec = AppSpec(
        name=app.name,
        outputs=specs,
        depends_on=["urn:test:sensor:a", "urn:test:sensor:b"],
    )
    runner_cls = AppRunner.__ray_actor_class__
    graph = runner_cls._app_spec_graph(None, spec)

    by_dependency = {
        str(dep): str(point)
        for point, _, dep in graph.triples((None, IS_CALCULATED_FROM, None))
    }
    assert by_dependency == {
        "urn:test:sensor:a": specs[0].point_uri,
        "urn:test:sensor:b": specs[1].point_uri,
    }
    assert all(isinstance(point, URIRef) for point, _, _ in graph.triples((None, IS_CALCULATED_FROM, None)))


def test_register_app_resolves_mapped_outputs():
    app = AverageApp()
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.register_app.return_value = {"ok": True}

    aq.register_app(app)

    registered = aq.client.register_app.call_args.args[0]
    assert len(registered.outputs) == 2
    assert {out.depends_on[0] for out in registered.outputs} == {
        "urn:test:sensor:a", "urn:test:sensor:b"
    }


def test_runner_reconciles_new_mapped_outputs(tmp_path):
    aq = MagicMock()
    runner_cls = AppRunner.__ray_actor_class__
    runner = runner_cls(AppSpec(name="average"), tmp_path, aq)
    runner.app = AverageApp()
    runner.queries = {"default": runner.app.fake_query}
    runner.query = runner.app.fake_query

    runner._sync_dynamic_outputs()
    assert len(runner.spec.outputs) == 2
    assert set(runner.spec.depends_on) == {
        "urn:test:sensor:a", "urn:test:sensor:b"
    }
    aq.insert_graph.assert_called_once()

    runner._sync_dynamic_outputs()
    aq.insert_graph.assert_called_once()

    status = runner.status()
    assert status["mappings"][0]["input_point_uri"] == "urn:test:sensor:a"
    assert status["mappings"][0]["output_point_uri"] in {
        output.point_uri for output in runner.spec.outputs
    }
