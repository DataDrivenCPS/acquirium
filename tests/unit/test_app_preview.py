from __future__ import annotations

from datetime import datetime, timezone

import pytest

from acquirium import App, Output
from acquirium.Apps.execution import (
    AppContractError,
    DryRunMutationError,
    prepare_app_debug,
    preview_app,
    output_specs,
    validate_outputs,
)
from acquirium.Apps.output_emission import PreviewSink
from acquirium.internals.models import AppOutputSpec


class FakeQuery:
    def to_dict(self):
        return {"query": "fake"}

    def resolved_nodes(self, *, only_data_nodes=False):
        assert only_data_nodes is True
        return ["urn:test:input"]


class FakeAcquirium:
    def __init__(self):
        self.mutations = []
        self.client = object()

    def insert_graph(self, *args, **kwargs):
        self.mutations.append((args, kwargs))


class PreviewApp(App):
    name = "preview_app"
    outputs = [{"kind": "timeseries", "point_uri": "urn:test:output"}]

    def build_query(self, aq):
        return FakeQuery()

    def build_app(self, ctx):
        return {"offset": ctx.params.get("offset", 0)}

    def run(self, ctx):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return [
            Output.timeseries(
                point_uri="urn:test:output",
                rows=[(ts, ctx.state["offset"] + ctx.params.get("value", 1))],
            )
        ]


def test_preview_runs_build_and_run_without_effects():
    aq = FakeAcquirium()
    result = preview_app(
        PreviewApp(),
        aq,
        build_params={"offset": 4},
        params={"value": 3},
        sink=PreviewSink(max_rows=1),
    )

    assert aq.mutations == []
    assert result.queries["default"]["matched_streams"] == ["urn:test:input"]
    assert result.effects[0]["kind"] == "would_insert_timeseries"
    assert result.effects[0]["rows"][0][1] == 7
    assert result.to_dict()["outputs"] == [
        {"kind": "timeseries", "point_uri": "urn:test:output", "row_count": 1}
    ]


def test_preview_blocks_acquirium_mutations():
    class MutatingApp(PreviewApp):
        def build_app(self, ctx):
            self.insert_graph("<urn:a> <urn:b> <urn:c> .")

    with pytest.raises(DryRunMutationError, match="insert_graph"):
        preview_app(MutatingApp(), FakeAcquirium(), sink=PreviewSink())


def test_validate_outputs_rejects_undeclared_destination():
    output = Output.timeseries(
        point_uri="urn:test:other",
        rows=[(datetime.now(timezone.utc), 1)],
    )
    declarations = [AppOutputSpec(kind="timeseries", point_uri="urn:test:declared")]

    with pytest.raises(AppContractError, match="undeclared point_uri"):
        validate_outputs([output], declarations)


def test_validate_outputs_rejects_non_output_values():
    with pytest.raises(AppContractError, match="must be Output"):
        validate_outputs([{"kind": "event"}])


def test_output_specs_reject_duplicate_destinations():
    app = PreviewApp()
    app.outputs = [
        {"kind": "timeseries", "point_uri": "urn:test:same"},
        {"kind": "event", "point_uri": "urn:test:same"},
    ]
    with pytest.raises(AppContractError, match="duplicate output"):
        output_specs(app)


def test_preview_sink_does_not_call_webhook(monkeypatch):
    def fail_post(*args, **kwargs):
        pytest.fail("preview must not issue HTTP requests")

    monkeypatch.setattr("acquirium.Apps.output_emission.requests.post", fail_post)
    effects = PreviewSink().emit(
        "app:test",
        [Output.trigger(url="hooks.example/alert", message={"status": "watch"})],
    )
    assert effects == [{
        "kind": "would_post_webhook",
        "url": "http://hooks.example/alert",
        "message": {"status": "watch"},
        "point_uri": None,
        "headers": {},
    }]


def test_debug_session_prepares_state_and_runs_without_emitting():
    aq = FakeAcquirium()
    session = prepare_app_debug(
        PreviewApp(),
        aq,
        build_params={"offset": 4},
        params={"value": 3},
    )

    assert session.state == {"offset": 4}
    assert session.streams == []
    assert session.namespace()["ctx"].params == {"value": 3}
    assert session.run()[0].payload["rows"][0][1] == 7
    assert aq.mutations == []
