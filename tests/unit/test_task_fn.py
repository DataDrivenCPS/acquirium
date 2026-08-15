"""Tests for the task function shipping/loading contract and TaskSpec."""

import textwrap

import pytest

from acquirium.Apps.task_fn import load_function, python_version, ship_function
from acquirium.internals.models import AppOutputSpec, TaskSpec


THRESHOLD = 35.0


def _helper(v):
    return v * 2


def check_tds(ctx):
    return [_helper(ctx) + THRESHOLD]


class TestShip:
    def test_ships_source_blob_and_version(self):
        shipped = ship_function(check_tds)
        assert shipped["fn_name"] == "check_tds"
        assert "def check_tds(ctx):" in shipped["fn_source"]
        assert shipped["fn_blob"] is not None
        assert shipped["python_version"] == python_version()

    def test_source_is_dedented(self):
        class Holder:
            @staticmethod
            def nested(ctx):
                return []
        src = ship_function(Holder.nested)["fn_source"]
        assert src.startswith("@staticmethod\ndef nested(ctx):") or src.startswith("def nested")

    def test_lambda_rejected(self):
        with pytest.raises(ValueError, match="named function"):
            ship_function(lambda ctx: [])

    def test_unreadable_source_rejected(self):
        # A function whose source cannot be read (built by exec) must be
        # refused: the source is the persistence contract.
        ns: dict = {}
        exec("def ghost(ctx):\n    return []\n", ns)
        with pytest.raises(ValueError, match="could not read the source"):
            ship_function(ns["ghost"])


class TestLoad:
    def test_blob_path_carries_helpers_and_globals(self):
        shipped = ship_function(check_tds)
        fn = load_function(
            fn_name=shipped["fn_name"], fn_source=shipped["fn_source"],
            fn_blob=shipped["fn_blob"], blob_python_version=shipped["python_version"],
        )
        # cloudpickle carried _helper and THRESHOLD by value.
        assert fn(1.0) == [37.0]

    def test_source_path_when_version_mismatches(self):
        source = textwrap.dedent("""
            def add_one(ctx):
                return [Output.event(point_uri="urn:x", severity="info", message=str(ctx + 1))]
        """)
        fn = load_function(
            fn_name="add_one", fn_source=source,
            fn_blob=b"garbage-from-another-python", blob_python_version="2.7",
        )
        # Output is provided by the exec namespace without an import.
        (out,) = fn(1)
        assert out.kind == "event" and out.payload["message"] == "2"

    def test_source_path_when_no_blob(self):
        fn = load_function(fn_name="f", fn_source="def f(ctx):\n    return [ctx]\n")
        assert fn(3) == [3]

    def test_source_missing_function_raises(self):
        with pytest.raises(ValueError, match="did not define"):
            load_function(fn_name="f", fn_source="x = 1\n")

    def test_corrupt_blob_falls_back_to_source(self):
        fn = load_function(
            fn_name="f", fn_source="def f(ctx):\n    return ['src']\n",
            fn_blob=b"not a pickle", blob_python_version=python_version(),
        )
        assert fn(None) == ["src"]


class TestTaskSpec:
    def test_to_app_spec_shares_the_graph_shape(self):
        spec = TaskSpec(
            name="tds", query={"nodes": []}, fn_name="f", fn_source="def f(ctx): return []",
            outputs=[AppOutputSpec(kind="trigger", point_uri="urn:t")],
            run_mode="interval", interval=10.0,
        )
        app = spec.to_app_spec()
        assert (app.name, app.kind, app.app_type) == ("tds", "task", "task")
        assert app.queries == {"default": {"nodes": []}}
        assert app.outputs[0].kind == "trigger"
        assert (app.run_mode, app.interval) == ("interval", 10.0)
        assert app.env is None  # tasks carry no environment by contract
