import logging

import pytest

import acquirium as aq
from acquirium import debug

MODULE_LEVEL = "from globals"


class _FakeConsole:
    """Stands in for code.InteractiveConsole, recording what it was handed."""

    seen: dict = {}

    def __init__(self, namespace):
        type(self).seen = {"namespace": namespace}

    def interact(self, banner=None, exitmsg=None):
        type(self).seen["banner"] = banner
        type(self).seen["exitmsg"] = exitmsg


@pytest.fixture
def interactive(monkeypatch):
    """A console that records instead of blocking, with a tty pretended."""
    monkeypatch.setattr(debug.code, "InteractiveConsole", _FakeConsole)
    monkeypatch.setattr(debug.sys.stdin, "isatty", lambda: True, raising=False)
    _FakeConsole.seen = {}
    return _FakeConsole


def test_console_sees_the_callers_locals_and_globals(interactive):
    def transform():
        local_frame = "from locals"
        shadowed = "local wins"
        aq.console()

    shadowed = "module level"  # noqa: F841 — the local above must win
    transform()

    namespace = interactive.seen["namespace"]
    assert namespace["local_frame"] == "from locals"
    assert namespace["MODULE_LEVEL"] == "from globals"
    assert namespace["shadowed"] == "local wins"


def test_console_banner_names_the_caller_and_its_variables(interactive):
    def transform():
        inputs, output = {}, {}
        aq.console()

    transform()

    banner = interactive.seen["banner"]
    assert "transform at" in banner
    assert "inputs, output" in banner
    assert "Ctrl-D" in banner
    assert interactive.seen["exitmsg"] == "resuming."


def test_a_custom_banner_replaces_the_default(interactive):
    aq.console("look here")

    assert interactive.seen["banner"] == "look here"


def test_depth_selects_an_outer_frame_for_wrappers(interactive):
    def helper():
        # A wrapper wants its own caller's scope, not its own.
        aq.console(depth=2)

    def outer():
        outer_only = "visible"  # noqa: F841
        helper()

    outer()

    assert "outer_only" in interactive.seen["namespace"]


def test_console_is_skipped_without_a_terminal(monkeypatch, caplog):
    # A console left in a deployed app must not block the server on input
    # that will never arrive.
    monkeypatch.setattr(debug.code, "InteractiveConsole", _FakeConsole)
    monkeypatch.setattr(debug.sys.stdin, "isatty", lambda: False, raising=False)
    _FakeConsole.seen = {}

    with caplog.at_level(logging.WARNING, logger="acquirium.debug"):
        aq.console()

    assert _FakeConsole.seen == {}
    assert "no interactive terminal" in caplog.text
    assert "--local" in caplog.text
