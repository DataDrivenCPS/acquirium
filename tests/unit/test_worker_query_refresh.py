"""Tests for worker-side query rebuild on graph_version change."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from acquirium.Apps.base import App
from acquirium.Apps.worker import _build_query_bundle, _maybe_refresh_query
from acquirium.internals.models import AppContext


# ─────────────────────── stubs ───────────────────────


class CountingApp(App):
    """App that records build_query() calls and returns sentinel objects."""

    name = "worker_test_app"
    version = "0.1"
    app_type = "soft_sensor"

    def __init__(self) -> None:
        self.build_count = 0
        self.return_dict = False
        self.raise_on_build = False

    def build_query(self, aq):  # type: ignore[override]
        self.build_count += 1
        if self.raise_on_build:
            raise RuntimeError("simulated build failure")
        if self.return_dict:
            return {"default": SimpleNamespace(tag=f"q{self.build_count}-default"),
                    "alt": SimpleNamespace(tag=f"q{self.build_count}-alt")}
        return SimpleNamespace(tag=f"q{self.build_count}")

    def run(self, ctx):  # type: ignore[override]
        return []


def make_ctx(query=None, queries=None) -> AppContext:
    return AppContext(
        app_id="worker_test_app",
        started_at=datetime.now(timezone.utc),
        start=None,
        end=None,
        query=query,
        params={},
        queries=queries,
    )


def make_aq(version_value: int | Exception = 0) -> Any:
    """Build a stub Acquirium client whose graph_version() returns/raises the given value."""
    aq = MagicMock()
    if isinstance(version_value, Exception):
        aq.graph_version.side_effect = version_value
    else:
        aq.graph_version.return_value = version_value
    return aq


# ─────────────────────── _build_query_bundle ───────────────────────


def test_build_query_bundle_single_query():
    app = CountingApp()
    query, queries = _build_query_bundle(app, aq=MagicMock())
    assert query.tag == "q1"
    assert queries == {"default": query}


def test_build_query_bundle_dict_uses_default_key():
    app = CountingApp()
    app.return_dict = True
    query, queries = _build_query_bundle(app, aq=MagicMock())
    assert query.tag == "q1-default"
    assert set(queries.keys()) == {"default", "alt"}
    assert queries["default"] is query


# ─────────────────────── _maybe_refresh_query ───────────────────────


def test_no_rebuild_when_version_unchanged():
    app = CountingApp()
    initial_query, initial_queries = _build_query_bundle(app, aq=MagicMock())
    ctx = make_ctx(query=initial_query, queries=initial_queries)
    aq = make_aq(version_value=5)

    result = _maybe_refresh_query(app, aq, ctx, last_version=5)
    assert result == 5
    assert app.build_count == 1  # only the initial build
    assert ctx.query is initial_query


def test_rebuild_when_version_advances():
    app = CountingApp()
    initial_query, initial_queries = _build_query_bundle(app, aq=MagicMock())
    ctx = make_ctx(query=initial_query, queries=initial_queries)
    aq = make_aq(version_value=7)

    result = _maybe_refresh_query(app, aq, ctx, last_version=5)
    assert result == 7
    assert app.build_count == 2  # initial + rebuild
    assert ctx.query is not initial_query
    assert ctx.query.tag == "q2"
    assert ctx.queries["default"] is ctx.query


def test_keeps_query_when_graph_version_call_fails():
    app = CountingApp()
    initial_query, initial_queries = _build_query_bundle(app, aq=MagicMock())
    ctx = make_ctx(query=initial_query, queries=initial_queries)
    aq = make_aq(version_value=ConnectionError("server unreachable"))

    result = _maybe_refresh_query(app, aq, ctx, last_version=3)
    assert result == 3  # unchanged
    assert app.build_count == 1  # no rebuild attempted
    assert ctx.query is initial_query


def test_keeps_query_and_retries_when_rebuild_fails():
    app = CountingApp()
    initial_query, initial_queries = _build_query_bundle(app, aq=MagicMock())
    ctx = make_ctx(query=initial_query, queries=initial_queries)
    aq = make_aq(version_value=10)

    # Make the next build_query() call raise.
    app.raise_on_build = True

    result = _maybe_refresh_query(app, aq, ctx, last_version=5)
    # Returned version stays at 5 so the next iteration will retry.
    assert result == 5
    # build_query was called again (and failed)
    assert app.build_count == 2
    # ctx.query is still the original (rebuild was atomic per-call)
    assert ctx.query is initial_query
