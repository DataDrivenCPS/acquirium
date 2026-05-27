from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from acquirium.Apps.base import App, AppContext, Output
from acquirium.Apps.output_emission import emit_outputs
from acquirium.Client.acquirium import Acquirium
from acquirium.Client.query import Query
from acquirium.Server.manager import Manager
from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.app_runner")


@dataclass
class _CachedQueries:
    """Snapshot of an app's resolved queries from a single build_query() call."""

    query: Query | None
    queries: dict[str, Query]


@dataclass
class AppRunner:
    """Server-side, in-process runner for registered Apps.

    Each app's `build_query()` is invoked once at registration and cached.
    When the underlying RDF graph changes (via `Manager.insert_graph` and
    other mutation paths), the runner schedules a background refresh that
    rebuilds every cached query. `run_app()` blocks while a refresh is in
    progress so callers always see queries built from the latest graph.
    """

    manager: Manager
    aq: Acquirium

    def __post_init__(self) -> None:
        self._apps: dict[str, App] = {}
        self._cached_queries: dict[str, _CachedQueries] = {}
        self._cache_lock = threading.Lock()

        # Refresh coordination:
        #   _refresh_event is SET when no refresh is running (run_app() may
        #   proceed). It is CLEARED while a refresh is in progress so
        #   run_app() blocks on it.
        #   _refresh_state_lock guards _refresh_pending / _refresh_active and
        #   coalesces overlapping refresh requests into a single worker.
        self._refresh_event = threading.Event()
        self._refresh_event.set()
        self._refresh_state_lock = threading.Lock()
        self._refresh_pending = False
        self._refresh_active = False

        _workers = int(os.getenv("ACQUIRIUM_APP_WORKERS", "4"))
        self._app_executor = ThreadPoolExecutor(
            max_workers=_workers,
            thread_name_prefix="acquirium-app",
        )

        # Subscribe to graph mutations from the Manager.
        self.manager.add_graph_change_listener(self._on_graph_change)

    # ─────────────────────── registration ───────────────────────

    def register(self, app: App) -> None:
        if not getattr(app, "name", None):
            raise ValueError("App must define .name")
        # Don't race with an in-flight refresh that might overwrite our build
        # with a stale snapshot.
        logger.debug("register: waiting for in-flight refresh (app=%s)", app.name)
        self._refresh_event.wait()
        self._apps[app.name] = app
        try:
            with timed_debug(logger, "register app=%s build_app_queries", app.name):
                self._build_app_queries(app)
        except Exception:
            # Don't fail registration if the initial build fails — the next
            # graph change (or run_app fallback) will retry.
            logger.exception("Initial query build failed for app '%s'", app.name)

    def unregister(self, app_id: str) -> None:
        logger.debug("unregister app_id=%s", app_id)
        self._apps.pop(app_id, None)
        with self._cache_lock:
            self._cached_queries.pop(app_id, None)

    def close(self) -> None:
        try:
            self.manager.remove_graph_change_listener(self._on_graph_change)
        except Exception:
            pass
        self._app_executor.shutdown(wait=True)

    # ─────────────────────── query caching ───────────────────────

    def _build_app_queries(self, app: App) -> None:
        with timed_debug(logger, "build_query app=%s", app.name):
            query_bundle = app.build_query(self.aq)
        if isinstance(query_bundle, dict):
            queries = query_bundle
            query = queries.get("default") or (next(iter(queries.values())) if queries else None)
        else:
            query = query_bundle
            queries = {"default": query}
        logger.debug("_build_app_queries app=%s queries=%d", app.name, len(queries))
        with self._cache_lock:
            self._cached_queries[app.name] = _CachedQueries(query=query, queries=queries)

    def _on_graph_change(self) -> None:
        """Schedule a background refresh of every cached query.

        Coalesces concurrent graph changes: if a refresh is already running,
        this just sets the pending flag so the worker will run again after it
        finishes its current pass.
        """
        with self._refresh_state_lock:
            self._refresh_pending = True
            if self._refresh_active:
                return
            self._refresh_active = True
            # Block any new run_app() calls until the refresh completes.
            self._refresh_event.clear()
        self.manager._executor.submit(self._refresh_loop)

    def _refresh_loop(self) -> None:
        try:
            while True:
                with self._refresh_state_lock:
                    if not self._refresh_pending:
                        self._refresh_active = False
                        self._refresh_event.set()
                        return
                    self._refresh_pending = False
                logger.info(
                    "Refreshing app queries due to graph change (%d apps)",
                    len(self._apps),
                )
                for name, app in list(self._apps.items()):
                    try:
                        self._build_app_queries(app)
                    except Exception:
                        logger.exception("Failed to refresh query for app '%s'", name)
        except Exception:
            # Make sure run_app() never deadlocks if the refresh thread dies.
            with self._refresh_state_lock:
                self._refresh_active = False
                self._refresh_pending = False
                self._refresh_event.set()
            raise

    def refresh_now(self, *, wait: bool = True, timeout: float | None = None) -> None:
        """Trigger a refresh and (optionally) wait for it to finish.

        Mostly intended for tests and manual triggers; production code should
        rely on the automatic graph-change listener.
        """
        self._on_graph_change()
        if wait:
            self._refresh_event.wait(timeout=timeout)

    # ─────────────────────── execution ───────────────────────

    def run_app(
        self,
        app_id: str,
        *,
        start=None,
        end=None,
        params: dict[str, Any] | None = None,
    ) -> "Future[list[Output]]":
        """Submit an app execution to the thread pool and return a Future.

        Blocks until any in-progress query refresh completes (so the app
        always sees queries built from the latest graph), then submits to
        the executor.  Multiple concurrent calls execute in parallel.

        Call ``.result()`` on the returned Future to wait for completion
        and retrieve outputs.
        """
        if app_id not in self._apps:
            raise KeyError(f"Unknown app_id={app_id}")

        # Block until any in-progress refresh has completed so we run with
        # queries built from the latest graph.
        self._refresh_event.wait()

        app = self._apps[app_id]
        with self._cache_lock:
            cached = self._cached_queries.get(app_id)

        if cached is None:
            # Initial build failed earlier — try once more before giving up.
            self._build_app_queries(app)
            with self._cache_lock:
                cached = self._cached_queries[app_id]

        ctx = AppContext(
            app_id=app_id,
            started_at=datetime.now(timezone.utc),
            start=start,
            end=end,
            query=cached.query,
            params=params or {},
            queries=cached.queries,
        )

        return self._app_executor.submit(self._run_and_emit_outputs, app, ctx)

    def _run_and_emit_outputs(self, app: App, ctx: AppContext) -> list[Output]:
        with timed_debug(logger, "app.run app_id=%s", ctx.app_id):
            outputs = app.run(ctx)
        logger.debug("emit_outputs app_id=%s outputs=%d", ctx.app_id, len(outputs))
        with timed_debug(logger, "emit_outputs app_id=%s", ctx.app_id):
            emit_outputs(ctx.app_id, outputs, insert_timeseries=self.manager.insert_timeseries, logger=logger)
        return outputs
