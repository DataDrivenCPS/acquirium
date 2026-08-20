"""ChangeRouter: turns publication wake-ups into coalesced actor dispatch.

Lives beside the Manager and AppSupervisor in the FastAPI process
(continuous_batch.md's "server-local ChangeRouter"). After a publication
commits, ``Manager.publish`` calls :meth:`ChangeRouter.wake` with the
touched ``ref_uri``s; the router maps those to subscribed apps, coalesces
for a short window, and dispatches one ``process_pending`` turn per ready
app. A periodic safety scan recovers any wake-up the router itself missed
(a crash between commit and wake, a dropped call) by comparing subscription
versions to stream heads through :class:`~acquirium.Storage.continuous.types.ContinuousStore`.

The router holds no durable state -- ``_pending``/``_in_flight`` are best-
effort hints, not a queue. Everything the safety scan needs to recover a
lost wake-up lives in storage (``app_subscriptions``, ``stream_heads``), per
continuous_batch.md's "recovery from durable storage rather than
notifications or actor memory."

All router-owned state (``_pending``, ``_in_flight``) is touched only from
the event loop: ``wake()`` is the sole cross-thread entry point, and it
hands off via ``loop.call_soon_threadsafe`` rather than mutating state
directly, so no additional lock is needed once that call lands on the loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Iterable

logger = logging.getLogger("acquirium.server.router")

# Given an app_id, run one process_pending turn and return a dict that
# includes at least {"has_more": bool}. Supplied by AppSupervisor, wrapping
# whatever async or Ray call actually reaches the app's actor -- the router
# has no Ray dependency itself.
DispatchFn = Callable[[str], Awaitable[dict[str, Any]]]

# Returns {ref_uri: [app_id, ...]} for every active/bootstrapping subscription.
SubscriptionIndexFn = Callable[[], dict[str, list[str]]]

# Returns app ids whose subscription version trails a subscribed stream's head.
LaggingAppsFn = Callable[[], list[str]]


class ChangeRouter:
    """Coalesced, safety-scanned dispatcher from publications to app actors."""

    def __init__(
        self,
        *,
        subscription_index: SubscriptionIndexFn,
        lagging_apps: LaggingAppsFn,
        dispatch: DispatchFn,
        coalesce_seconds: float = 0.05,
        safety_scan_seconds: float = 1.0,
    ):
        self._subscription_index = subscription_index
        self._lagging_apps = lagging_apps
        self._dispatch = dispatch
        self._coalesce_seconds = coalesce_seconds
        self._safety_scan_seconds = safety_scan_seconds

        self._pending: set[str] = set()
        self._in_flight: set[str] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._coalesce_task: asyncio.Task | None = None
        self._safety_task: asyncio.Task | None = None
        self._stopped = False

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Capture the running loop and start the safety-scan background task."""
        self._loop = asyncio.get_running_loop()
        self._stopped = False
        self._safety_task = asyncio.create_task(self._safety_scan_loop())

    async def stop(self) -> None:
        """Cancel background tasks. In-flight dispatches are not awaited --
        callers that need a clean drain should stop apps first."""
        self._stopped = True
        for task in (self._safety_task, self._coalesce_task):
            if task is not None and not task.done():
                task.cancel()

    # ------------------------------------------------------------------
    # wake / dispatch
    # ------------------------------------------------------------------

    def wake(self, ref_uris: Iterable[str]) -> None:
        """Signal that durable work may exist for apps subscribed to *ref_uris*.

        Threadsafe: safe to call from any thread, including a FastAPI sync
        route handler running off the event loop (``Manager.publish``'s
        caller). A no-op before :meth:`start` -- the periodic safety scan
        will pick up any work once the router is running.
        """
        loop = self._loop
        if loop is None:
            return
        loop.call_soon_threadsafe(self._wake_on_loop, list(ref_uris))

    def trigger(self, app_id: str) -> None:
        """Mark one app pending immediately, regardless of what changed.

        Used at startup to (re)trigger every active/bootstrapping app, since
        durable progress -- not router memory -- is what a restart resumes
        from.
        """
        self._pending.add(app_id)
        self._schedule_coalesced_dispatch()

    def _wake_on_loop(self, ref_uris: list[str]) -> None:
        index = self._subscription_index()
        app_ids = {app_id for ref in ref_uris for app_id in index.get(ref, [])}
        if not app_ids:
            return
        self._pending |= app_ids
        self._schedule_coalesced_dispatch()

    def _schedule_coalesced_dispatch(self) -> None:
        if self._coalesce_task is not None and not self._coalesce_task.done():
            return  # a coalescing window is already open; it will pick this up
        self._coalesce_task = asyncio.create_task(self._coalesced_dispatch())

    async def _coalesced_dispatch(self) -> None:
        await asyncio.sleep(self._coalesce_seconds)
        await self._dispatch_ready()

    async def _dispatch_ready(self) -> None:
        """Fire one process_pending turn per pending app not already in flight.

        Busy apps have one pending bit, not an unbounded queue: an app
        already in flight simply stays in ``_pending`` and is picked up by
        the next coalescing window or safety scan once it finishes.
        """
        ready = [app_id for app_id in self._pending if app_id not in self._in_flight]
        for app_id in ready:
            self._pending.discard(app_id)
            self._in_flight.add(app_id)
            asyncio.create_task(self._run_one(app_id))

    async def _run_one(self, app_id: str) -> None:
        try:
            result = await self._dispatch(app_id)
        except Exception:
            logger.exception("router: process_pending failed for app %s", app_id)
            result = {}
        finally:
            self._in_flight.discard(app_id)
        if result.get("has_more"):
            self._pending.add(app_id)
        if self._pending:
            self._schedule_coalesced_dispatch()

    # ------------------------------------------------------------------
    # safety scan
    # ------------------------------------------------------------------

    async def _safety_scan_loop(self) -> None:
        while not self._stopped:
            try:
                await asyncio.sleep(self._safety_scan_seconds)
            except asyncio.CancelledError:
                return
            try:
                lagging = await asyncio.to_thread(self._lagging_apps)
            except Exception:
                logger.exception("router: safety scan's lagging_apps() failed")
                continue
            if lagging:
                self._pending |= set(lagging)
                await self._dispatch_ready()
