"""Auto-run: dispatch apps and tasks when the streams they read change.

The data path is the trigger source: every timeseries insert passes through
one of three ``Manager`` choke points, each of which calls
:meth:`ChangeFeed.notify` with the affected stream (ref) URIs. From there:

- **Enqueue-only on the caller's thread.** ``notify`` takes one short local
  lock and returns. It never touches the app supervisor's lock and never
  calls into Ray: the insert endpoints run on the server's bounded request
  threadpool, and blocking them behind supervisor work — whose actors
  re-enter this server over HTTP — is a deadlock. Same shape as
  ``insert_stats``: append under a local lock, let a background thread do
  the work.
- **Subscription index.** ``ref_uri -> {app names}`` built from each app's
  provenance sets (declared ``acq:mayUse`` ∪ observed ``prov:used``); the
  supervisor refreshes an app's subscriptions whenever its provenance
  changes and drops them on delete.
- **Cycle guard, in the data path.** App outputs are inserted through the
  same choke points, so two apps whose inputs cover each other's outputs
  would trigger each other forever — and ``measurement(frm="*")`` matches
  an app's own virtual points. Rules: an insert from ``app:X`` never
  triggers X, and cascades stop at depth 1 — an app-originated insert may
  trigger the apps that read that stream, but *those* apps' outputs trigger
  nothing (they carry a cascade marker through the source id). This is a
  data-path guard; the graph-side ``data_version`` exclusion covers a
  different loop (provenance writes).
- **Debounce.** A 50k-row bulk insert must produce one run, not fifty
  thousand. Notifications for an app coalesce for ``debounce_seconds``,
  then dispatch once; a ``min_interval`` floor bounds the rate for streams
  that never stop changing. Dispatch enters the app's own
  ``IntervalScheduler.trigger()`` (single-flight for all dispatch paths),
  so a change-triggered run and an interval run can never race one actor.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Iterable

logger = logging.getLogger("acquirium.apps.change_feed")

APP_SOURCE_PREFIX = "app:"
DEFAULT_DEBOUNCE_SECONDS = 0.5
DEFAULT_MIN_INTERVAL = 5.0


class ChangeFeed:
    """Data-change notifications → debounced, cycle-safe app dispatch."""

    def __init__(
        self,
        dispatch: Callable[[str, str], bool],
        *,
        debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
        min_interval: float = DEFAULT_MIN_INTERVAL,
    ):
        """``dispatch(app_name, reason) -> bool`` runs the app (True if it
        actually dispatched). It is called on the feed's own thread and may
        block briefly (a Ray remote call), never on an insert thread."""
        self._dispatch = dispatch
        self.debounce_seconds = float(debounce_seconds)
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._subs: dict[str, set[str]] = defaultdict(set)      # ref -> apps
        self._app_refs: dict[str, set[str]] = {}                 # app -> refs
        # Per-app pending state: first/last notify time, cascade-only flag.
        self._pending: dict[str, dict[str, Any]] = {}
        self._last_dispatch: dict[str, float] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.stats = {"notified": 0, "coalesced": 0, "dispatched": 0, "suppressed_cycles": 0}

    # ─────────────────────── subscriptions ───────────────────────

    def subscribe(self, app_name: str, ref_uris: Iterable[str]) -> None:
        """Replace ``app_name``'s subscription set (called on provenance change)."""
        refs = {str(u) for u in ref_uris if u}
        with self._lock:
            for old in self._app_refs.get(app_name, set()) - refs:
                self._subs[old].discard(app_name)
                if not self._subs[old]:
                    del self._subs[old]
            for new in refs:
                self._subs[new].add(app_name)
            if refs:
                self._app_refs[app_name] = refs
            else:
                self._app_refs.pop(app_name, None)

    def unsubscribe(self, app_name: str) -> None:
        self.subscribe(app_name, ())
        with self._lock:
            self._pending.pop(app_name, None)
            self._last_dispatch.pop(app_name, None)

    def subscribers(self, ref_uri: str) -> set[str]:
        with self._lock:
            return set(self._subs.get(str(ref_uri), ()))

    # ─────────────────────── the hook ───────────────────────

    def notify(self, source_id: str, ref_uris: Iterable[str], cascade: bool = False) -> None:
        """Record that streams changed. Enqueue-only; safe on any thread.

        ``source_id`` is the inserting datasource. If it is an app
        (``app:<name>``), that app is never triggered by its own insert, and
        the runs this insert triggers are marked as cascade — their own
        inserts then trigger nothing (depth 1). ``cascade=True`` says this
        insert *is* such a run's output.
        """
        if cascade:
            with self._lock:
                self.stats["suppressed_cycles"] += 1
            return
        origin_app = source_id[len(APP_SOURCE_PREFIX):] if source_id.startswith(APP_SOURCE_PREFIX) else None
        now = time.monotonic()
        with self._lock:
            targets: set[str] = set()
            for ref in ref_uris:
                targets.update(self._subs.get(str(ref), ()))
            if origin_app is not None and origin_app in targets:
                targets.discard(origin_app)
                self.stats["suppressed_cycles"] += 1
            if not targets:
                return
            self.stats["notified"] += 1
            for app in targets:
                p = self._pending.get(app)
                if p is None:
                    self._pending[app] = {"first": now, "last": now, "cascade": origin_app is not None}
                else:
                    p["last"] = now
                    p["cascade"] = p["cascade"] or origin_app is not None
                    self.stats["coalesced"] += 1
        self._wake.set()

    # ─────────────────────── the worker ───────────────────────

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="acquirium-change-feed")
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None

    def _due(self, now: float) -> list[tuple[str, bool]]:
        """Pop the apps whose debounce window has closed and floor allows."""
        due: list[tuple[str, bool]] = []
        with self._lock:
            for app, p in list(self._pending.items()):
                if now - p["last"] < self.debounce_seconds:
                    continue
                if now - self._last_dispatch.get(app, -1e18) < self.min_interval:
                    continue  # floored: stays pending, re-checked next wake
                due.append((app, p["cascade"]))
                del self._pending[app]
                self._last_dispatch[app] = now
        return due

    def _next_wait(self) -> float:
        with self._lock:
            if not self._pending:
                return 3600.0
            now = time.monotonic()
            waits = []
            for app, p in self._pending.items():
                w = max(
                    self.debounce_seconds - (now - p["last"]),
                    self.min_interval - (now - self._last_dispatch.get(app, -1e18)),
                    0.0,
                )
                waits.append(w)
            return max(min(waits), 0.01)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=self._next_wait())
            self._wake.clear()
            if self._stop.is_set():
                break
            for app, cascade in self._due(time.monotonic()):
                try:
                    if self._dispatch(app, "cascade" if cascade else "change"):
                        with self._lock:
                            self.stats["dispatched"] += 1
                except Exception:
                    logger.exception("change-triggered dispatch failed for '%s'", app)

    def flush_now(self) -> list[str]:
        """Test/ops hook: dispatch everything due right now, synchronously."""
        dispatched = []
        for app, cascade in self._due(time.monotonic()):
            if self._dispatch(app, "cascade" if cascade else "change"):
                dispatched.append(app)
        return dispatched

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                **self.stats,
                "subscribed_apps": len(self._app_refs),
                "subscribed_streams": len(self._subs),
                "pending": sorted(self._pending),
            }
