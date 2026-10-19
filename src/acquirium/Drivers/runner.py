"""Run one driver's tick loop, in its own process.

``DriverSupervisor`` starts each driver as ``python -m acquirium.Drivers.runner``.
The child talks to the server over HTTP like any other client, so a driver
that crashes, leaks, or blocks cannot take the server down with it.

Protocol between supervisor and child:

- **stdin** carries one pickled start payload (see :func:`main`) and then
  stays open. EOF on stdin means "stop": the supervisor closes it to stop
  the driver, and the OS closes it if the supervisor dies, so a driver never
  outlives its server.
- **stdout** carries one JSON status line once setup finishes —
  ``{"ok": true}`` or ``{"ok": false, "error": "..."}`` — so setup failures
  reach whoever started the driver. Everything else the driver writes to
  stdout is redirected to stderr.
- **stderr** is inherited, so driver logs land in the server's log.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, BinaryIO

import json
import logging
import os
import pickle
import signal
import sys
import threading
import time
import traceback
from pathlib import Path

from acquirium.internals._log import configure_logging, timed_debug as _timed_debug

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Drivers.Driver import Driver

logger = logging.getLogger("acquirium.driver.runner")

# Floor for graph-change polling when a driver ticks faster than this. Ticking
# is the driver's own cadence — for ingest drivers it is also how often buffered
# observations are inserted — while graph changes are rare, so a fast tick must
# not turn into a fast poll of the server.
DEFAULT_GRAPH_POLL_INTERVAL = 10.0


class DriverRunner:
    """Own one driver instance and drive its tick loop.

    Lifecycle::

        runner = DriverRunner(driver_cls, cfg, aq, interval)
        runner.setup()
        runner.run()      # blocks until stop() is called from another thread
    """

    def __init__(
        self,
        driver_cls: type[Driver],
        driver_cfg: dict,
        acquirium_cli: Acquirium,
        interval: float,
    ):
        self.driver: Driver = driver_cls(acquirium_cli, driver_cfg)
        self.acquirium_cli = acquirium_cli
        self.interval = float(interval)
        if self.interval <= 0:
            raise ValueError("driver interval must be greater than zero")
        configured_poll = driver_cfg.get("driver", {}).get("graph_poll_interval")
        self.graph_poll_interval = (
            float(configured_poll)
            if configured_poll is not None
            else max(self.interval, DEFAULT_GRAPH_POLL_INTERVAL)
        )
        if self.graph_poll_interval <= 0:
            raise ValueError("graph_poll_interval must be greater than zero")
        self._last_graph_poll = 0.0
        self.source_version = 0
        self.logger = logging.getLogger(
            f"acquirium.driver.{type(self.driver).__name__}"
        )
        self._stop_event = threading.Event()

    def setup(self) -> None:
        """One-time driver setup.

        The supervisor starts drivers one at a time and waits for each setup,
        so setup-time graph writes cannot race each other.
        """
        self.driver.setup()
        self.driver._after_setup()
        # Seed after setup so the loop doesn't fire on_graph_change() for the
        # pre-existing graph or this driver's own setup insertions.
        self._last_graph_poll = time.monotonic()
        try:
            self.source_version = int(self.acquirium_cli.graph_status()["source_version"])
        except Exception:
            pass

    def run(self) -> None:
        name = type(self.driver).__name__
        self.logger.info("Starting driver runner for %s", name)
        self._tick()
        next_tick = time.monotonic() + self.interval
        next_graph_poll = self._last_graph_poll + self.graph_poll_interval
        while not self._stop_event.is_set():
            timeout = max(0.0, min(next_tick, next_graph_poll) - time.monotonic())
            if self._stop_event.wait(timeout):
                break
            now = time.monotonic()
            if now >= next_graph_poll:
                self._poll_graph_version()
                next_graph_poll = time.monotonic() + self.graph_poll_interval
            if now >= next_tick:
                self._tick()
                next_tick = time.monotonic() + self.interval
        self.logger.debug("driver loop exit: %s", name)
        try:
            self.driver._shutdown()
        except Exception:
            self.logger.exception("shutdown error")
            raise

    def stop(self) -> None:
        """Ask run() to exit after the current tick; cleanup happens there."""
        self._stop_event.set()

    def _poll_graph_version(self) -> None:
        """Fire on_graph_change() when the server's source generation advances.

        Polled on its own cadence rather than every tick: graph mutations are
        rare, while the tick interval is the driver's data cadence and may be
        far shorter. Version check failures (e.g. server briefly unreachable)
        must not skip the tick, so everything here is guarded.
        """
        now = time.monotonic()
        if now - self._last_graph_poll < self.graph_poll_interval:
            return
        self._last_graph_poll = now
        try:
            source_version = int(self.acquirium_cli.graph_status()["source_version"])
        except Exception:
            return
        if source_version == self.source_version:
            return
        self.source_version = source_version
        try:
            self.driver.on_graph_change()
        except Exception:
            self.logger.exception("on_graph_change error")

    def _tick(self) -> None:
        try:
            with _timed_debug(self.logger, "tick"):
                self.driver.tick()
        except Exception:
            self.logger.exception("tick error")


def _report(status: BinaryIO, **message: Any) -> None:
    status.write(json.dumps(message).encode() + b"\n")
    status.flush()


def main() -> int:
    """Child-process entry point; see the module docstring for the protocol.

    The start payload is a dict with ``spec``, ``config``, ``interval``,
    ``server_url``, ``server_port``, and ``use_ssl``.
    """
    # Keep the real stdout for the status line and point fd 1 at stderr, so
    # stray prints from driver code (or C extensions) cannot corrupt it.
    status = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    configure_logging()

    payload = pickle.load(sys.stdin.buffer)
    try:
        from acquirium.cli import _import_driver_class
        from acquirium.Client.acquirium import Acquirium

        config = payload["config"]
        base_dir = config.get("__config_dir", os.getcwd())
        driver_cls = _import_driver_class(payload["spec"], base_dir=Path(base_dir))
        aq = Acquirium(
            server_url=payload["server_url"],
            server_port=payload["server_port"],
            use_ssl=payload["use_ssl"],
        )
        runner = DriverRunner(driver_cls, config, aq, payload["interval"])
        runner.setup()
    except BaseException as exc:
        logger.error("driver %s failed to start:\n%s", payload["spec"], traceback.format_exc())
        _report(status, ok=False, error=f"{type(exc).__name__}: {exc}")
        return 1
    _report(status, ok=True)

    def _stop_on_eof() -> None:
        sys.stdin.buffer.read()
        runner.stop()

    threading.Thread(target=_stop_on_eof, name="driver-stdin", daemon=True).start()
    signal.signal(signal.SIGTERM, lambda *_: runner.stop())
    try:
        runner.run()
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
