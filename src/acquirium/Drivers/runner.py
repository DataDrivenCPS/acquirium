from __future__ import annotations
from typing import TYPE_CHECKING, Any

import asyncio
import logging
import ray

from acquirium.internals._log import configure_logging, timed_debug as _timed_debug
from acquirium.internals.internals_namespaces import *

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Drivers.Driver import Driver

logger = logging.getLogger("acquirium.driver.runner")

@ray.remote
class DriverRunner:
    """Run one driver's tick loop in its own Ray actor.

    The driver is constructed inside the actor so its state (DriverState,
    client connections) never crosses the process boundary. Lifecycle:

        runner = DriverRunner.remote(driver_cls, cfg, aq, interval)
        ray.get(runner.setup.remote())   # serially across actors
        run_ref = runner.run.remote()
        ...
        runner.stop.remote()             # loop exits, driver.stop() runs
        ray.get(run_ref)                 # join
    """

    def __init__(
        self,
        driver_cls: type[Driver],
        driver_cfg: dict,
        acquirium_cli: Acquirium,
        interval: float,
    ):
        # Ray workers don't inherit the server process's logging config.
        configure_logging()
        self.driver: Driver = driver_cls(acquirium_cli, driver_cfg)
        self.acquirium_cli = acquirium_cli
        self.interval = interval
        self.source_version = 0
        self.logger = logging.getLogger(
            f"acquirium.driver.{type(self.driver).__name__}"
        )
        self._stop_event = asyncio.Event()
        # Captured when run() starts so the sync stop() can flip the event on
        # the loop thread via call_soon_threadsafe.
        self._loop: asyncio.AbstractEventLoop | None = None

    def setup(self) -> None:
        """One-time driver setup.

        The caller must ray.get() these one actor at a time so setup-time
        graph writes cannot race each other (DriverSupervisor holds its lock
        across setup for exactly this reason).
        """
        self.driver.setup()
        # Seed after setup so the loop doesn't fire on_graph_change() for the
        # pre-existing graph or this driver's own setup insertions.
        try:
            self.source_version = int(self.acquirium_cli.graph_status()["source_version"])
        except Exception:
            pass

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        name = type(self.driver).__name__
        self.logger.info("Starting driver runner for %s", name)
        self._tick()
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass
            # Version check failures (e.g. server briefly unreachable) must
            # not skip the tick, so they are guarded separately.
            try:
                source_version = int(self.acquirium_cli.graph_status()["source_version"])
                if source_version != self.source_version:
                    self.source_version = source_version
                    try:
                        self.driver.on_graph_change()
                    except Exception:
                        self.logger.exception("on_graph_change error")
            except Exception:
                pass
            self._tick()
        self.logger.debug("driver loop exit: %s", name)
        try:
            self.driver.stop()
        except Exception:
            self.logger.exception("stop error")

    def stop(self) -> None:
        """Signal run() to exit; driver.stop() cleanup happens there.

        Sync methods run off the actor's event-loop thread, so the
        asyncio.Event must be set via the loop rather than touched directly.
        """
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._stop_event.set)
        else:
            # run() hasn't captured the loop yet; no coroutine is waiting.
            self._stop_event.set()

    def _tick(self) -> None:
        try:
            with _timed_debug(self.logger, "tick"):
                self.driver.tick()
        except Exception:
            self.logger.exception("tick error")
