from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ray

from acquirium.internals._log import configure_logging, timed_debug as _timed_debug

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Driver import Driver

logger = logging.getLogger("acquirium.ray")


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
        self.graph_version = 0
        self.logger = logging.getLogger(
            f"acquirium.driver.{type(self.driver).__name__}"
        )
        self._stop_event = asyncio.Event()

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
            self.graph_version = self.acquirium_cli.graph_version()
        except Exception:
            pass

    async def run(self) -> None:
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
                v = self.acquirium_cli.graph_version()
                if v != self.graph_version:
                    self.graph_version = v
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
        """Signal run() to exit; driver.stop() cleanup happens there."""
        self._stop_event.set()

    def _tick(self) -> None:
        try:
            with _timed_debug(self.logger, "tick"):
                self.driver.tick()
        except Exception:
            self.logger.exception("tick error")


class DriverSupervisor:
    """Owns the DriverRunner actors of one server process, keyed by name.

    Lives in the FastAPI process. start_driver() imports the driver class,
    spawns a DriverRunner actor that connects back to this server over HTTP,
    runs setup, and starts the tick loop. The internal lock is held across
    setup so two drivers' setup-time graph writes can never race.
    """

    def __init__(self, server_url: str, server_port: int, use_ssl: bool = False):
        self.server_url = server_url
        self.server_port = int(server_port)
        self.use_ssl = bool(use_ssl)
        self._drivers: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return f"{'https' if self.use_ssl else 'http'}://{self.server_url}:{self.server_port}"

    def start_driver(
        self,
        *,
        spec: str,
        config: dict,
        interval: float | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        from acquirium.cli import _import_driver_class
        from acquirium.Client.acquirium import Acquirium

        driver_section = config.get("driver", {})
        effective_interval = float(
            interval if interval is not None else driver_section.get("interval", 10.0)
        )
        driver_name = name or spec.rsplit(":", 1)[-1]

        with self._lock:
            if driver_name in self._drivers:
                raise ValueError(f"Driver '{driver_name}' is already running")

            base_dir = Path(config.get("__config_dir", Path.cwd()))
            driver_cls = _import_driver_class(spec, base_dir=base_dir)
            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
                insert_batch_rows=int(driver_section.get("insert_batch_rows", 50_000)),
            )
            runner = DriverRunner.remote(driver_cls, config, aq, effective_interval)
            try:
                ray.get(runner.setup.remote())
            except Exception:
                ray.kill(runner)
                raise
            run_ref = runner.run.remote()

            record = {
                "name": driver_name,
                "spec": spec,
                "interval": effective_interval,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "actor": runner,
                "run_ref": run_ref,
            }
            self._drivers[driver_name] = record
            logger.info("Started driver '%s' (%s, interval=%.1fs)", driver_name, spec, effective_interval)
            return self._public_info(record)

    def stop_driver(self, name: str, *, timeout: float = 10.0) -> dict[str, Any]:
        with self._lock:
            record = self._drivers.pop(name, None)
        if record is None:
            raise KeyError(f"No running driver named '{name}'")

        record["actor"].stop.remote()
        try:
            ray.get(record["run_ref"], timeout=timeout)
        except Exception:
            logger.warning("Driver '%s' did not exit within %.1fs; killing actor", name, timeout)
        ray.kill(record["actor"])
        logger.info("Stopped driver '%s'", name)
        return {"name": name, "stopped": True}

    def list_drivers(self) -> list[dict[str, Any]]:
        with self._lock:
            records = list(self._drivers.values())
        return [self._public_info(r) for r in records]

    def stop_all(self, *, timeout: float = 10.0) -> None:
        with self._lock:
            records = list(self._drivers.values())
            self._drivers.clear()
        if not records:
            return

        # Signal every driver to exit first, then join them in one window, so
        # N drivers' shutdown timeouts overlap instead of stacking serially the
        # way a loop over stop_driver() would.
        for record in records:
            try:
                record["actor"].stop.remote()
            except Exception:
                logger.exception("Failed to signal stop for driver '%s'", record["name"])

        by_ref = {record["run_ref"]: record for record in records}
        _, not_ready = ray.wait(list(by_ref), num_returns=len(by_ref), timeout=timeout)
        for ref in not_ready:
            logger.warning(
                "Driver '%s' did not exit within %.1fs; killing actor",
                by_ref[ref]["name"], timeout,
            )

        for record in records:
            ray.kill(record["actor"])
            logger.info("Stopped driver '%s'", record["name"])

    def _public_info(self, record: dict[str, Any]) -> dict[str, Any]:
        ready, _ = ray.wait([record["run_ref"]], timeout=0)
        if not ready:
            status = "running"
        else:
            # run() returned: clean exit, or the loop/actor died with an error.
            try:
                ray.get(record["run_ref"])
                status = "stopped"
            except Exception as exc:
                status = f"failed: {exc}"
        return {
            "name": record["name"],
            "spec": record["spec"],
            "interval": record["interval"],
            "started_at": record["started_at"],
            "status": status,
        }
