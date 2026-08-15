from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ray

from acquirium.Drivers.runner import DriverRunner


logger = logging.getLogger("acquirium.driver.supervis")


def _worker_pythonpath(source_dir: str) -> str:
    """Prepend ``source_dir`` to this process's PYTHONPATH for a Ray worker.

    runtime_env env_vars replace rather than extend, so the inherited value has
    to be carried over explicitly or the worker loses it.
    """
    inherited = os.environ.get("PYTHONPATH", "")
    if not inherited:
        return source_dir
    if source_dir in inherited.split(os.pathsep):
        return inherited
    return source_dir + os.pathsep + inherited

class DriverSupervisor:
    """Owns the DriverRunner actors of one server process, keyed by name.

    Lives in the FastAPI process. start_driver() imports the driver class,
    spawns a DriverRunner actor that connects back to this server over HTTP,
    runs setup, and starts the tick loop.

    Locking discipline mirrors AppSupervisor: ``_lock`` guards only the
    ``_drivers`` dict and is never held across an actor call (setup re-enters
    this server over HTTP — register_datasource, graph writes — and would
    deadlock a saturated request threadpool contending on the same lock).
    The name is reserved with a placeholder record instead, and concurrent
    setups serialize their graph writes via ``_build_lock``, which no request
    path takes.
    """

    def __init__(self, server_url: str, server_port: int, use_ssl: bool = False):
        self.server_url = server_url
        self.server_port = int(server_port)
        self.use_ssl = bool(use_ssl)
        self._drivers: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._build_lock = threading.Lock()

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
        from acquirium.cli import _driver_source_dir
        from acquirium.Client.acquirium import Acquirium

        driver_section = config.get("driver", {})
        effective_interval = float(
            interval if interval is not None else driver_section.get("interval", 10.0)
        )
        driver_name = name or spec.rsplit(":", 1)[-1]

        placeholder = {
            "name": driver_name,
            "spec": spec,
            "interval": effective_interval,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "actor": None,
            "run_ref": None,
        }
        with self._lock:
            if driver_name in self._drivers:
                raise ValueError(f"Driver '{driver_name}' is already running")
            self._drivers[driver_name] = placeholder

        try:
            base_dir = Path(config.get("__config_dir", Path.cwd()))
            # Path resolution only — the import itself happens inside the
            # actor, so the server process never needs the driver's deps.
            source_dir = _driver_source_dir(spec, base_dir=base_dir)
            aq = Acquirium(
                server_url=self.server_url,
                server_port=self.server_port,
                use_ssl=self.use_ssl,
                insert_batch_rows=int(driver_section.get("insert_batch_rows", 50_000)),
            )
            runner_cls = DriverRunner
            if source_dir is not None:
                runner_cls = DriverRunner.options(
                    runtime_env={"env_vars": {"PYTHONPATH": _worker_pythonpath(source_dir)}}
                )
            # Setup-time graph writes of concurrent driver starts serialize on
            # the build lock (never the record lock — see class docstring).
            with self._build_lock:
                runner = runner_cls.remote(spec, config, aq, effective_interval, str(base_dir))
                try:
                    ray.get(runner.setup.remote())
                except Exception as exc:
                    ray.kill(runner)
                    # Import/setup failures arrive wrapped in RayActorError;
                    # surface the underlying cause so the HTTP detail reads
                    # like the real error, not an actor traceback dump.
                    cause = getattr(exc, "cause", None)
                    raise RuntimeError(
                        f"Driver '{driver_name}' failed to start: {cause or exc}"
                    ) from exc
            run_ref = runner.run.remote()
        except Exception:
            with self._lock:
                if self._drivers.get(driver_name) is placeholder:
                    del self._drivers[driver_name]
            raise

        record = {**placeholder, "actor": runner, "run_ref": run_ref}
        with self._lock:
            superseded = self._drivers.get(driver_name) is not placeholder
            if not superseded:
                self._drivers[driver_name] = record
        if superseded:
            # stop_all (or a shutdown) cleared the reservation while setup was
            # in flight — this driver must not outlive it.
            runner.stop.remote()
            ray.kill(runner)
            raise RuntimeError(f"Driver '{driver_name}' was stopped during startup")
        logger.info("Started driver '%s' (%s, interval=%.1fs)", driver_name, spec, effective_interval)
        return self._public_info(record)

    def stop_driver(self, name: str, *, timeout: float = 10.0) -> dict[str, Any]:
        with self._lock:
            record = self._drivers.get(name)
            if record is None:
                raise KeyError(f"No running driver named '{name}'")
            if record.get("actor") is None:
                raise ValueError(f"Driver '{name}' is still starting; retry shortly")
            self._drivers.pop(name)

        record["actor"].stop.remote()
        try:
            ray.get(record["run_ref"], timeout=timeout)
        except ray.exceptions.GetTimeoutError:
            logger.warning("Driver '%s' did not exit within %.1fs; killing actor", name, timeout)
            ray.kill(record["actor"])
            return {"name": name, "stopped": False, "error": "shutdown timed out"}
        except Exception as exc:
            ray.kill(record["actor"])
            raise RuntimeError(f"Driver '{name}' shutdown failed: {exc}") from exc
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
        # Reservation placeholders (actor still being built) have nothing to
        # stop; their start path will find the record gone and clean up.
        records = [r for r in records if r.get("actor") is not None]
        if not records:
            return

        for record in records:
            try:
                record["actor"].stop.remote()
            except Exception:
                logger.exception("Failed to signal stop for driver '%s'", record["name"])

        by_ref = {record["run_ref"]: record for record in records}
        ready, not_ready = ray.wait(list(by_ref), num_returns=len(by_ref), timeout=timeout)
        for ref in ready:
            try:
                ray.get(ref)
            except Exception:
                logger.exception("Driver '%s' shutdown failed", by_ref[ref]["name"])
        for ref in not_ready:
            logger.warning(
                "Driver '%s' did not exit within %.1fs; killing actor",
                by_ref[ref]["name"], timeout,
            )

        for record in records:
            ray.kill(record["actor"])
            logger.info("Stopped driver '%s'", record["name"])

    def _public_info(self, record: dict[str, Any]) -> dict[str, Any]:
        if record.get("run_ref") is None:
            return {
                "name": record["name"],
                "spec": record["spec"],
                "interval": record["interval"],
                "started_at": record["started_at"],
                "status": "starting",
            }
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
