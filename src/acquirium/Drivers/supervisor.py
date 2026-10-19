from __future__ import annotations

import json
import logging
import pickle
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger("acquirium.driver.supervis")


class DriverSupervisor:
    """Owns the driver processes of one server process, keyed by name.

    Lives in the FastAPI process. start_driver() spawns a
    ``python -m acquirium.Drivers.runner`` child that imports the driver,
    connects back to this server over HTTP, runs setup, reports the result,
    and starts the tick loop. The internal lock is held across setup so two
    drivers' setup-time graph writes can never race.

    A child stops when its stdin closes — on stop_driver(), or when this
    process dies — so drivers never outlive their server.
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
        driver_section = config.get("driver", {})
        effective_interval = float(
            interval if interval is not None else driver_section.get("interval", 10.0)
        )
        if effective_interval <= 0:
            raise ValueError("driver interval must be greater than zero")
        driver_name = name or spec.rsplit(":", 1)[-1]

        with self._lock:
            if driver_name in self._drivers:
                raise ValueError(f"Driver '{driver_name}' is already running")

            proc = subprocess.Popen(
                [sys.executable, "-m", "acquirium.Drivers.runner"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                # Its own session: a terminal Ctrl-C reaches only the server,
                # which then stops drivers in order instead of mid-tick.
                start_new_session=True,
            )
            assert proc.stdin is not None and proc.stdout is not None
            try:
                pickle.dump(
                    {
                        "spec": spec,
                        "config": config,
                        "interval": effective_interval,
                        "server_url": self.server_url,
                        "server_port": self.server_port,
                        "use_ssl": self.use_ssl,
                    },
                    proc.stdin,
                )
                proc.stdin.flush()
                # The status line is the child's only stdout output.
                line = proc.stdout.readline()
                proc.stdout.close()
            except BaseException:
                proc.kill()
                proc.wait()
                raise
            status = json.loads(line) if line else {"ok": False, "error": None}
            if not status["ok"]:
                proc.stdin.close()
                code = proc.wait()
                raise RuntimeError(
                    f"Driver '{driver_name}' failed to start: "
                    f"{status['error'] or f'process exited with code {code}'}"
                )

            record = {
                "name": driver_name,
                "spec": spec,
                "interval": effective_interval,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "proc": proc,
            }
            self._drivers[driver_name] = record
            logger.info(
                "Started driver '%s' (%s, interval=%.1fs, pid=%d)",
                driver_name, spec, effective_interval, proc.pid,
            )
            return self._public_info(record)

    def stop_driver(self, name: str, *, timeout: float = 10.0) -> dict[str, Any]:
        with self._lock:
            record = self._drivers.pop(name, None)
        if record is None:
            raise KeyError(f"No running driver named '{name}'")

        proc: subprocess.Popen = record["proc"]
        _signal_stop(proc)
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning("Driver '%s' did not exit within %.1fs; killing it", name, timeout)
            proc.kill()
            proc.wait()
            return {"name": name, "stopped": False, "error": "shutdown timed out"}
        if code != 0:
            raise RuntimeError(f"Driver '{name}' shutdown failed: exit code {code}")
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

        # Signal every driver to exit first, then join them against one
        # deadline, so N drivers' shutdown timeouts overlap instead of
        # stacking serially the way a loop over stop_driver() would.
        for record in records:
            _signal_stop(record["proc"])
        deadline = time.monotonic() + timeout
        for record in records:
            proc: subprocess.Popen = record["proc"]
            try:
                code = proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                logger.warning(
                    "Driver '%s' did not exit within %.1fs; killing it", record["name"], timeout,
                )
                proc.kill()
                proc.wait()
                continue
            if code != 0:
                logger.error("Driver '%s' shutdown failed: exit code %d", record["name"], code)
            else:
                logger.info("Stopped driver '%s'", record["name"])

    def _public_info(self, record: dict[str, Any]) -> dict[str, Any]:
        code = record["proc"].poll()
        if code is None:
            status = "running"
        elif code == 0:
            status = "stopped"
        else:
            status = f"failed: exit code {code}"
        return {
            "name": record["name"],
            "spec": record["spec"],
            "interval": record["interval"],
            "started_at": record["started_at"],
            "pid": record["proc"].pid,
            "status": status,
        }


def _signal_stop(proc: subprocess.Popen) -> None:
    """Close the child's stdin, which its runner treats as a stop request."""
    try:
        assert proc.stdin is not None
        proc.stdin.close()
    except OSError:
        pass  # Already exited and the pipe is broken; wait() reports how.
