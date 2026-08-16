"""Run [[drivers]] on *this* machine against a remote Acquirium server.

Edge execution: the driver process lives next to the data source (a plant
PLC network, a lab box) while the server runs elsewhere. Nothing new in
the driver contract makes this possible — drivers already do every graph
and datasource write through the injected ``Acquirium`` HTTP client from
inside ``setup()``, ``declare()``/``add()`` flow through the same client,
and ``_shutdown()`` flushes buffered rows on stop — so a clean edge stop
loses nothing in transit. Per-driver environments (``env = {...}``) work
locally unchanged: the venvs land under this machine's env storage root.

The mechanics reuse the server's own machinery: a local ``ray.init()`` and
a :class:`DriverSupervisor` constructed with the *remote* address, so
``DriverRunner`` is the identical actor either way. The remote server's
``/drivers/list`` does not know about edge drivers — they are visible
through their datasource registration and the data they write.
"""
from __future__ import annotations

import logging
import signal
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger("acquirium.driver.local")


def _wait_for_server(base_url: str, *, attempts: int = 30, delay: float = 2.0) -> bool:
    import time

    import requests

    for _ in range(attempts):
        try:
            if requests.get(f"{base_url}/health", timeout=5).ok:
                return True
        except requests.RequestException:
            pass
        time.sleep(delay)
    return False


def run_drivers_locally(
    cfg: dict,
    *,
    server_url: str,
    server_port: int,
    use_ssl: bool = False,
    env_storage_root: Path | str | None = None,
    stop_event: threading.Event | None = None,
    ray_kwargs: dict[str, Any] | None = None,
) -> int:
    """Start every [[drivers]] entry locally, run until stopped, return a
    process exit code (0 clean, 1 if any driver failed to start).

    Blocks on ``stop_event`` (SIGINT/SIGTERM set it when run from the CLI).
    On stop, drivers are signalled, joined (buffers flushed), and Ray shut
    down.
    """
    import ray

    from acquirium.Drivers.supervisor import DriverSupervisor

    base = f"{'https' if use_ssl else 'http'}://{server_url}:{server_port}"
    if not _wait_for_server(base):
        logger.error("Acquirium server never answered at %s", base)
        return 1

    entries = cfg.get("drivers", [])
    if not entries:
        logger.error("No [[drivers]] entries in config; nothing to run")
        return 1

    ray.init(ignore_reinit_error=True, **(ray_kwargs or {}))
    supervisor = DriverSupervisor(
        server_url=server_url, server_port=server_port, use_ssl=use_ssl,
        env_storage_root=env_storage_root,
    )
    stop_event = stop_event or threading.Event()
    failures = 0
    driver_cfg = cfg.get("driver", {})
    # Match the server's config-driver semantics: entry keys override the
    # [driver] section, and the connect address in the merged config is the
    # remote server (drivers read it for their own client if they build one).
    for entry in entries:
        spec = entry.get("spec")
        if not spec:
            logger.warning("[[drivers]] entry missing 'spec'; skipping")
            continue
        overrides = {k: v for k, v in entry.items() if k not in ("spec", "name")}
        merged = {
            **cfg,
            "driver": {**driver_cfg, **overrides,
                       "server_url": server_url, "server_port": server_port, "use_ssl": use_ssl},
        }
        interval = float(overrides.get("interval", driver_cfg.get("interval", 10.0)))
        try:
            info = supervisor.start_driver(
                spec=spec, config=merged, interval=interval, name=entry.get("name"),
            )
            logger.info("Started local driver '%s' (%s, interval=%.1fs) -> %s",
                        info["name"], spec, info["interval"], base)
        except Exception:
            failures += 1
            logger.exception("Local driver %s failed to start", spec)

    if not supervisor.list_drivers():
        ray.shutdown()
        return 1

    try:
        stop_event.wait()
    finally:
        logger.info("Stopping local drivers")
        supervisor.stop_all()
        ray.shutdown()
    return 1 if failures else 0


def install_stop_signals(stop_event: threading.Event) -> None:
    """SIGINT/SIGTERM -> set the event (so drivers get a clean shutdown)."""
    def _handler(*_):
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except ValueError:
            pass  # not on the main thread
