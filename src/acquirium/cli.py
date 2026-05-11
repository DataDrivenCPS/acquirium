from __future__ import annotations

"""acquirium CLI

Single subcommand:

  acquirium server [--config FILE] [--host HOST] [--port PORT] [--reload]
      Start the Acquirium FastAPI server, plus any [[drivers]] listed in
      the config as background threads.

      Set ``[server] enabled = false`` in the config to run drivers only
      (no FastAPI server).  Drivers connect to the remote Acquirium instance
      declared in the ``[driver]`` section (server_url / server_port).
"""

import importlib
import importlib.util
import inspect
import logging
import os
import signal
import sys
import threading
import time
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Optional

if TYPE_CHECKING:
    from acquirium.Driver import Driver
    from acquirium.Client.acquirium import Acquirium

import typer

app = typer.Typer(
    name="acquirium",
    help="Acquirium CLI — start the server or run drivers from a config file.",
    add_completion=False,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

# Mapping from acquirium.toml [server] keys to environment variable names.
_SERVER_ENV_MAP: dict[str, str] = {
    "data_dir":               "ACQUIRIUM_DATA_DIR",
    "pg_dsn":                 "PG_DSN",
    "duckdb_path":            "ACQUIRIUM_DUCKDB_PATH",
    "timeseries_backend":     "ACQUIRIUM_TIMESERIES_BACKEND",
    "graph_path":             "ACQUIRIUM_GRAPH_PATH",
    "graph_name":             "ACQUIRIUM_GRAPH_NAME",
    "ontology_dependencies":  "ACQUIRIUM_ONTOLOGY_DEPENDENCIES",
    "embedding_model":        "ACQUIRIUM_EMBEDDING_MODEL",
    "recreate":               "ACQUIRIUM_RECREATE",
    "workers":                "ACQUIRIUM_WORKERS",
}


def _load_config(path: Path | None) -> dict:
    """Load an acquirium.toml file.  Falls back to cwd/acquirium.toml if path is None."""
    if path is None:
        default = Path("acquirium.toml")
        if default.exists():
            path = default
        else:
            return {}
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    cfg["__config_dir"] = str(path.resolve().parent)
    return cfg


def _apply_server_env(cfg: dict) -> None:
    """Set env var defaults from cfg['server'].  Existing env vars are never overwritten."""
    server = cfg.get("server", {})
    config_dir = Path(cfg.get("__config_dir", Path.cwd()))
    for key, env_var in _SERVER_ENV_MAP.items():
        if key not in server:
            continue
        value = server[key]
        if key in {"data_dir", "duckdb_path", "graph_path"}:
            value = str((config_dir / value).resolve()) if not Path(value).is_absolute() else str(value)
        if isinstance(value, list):
            str_value = ",".join(str(v) for v in value)
        elif isinstance(value, bool):
            str_value = "true" if value else "false"
        else:
            str_value = str(value)
        os.environ.setdefault(env_var, str_value)


# ---------------------------------------------------------------------------
# Driver import helpers
# ---------------------------------------------------------------------------

def _import_driver_class(driver_spec: str, *, base_dir: Path | None = None) -> type:
    """Resolve a ``path/to/file.py:ClassName`` or ``my.module:ClassName`` spec to a Driver subclass.

    Raises ``ValueError`` on any resolution failure so callers in background
    threads see a real exception rather than a silent ``SystemExit``.
    """
    from acquirium.Driver import Driver as _Driver

    if ":" not in driver_spec:
        raise ValueError(
            f"driver spec must include a class name (e.g. my_driver.py:MyDriver), got {driver_spec!r}"
        )

    path_part, class_name = driver_spec.rsplit(":", 1)

    is_file = "/" in path_part or path_part.endswith(".py") or Path(path_part).exists()
    if is_file:
        file_path = Path(path_part)
        if not file_path.is_absolute():
            file_path = ((base_dir or Path.cwd()) / file_path).resolve()
        if not file_path.exists():
            raise ValueError(f"driver file not found: {path_part}")
        spec = importlib.util.spec_from_file_location("_acquirium_driver_module", file_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"could not load file: {path_part}")
        file_dir = str(file_path.parent)
        if file_dir not in sys.path:
            sys.path.insert(0, file_dir)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    else:
        try:
            mod = importlib.import_module(path_part)
        except ModuleNotFoundError as exc:
            raise ValueError(f"could not import module '{path_part}': {exc}") from exc

    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ValueError(f"'{class_name}' not found in {path_part}")
    if not (inspect.isclass(cls) and issubclass(cls, _Driver) and cls is not _Driver):
        raise ValueError(f"'{class_name}' is not a Driver subclass")
    return cls


def _driver_connect_cfg(
    driver_cfg: dict,
    *,
    fallback_host: str = "localhost",
    fallback_port: int = 8000,
) -> tuple[str, int, bool, float]:
    """Return (host, port, use_ssl, interval) from a [driver] config dict."""
    host = driver_cfg.get("server_url", fallback_host)
    # 0.0.0.0 is a bind address, not a connectable host
    if host == "0.0.0.0":
        host = "localhost"
    port = int(driver_cfg.get("server_port", fallback_port))
    use_ssl = driver_cfg.get("use_ssl", False)
    interval = float(driver_cfg.get("interval", 10.0))
    return host, port, use_ssl, interval


# ---------------------------------------------------------------------------
# Shared driver tick loop
# ---------------------------------------------------------------------------

def _run_driver_loop(
    driver: "Driver",
    aq: "Acquirium",
    interval: float,
    stop_event: threading.Event,
) -> None:
    """Tick loop shared by in-process and driver-only threads.

    Runs an initial tick immediately, then waits *interval* seconds between
    subsequent ticks.  Polls graph version before each tick and calls
    ``on_graph_change()`` when the version advances.  Calls ``driver.stop()``
    on exit regardless of how the loop ends.

    Connection errors cause the wait to follow exponential backoff (2 s →
    300 s) instead of the fixed *interval*, so the driver recovers quickly
    after a short outage without hammering on a prolonged one.  Non-connection
    errors (bugs in the driver) always log a full traceback.
    """
    from acquirium.Driver import _is_connection_error
    from acquirium.DriverState import ExponentialBackoff

    _log = logging.getLogger(f"acquirium.driver.{driver.__class__.__name__}")
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0)
    known_version = 0
    try:
        known_version = aq.graph_version()
    except Exception:
        pass

    def _tick() -> float:
        """Run one tick; return the wait time to use before the next tick."""
        try:
            driver.tick()
            if backoff.is_in_backoff():
                _log.info("Server connection restored.")
            backoff.record_success()
            return interval
        except Exception as exc:
            if _is_connection_error(exc):
                backoff.record_failure()
                wait = backoff.next_delay()
                _log.warning(
                    "Server unreachable (%s); next tick in %.1fs.",
                    type(exc).__name__,
                    wait,
                )
                return wait
            _log.exception("tick error")
            return interval

    wait = _tick()

    while not stop_event.wait(timeout=wait):
        try:
            v = aq.graph_version()
            if v != known_version:
                known_version = v
                try:
                    driver.on_graph_change()
                except Exception:
                    _log.exception("on_graph_change error")
        except Exception:
            pass
        wait = _tick()

    try:
        driver.stop()
    except Exception:
        _log.exception("stop error")


# ---------------------------------------------------------------------------
# Driver-only mode (server.enabled = false)
# ---------------------------------------------------------------------------

def _run_driver_only_mode(cfg: dict) -> None:
    """Run [[drivers]] against a remote Acquirium server without starting FastAPI."""
    from acquirium.Client.acquirium import Acquirium

    driver_cfg = cfg.get("driver", {})
    server_url, server_port, use_ssl, default_interval = _driver_connect_cfg(driver_cfg)
    stop_event = threading.Event()

    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    threads: list[threading.Thread] = []
    for entry in cfg.get("drivers", []):
        spec = entry.get("spec")
        if not spec:
            typer.echo("Warning: [[drivers]] entry missing 'spec'; skipping", err=True)
            continue
        overrides = {k: v for k, v in entry.items() if k != "spec"}
        merged = {**cfg, "driver": {**driver_cfg, **overrides}, "__driver_id": spec}
        interval = float(overrides.get("interval", driver_cfg.get("interval", default_interval)))

        try:
            driver_cls = _import_driver_class(
                spec, base_dir=Path(cfg.get("__config_dir", Path.cwd()))
            )
            aq = Acquirium(
                server_url=server_url,
                server_port=server_port,
                use_ssl=use_ssl,
                insert_batch_rows=int(merged["driver"].get("insert_batch_rows", 50_000)),
            )
            driver = driver_cls(aq, merged)
            driver.setup()
        except Exception as exc:
            typer.echo(f"Driver {spec} failed to start: {exc}", err=True)
            continue

        t = threading.Thread(
            target=_run_driver_loop,
            args=(driver, aq, interval, stop_event),
            daemon=True,
            name=f"acquirium-driver-{spec.rsplit(':', 1)[-1]}",
        )
        t.start()
        threads.append(t)
        typer.echo(f"Started driver: {spec}")

    if not threads:
        typer.echo("No drivers started; exiting.", err=True)
        return

    typer.echo(f"Running {len(threads)} driver(s). Ctrl-C or SIGTERM to stop.")
    try:
        stop_event.wait()
    except KeyboardInterrupt:
        typer.echo("\nShutting down...")
        stop_event.set()
    for t in threads:
        t.join(timeout=10)
    typer.echo("Done.")


# ---------------------------------------------------------------------------
# server subcommand
# ---------------------------------------------------------------------------

@app.command("server")
def server_cmd(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml")] = None,
    host: Annotated[Optional[str], typer.Option("--host", help="Bind host")] = None,
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Bind port")] = None,
    reload: Annotated[bool, typer.Option("--reload", help="Enable uvicorn auto-reload (development)")] = False,
    workers: Annotated[Optional[int], typer.Option("--workers", "-w", help="Uvicorn worker processes (timescale backend only; incompatible with --reload)")] = None,
) -> None:
    """Start the Acquirium server and any [[drivers]] declared in the config.

    Set ``[server] enabled = false`` in the config to run drivers only
    (no HTTP server).
    """
    cfg = _load_config(config)
    _apply_server_env(cfg)

    server_cfg = cfg.get("server", {})
    if not server_cfg.get("enabled", True):
        _run_driver_only_mode(cfg)
        return

    import uvicorn

    # Propagate the config path so the lifespan can start [[drivers]] in-process.
    if config:
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(config.resolve()))
    elif Path("acquirium.toml").exists():
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(Path("acquirium.toml").resolve()))

    effective_host = host or server_cfg.get("host", "0.0.0.0")
    effective_port = port or server_cfg.get("port", 8000)
    effective_workers = (
        workers
        or int(os.environ.get("ACQUIRIUM_WORKERS", 0))
        or server_cfg.get("workers", 1)
    )
    if reload:
        effective_workers = 1  # uvicorn forbids workers > 1 with reload

    # Raise KeyboardInterrupt on SIGTERM so uvicorn's existing Ctrl-C handler
    # runs the same graceful shutdown path for both signals.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    typer.echo(f"Starting Acquirium server on {effective_host}:{effective_port}")
    uvicorn.run(
        "acquirium.Server.app:app",
        host=effective_host,
        port=effective_port,
        reload=reload,
        workers=effective_workers,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app()


if __name__ == "__main__":
    main()
