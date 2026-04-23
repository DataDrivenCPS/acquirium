from __future__ import annotations

"""acquirium CLI

Two subcommands:

  acquirium server [--config FILE] [--host HOST] [--port PORT] [--reload]
      Start the Acquirium FastAPI server via uvicorn.  Config file values are
      applied as environment-variable defaults before uvicorn starts, so
      existing env vars always take precedence.

      Any [[drivers]] entries in the config file are auto-started as background
      threads alongside the server.

  acquirium run DRIVER [--config FILE] [--interval SECONDS]
      Load and run a Driver subclass.  DRIVER can be:
        - path/to/file.py:ClassName   (file path + explicit class)
        - path/to/file.py             (file path, auto-discovers first Driver subclass)
        - my.module:ClassName         (dotted import path + explicit class)
        - my.module                   (dotted import, auto-discovers first Driver subclass)
"""

import importlib
import importlib.util
import inspect
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
    help="Acquirium CLI — start the server or run a data-collection driver.",
    add_completion=False,
)

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
        return tomllib.load(f)


def _apply_server_env(cfg: dict) -> None:
    """Set env var defaults from cfg['server'].  Existing env vars are never overwritten."""
    server = cfg.get("server", {})
    for key, env_var in _SERVER_ENV_MAP.items():
        if key not in server:
            continue
        value = server[key]
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

def _import_driver_class(driver_spec: str) -> type:
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
        file_path = Path(path_part).resolve()
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


def _check_graph_change(driver: "Driver", aq: "Acquirium", known_version: int) -> int:
    """Call driver.on_graph_change() if graph version has advanced. Returns updated known version."""
    try:
        v = aq.graph_version()
    except Exception:
        return known_version
    if v != known_version:
        driver.on_graph_change()
        return v
    return known_version


def _wait_for_server(host: str, port: int, *, timeout: float = 30.0, stop: threading.Event) -> bool:
    """Poll GET /health until the server responds 200 or timeout/stop fires."""
    import requests

    deadline = time.monotonic() + timeout
    url = f"http://{host}:{port}/health"
    while time.monotonic() < deadline and not stop.is_set():
        try:
            if requests.get(url, timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        stop.wait(timeout=1.0)
    return False


def _run_default_driver(
    driver_entry: dict,
    server_host: str,
    server_port: int,
    cfg: dict,
    stop: threading.Event,
) -> None:
    """Thread target: wait for server, then run a driver from a [[drivers]] entry."""
    from acquirium.Client.acquirium import Acquirium

    spec = driver_entry["spec"]
    driver_overrides = {k: v for k, v in driver_entry.items() if k != "spec"}
    merged_cfg = {**cfg, "driver": {**cfg.get("driver", {}), **driver_overrides}}

    driver_cfg = merged_cfg.get("driver", {})
    connect_host, connect_port, use_ssl, interval = _driver_connect_cfg(
        driver_cfg, fallback_host=server_host, fallback_port=server_port
    )

    # Wait for server to be ready before setup()
    if not _wait_for_server(connect_host, connect_port, stop=stop):
        typer.echo(f"Default driver {spec}: server did not become ready in time, aborting.", err=True)
        return

    if stop.is_set():
        return

    try:
        driver_cls = _import_driver_class(spec)
        aq = Acquirium(server_url=connect_host, server_port=connect_port, use_ssl=use_ssl)
        driver = driver_cls(aq, merged_cfg)
        driver.setup()
        known_version = aq.graph_version()
        typer.echo(f"Default driver ready: {driver_cls.__name__}")
    except Exception as exc:
        typer.echo(f"Default driver {spec} setup failed: {exc}", err=True)
        return

    try:
        while not stop.wait(timeout=interval):
            try:
                known_version = _check_graph_change(driver, aq, known_version)
                driver.loop()
            except Exception as exc:
                typer.echo(f"Default driver {spec} loop error: {exc}", err=True)
    finally:
        try:
            driver.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# server subcommand
# ---------------------------------------------------------------------------

@app.command("server")
def server_cmd(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml")] = None,
    host: Annotated[Optional[str], typer.Option("--host", help="Bind host")] = None,
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Bind port")] = None,
    reload: Annotated[bool, typer.Option("--reload", help="Enable uvicorn auto-reload (development)")] = False,
) -> None:
    """Start the Acquirium FastAPI server, plus any [[drivers]] listed in the config."""
    import uvicorn

    cfg = _load_config(config)
    _apply_server_env(cfg)

    server_cfg = cfg.get("server", {})
    effective_host = host or server_cfg.get("host", "0.0.0.0")
    effective_port = port or server_cfg.get("port", 8000)

    driver_entries: list[dict] = cfg.get("drivers", [])
    stop_event = threading.Event()

    def _sigterm_handler(signum, frame):  # noqa: ANN001
        stop_event.set()
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    for entry in driver_entries:
        spec = entry.get("spec")
        if not spec:
            typer.echo("Warning: [[drivers]] entry missing 'spec', skipping.", err=True)
            continue
        t = threading.Thread(
            target=_run_default_driver,
            args=(entry, effective_host, effective_port, cfg, stop_event),
            daemon=True,
        )
        t.start()
        typer.echo(f"Starting default driver: {spec}")

    typer.echo(f"Starting Acquirium server on {effective_host}:{effective_port}")
    try:
        uvicorn.run(
            "acquirium.Server.app:app",
            host=effective_host,
            port=effective_port,
            reload=reload,
        )
    finally:
        stop_event.set()


# ---------------------------------------------------------------------------
# run subcommand
# ---------------------------------------------------------------------------

@app.command("run")
def run_cmd(
    driver_spec: Annotated[str, typer.Argument(help=(
        "Driver to run.  Examples: "
        "scripts/my_driver.py:MyDriver  |  "
        "my.module:MyDriver  |  "
        "scripts/my_driver.py  (auto-discover)"
    ))],
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml")] = None,
    interval: Annotated[Optional[float], typer.Option("--interval", "-i", help="Seconds between loop() calls")] = None,
) -> None:
    """Load and run a Driver subclass in a managed loop."""
    from acquirium.Client.acquirium import Acquirium

    cfg = _load_config(config)
    driver_cfg = cfg.get("driver", {})
    server_url, server_port, use_ssl, cfg_interval = _driver_connect_cfg(driver_cfg)
    effective_interval: float = interval if interval is not None else cfg_interval

    try:
        driver_cls = _import_driver_class(driver_spec)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    typer.echo(f"Loaded driver: {driver_cls.__name__} from {driver_spec}")

    aq = Acquirium(server_url=server_url, server_port=server_port, use_ssl=use_ssl)
    driver = driver_cls(aq, cfg)

    def _sigterm_handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    typer.echo("Running driver.setup()...")
    driver.setup()
    known_version = aq.graph_version()
    typer.echo(f"Setup complete. Starting loop (interval={effective_interval}s). Ctrl-C to stop.")

    try:
        while True:
            known_version = _check_graph_change(driver, aq, known_version)
            driver.loop()
            time.sleep(effective_interval)
    except KeyboardInterrupt:
        typer.echo("\nShutting down...")
        driver.stop()
        typer.echo("Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app()


if __name__ == "__main__":
    main()
