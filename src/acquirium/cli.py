from __future__ import annotations

"""acquirium CLI

Subcommands:

  acquirium server [--config FILE] [--host HOST] [--port PORT] [--reload]
      Start the Acquirium FastAPI server. [[drivers]] listed in the config
      are started as Ray actors that connect back over the API.

      Set ``[server] enabled = false`` in the config to submit the
      [[drivers]] to the remote Acquirium instance declared in the
      ``[driver]`` section (server_url / server_port) instead.

  acquirium driver start CONFIG    Submit the config's [[drivers]] to a server.
  acquirium driver list            List drivers running on a server.
  acquirium driver stop --name X   Stop a running driver.
"""

import importlib
import importlib.util
import inspect
import json
import os
import signal
import sys
import tomllib
from pathlib import Path
from typing import Annotated, Optional

import typer

app = typer.Typer(
    name="acquirium",
    help="Acquirium CLI — run the server and manage drivers.",
    add_completion=False,
)


@app.callback()
def _root() -> None:
    """Keep Typer in multi-command mode so `acquirium server ...` works
    even when `server` is the only registered subcommand."""

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

    # `[ontologies] sources` is read directly from acquirium.toml by
    # Manager — see acquirium.Server.config.load_ontology_config.


# ---------------------------------------------------------------------------
# Driver import helpers
# ---------------------------------------------------------------------------

def _import_driver_class(
    driver_spec: str, *, base_dir: Path | None = None
) -> tuple[type, str | None]:
    """Resolve a ``path/to/file.py:ClassName`` or ``my.module:ClassName`` spec to a Driver subclass.

    Returns the class and, for a file spec, the directory added to ``sys.path``
    so the file's sibling modules resolve. Callers that ship the class to
    another process must put that directory on the target's ``PYTHONPATH``:
    siblings imported by name pickle by reference, so the receiving process has
    to be able to import them itself. Module specs return ``None`` — already
    importable anywhere.

    Raises ``ValueError`` on any resolution failure so callers in background
    threads see a real exception rather than a silent ``SystemExit``.
    """
    from acquirium.Drivers.Driver import Driver as _Driver

    if ":" not in driver_spec:
        raise ValueError(
            f"driver spec must include a class name (e.g. my_driver.py:MyDriver), got {driver_spec!r}"
        )

    path_part, class_name = driver_spec.rsplit(":", 1)

    source_dir: str | None = None
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
        source_dir = str(file_path.parent)
        if source_dir not in sys.path:
            sys.path.insert(0, source_dir)
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
    return cls, source_dir


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


def _sigterm_as_keyboard_interrupt(*_) -> None:
    raise KeyboardInterrupt


# ---------------------------------------------------------------------------
# Remote driver management (drivers run as Ray actors on the server)
# ---------------------------------------------------------------------------

def _server_base_url(
    cfg: dict,
    server_url: Optional[str] = None,
    server_port: Optional[int] = None,
) -> str:
    """Build the server base URL from CLI options, falling back to [driver] config."""
    host, port, use_ssl, _ = _driver_connect_cfg(cfg.get("driver", {}))
    host = server_url or host
    port = server_port or port
    return f"{'https' if use_ssl else 'http'}://{host}:{port}"


def _push_drivers_to_server(
    cfg: dict,
    server_url: Optional[str] = None,
    server_port: Optional[int] = None,
) -> None:
    """Submit every [[drivers]] entry in cfg to the server's /drivers/start.

    The server imports the driver spec and runs it as a Ray actor, so file
    based specs must resolve on the server host.
    """
    import requests

    base = _server_base_url(cfg, server_url, server_port)
    entries = cfg.get("drivers", [])
    if not entries:
        typer.echo("No [[drivers]] entries in config; nothing to start.", err=True)
        raise typer.Exit(1)

    driver_cfg = cfg.get("driver", {})
    failures = 0
    for entry in entries:
        spec = entry.get("spec")
        if not spec:
            typer.echo("Warning: [[drivers]] entry missing 'spec'; skipping", err=True)
            continue
        overrides = {k: v for k, v in entry.items() if k not in ("spec", "name")}
        merged = {**cfg, "driver": {**driver_cfg, **overrides}}
        interval = float(overrides.get("interval", driver_cfg.get("interval", 10.0)))
        payload = {
            "spec": spec,
            "name": entry.get("name"),
            "interval": interval,
            "config": merged,
        }
        try:
            # Driver setup runs inside this request (WaterTAP builds can be
            # slow), hence the long timeout. default=str keeps TOML dates
            # JSON-serializable.
            resp = requests.post(
                f"{base}/drivers/start",
                data=json.dumps(payload, default=str),
                headers={"Content-Type": "application/json"},
                timeout=600,
            )
        except requests.RequestException as exc:
            typer.echo(f"Driver {spec} failed: could not reach server at {base}: {exc}", err=True)
            failures += 1
            continue
        if resp.ok:
            info = resp.json().get("driver", {})
            typer.echo(f"Started driver '{info.get('name')}' ({spec}, interval={info.get('interval')}s)")
        else:
            detail = resp.json().get("detail", resp.text) if resp.text else resp.reason
            typer.echo(f"Driver {spec} failed to start: {detail}", err=True)
            failures += 1

    if failures:
        raise typer.Exit(1)


driver_app = typer.Typer(help="Manage drivers running on an Acquirium server.", add_completion=False)
app.add_typer(driver_app, name="driver")

_ServerUrlOpt = Annotated[Optional[str], typer.Option("--server-url", help="Server host (default: [driver] server_url or localhost)")]
_ServerPortOpt = Annotated[Optional[int], typer.Option("--server-port", help="Server port (default: [driver] server_port or 8000)")]


@driver_app.command("start")
def driver_start(
    config: Annotated[Path, typer.Argument(help="Path to acquirium.toml with [[drivers]] entries")],
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """Start the drivers declared in the config on the server and exit."""
    cfg = _load_config(config)
    _push_drivers_to_server(cfg, server_url, server_port)


@driver_app.command("list")
def driver_list(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """List drivers running on the server."""
    import requests

    cfg = _load_config(config)
    base = _server_base_url(cfg, server_url, server_port)
    try:
        resp = requests.get(f"{base}/drivers/list", timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        typer.echo(f"Could not list drivers at {base}: {exc}", err=True)
        raise typer.Exit(1)

    drivers = resp.json().get("drivers", [])
    if not drivers:
        typer.echo("No drivers running.")
        return
    name_w = max(len(d["name"]) for d in drivers)
    for d in drivers:
        typer.echo(
            f"{d['name']:<{name_w}}  {d['status']:<8}  interval={d['interval']}s  "
            f"started={d['started_at']}  spec={d['spec']}"
        )


@driver_app.command("stop")
def driver_stop(
    name: Annotated[str, typer.Option("--name", help="Driver name as shown by 'acquirium driver list'")],
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """Stop a running driver by name."""
    import requests

    cfg = _load_config(config)
    base = _server_base_url(cfg, server_url, server_port)
    try:
        resp = requests.post(f"{base}/drivers/stop", json={"name": name}, timeout=60)
    except requests.RequestException as exc:
        typer.echo(f"Could not reach server at {base}: {exc}", err=True)
        raise typer.Exit(1)
    if resp.ok:
        typer.echo(f"Stopped driver '{name}'")
    else:
        detail = resp.json().get("detail", resp.text) if resp.text else resp.reason
        typer.echo(f"Failed to stop driver '{name}': {detail}", err=True)
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# server subcommand
# ---------------------------------------------------------------------------

@app.command("server")
def server_cmd(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml")] = None,
    host: Annotated[Optional[str], typer.Option("--host", help="Bind host")] = None,
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Bind port")] = None,
    reload: Annotated[bool, typer.Option("--reload", help="Enable uvicorn auto-reload (development)")] = False,
    workers: Annotated[Optional[int], typer.Option("--workers", "-w", help="Uvicorn worker processes; must be 1 — the embedded Oxigraph graph store is single-process on every backend")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable DEBUG logs in acquirium.* (server, storage, drivers)")] = False,
) -> None:
    """Start the server and configured drivers.

    Set ``[server] enabled = false`` in the config to skip the HTTP server
    and submit the [[drivers]] to the remote server from the [driver]
    section instead (same as ``acquirium driver start``).
    """
    if verbose:
        os.environ["ACQUIRIUM_VERBOSE"] = "1"

    from acquirium.internals._log import configure_logging
    configure_logging(verbose=verbose or os.environ.get("ACQUIRIUM_VERBOSE") == "1")

    cfg = _load_config(config)
    _apply_server_env(cfg)

    server_cfg = cfg.get("server", {})
    if not server_cfg.get("enabled", True):
        _push_drivers_to_server(cfg)
        return

    import uvicorn

    # Propagate the config path so the lifespan can start [[drivers]] as Ray actors.
    if config:
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(config.resolve()))
    elif Path("acquirium.toml").exists():
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(Path("acquirium.toml").resolve()))

    effective_host = host or server_cfg.get("host", "0.0.0.0")
    effective_port = port or server_cfg.get("port", 8000)
    # Driver actors connect back to this server over HTTP; the lifespan reads
    # this to know its own port when [driver] server_port is not set.
    os.environ["ACQUIRIUM_SELF_PORT"] = str(effective_port)
    effective_workers = (
        workers
        or int(os.environ.get("ACQUIRIUM_WORKERS", 0))
        or server_cfg.get("workers", 1)
    )
    if reload:
        effective_workers = 1  # uvicorn forbids workers > 1 with reload

    if effective_workers > 1:
        # Each worker process builds its own Manager, and every Manager opens the
        # embedded Oxigraph store at the same graph_path. RocksDB permits one
        # process per store, so N workers means N writers over the same files.
        # This holds on the timescale backend too: it moves the timeseries store
        # out of process, but the graph store stays embedded.
        typer.echo(
            f"Refusing to start with workers={effective_workers}: the embedded Oxigraph "
            "graph store supports a single process, whatever the timeseries backend. "
            "Start with workers=1.",
            err=True,
        )
        raise typer.Exit(1)

    signal.signal(signal.SIGTERM, _sigterm_as_keyboard_interrupt)

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
