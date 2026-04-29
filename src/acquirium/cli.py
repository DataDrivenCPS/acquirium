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
    "workers":                "ACQUIRIUM_WORKERS",
    "read_batch_size":        "ACQUIRIUM_READ_BATCH_SIZE",
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


def _check_graph_change(driver: "Driver", aq: "Acquirium", known_version: int) -> int:
    """Call driver.on_graph_change() if graph version has advanced. Returns updated known version."""
    try:
        v = aq.graph_version()
    except Exception as exc:
        typer.echo(f"Warning: graph_version() failed: {exc}", err=True)
        return known_version
    if v != known_version:
        try:
            driver.on_graph_change()
        except Exception as exc:
            typer.echo(f"Driver on_graph_change() error: {exc}", err=True)
        return v
    return known_version



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
    """Start the Acquirium FastAPI server, plus any [[drivers]] listed in the config."""
    import uvicorn

    cfg = _load_config(config)
    _apply_server_env(cfg)

    # Propagate the config path so the lifespan can start [[drivers]] in-process.
    if config:
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(config.resolve()))
    elif Path("acquirium.toml").exists():
        os.environ.setdefault("ACQUIRIUM_CONFIG", str(Path("acquirium.toml").resolve()))

    server_cfg = cfg.get("server", {})
    effective_host = host or server_cfg.get("host", "0.0.0.0")
    effective_port = port or server_cfg.get("port", 8000)
    effective_workers = (
        workers
        or int(os.environ.get("ACQUIRIUM_WORKERS", 0))
        or server_cfg.get("workers", 1)
    )
    if reload:
        effective_workers = 1  # uvicorn forbids workers > 1 with reload

    def _sigterm_handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    typer.echo(f"Starting Acquirium server on {effective_host}:{effective_port}")
    uvicorn.run(
        "acquirium.Server.app:app",
        host=effective_host,
        port=effective_port,
        reload=reload,
        workers=effective_workers,
    )


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
        driver_cls = _import_driver_class(driver_spec, base_dir=Path(cfg.get("__config_dir", Path.cwd())))
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
    typer.echo(f"Loaded driver: {driver_cls.__name__} from {driver_spec}")

    aq = Acquirium(
        server_url=server_url,
        server_port=server_port,
        use_ssl=use_ssl,
        insert_batch_rows=int(driver_cfg.get("insert_batch_rows", 50_000)),
    )
    driver = driver_cls(aq, cfg)

    def _sigterm_handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    typer.echo("Running driver.setup()...")
    driver.setup()
    try:
        known_version = aq.graph_version()
    except Exception:
        known_version = 0
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
