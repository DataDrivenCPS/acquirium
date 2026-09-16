from __future__ import annotations

"""acquirium CLI

Subcommands:

  acquirium server [--config FILE] [--host HOST] [--port PORT] [--reload]
      Start the Acquirium FastAPI server. [[drivers]] listed in the config
      are started as Ray actors that connect back over the API, followed by
      deployments from [[apps]].

      Set ``[server] enabled = false`` in the config to submit the
      [[drivers]] to the remote Acquirium instance declared in the
      ``[driver]`` section (server_url / server_port) instead.

  acquirium driver start CONFIG    Submit the config's [[drivers]] to a server.
  acquirium driver list            List drivers running on a server.
  acquirium driver stop --name X   Stop a running driver.
  acquirium app check MODULE:CLASS Dry-run an app locally or on a server.
  acquirium app list               List deployed apps and binding status.
  acquirium app inspect NAME       Inspect schemas, settings, streams, and progress.
"""

import importlib
import importlib.util
import inspect
import json
import os
import signal
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any, Iterator, Optional

import typer

from acquirium.Server.config import (
    DEFAULT_LOCAL_CONFIG,
    load_config,
    load_local_config,
)

app = typer.Typer(
    name="acquirium",
    help="Acquirium CLI — run the server and manage drivers and apps.",
    add_completion=False,
)


@app.callback()
def _root() -> None:
    """Keep Typer in multi-command mode so `acquirium server ...` works
    even when `server` is the only registered subcommand."""

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
    fallback_host: str = "127.0.0.1",
    fallback_port: int = 8000,
) -> tuple[str, int, bool, float]:
    """Return (host, port, use_ssl, interval) from a [driver] config dict."""
    host = driver_cfg.get("server_url", fallback_host)
    # 0.0.0.0 is a bind address, not a connectable host. 127.0.0.1 rather than
    # "localhost": see AcquiriumClient (issue #85).
    if host == "0.0.0.0":
        host = "127.0.0.1"
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

_ServerUrlOpt = Annotated[Optional[str], typer.Option("--server-url", help="Server host (default: driver.server_url or 127.0.0.1)")]
_ServerPortOpt = Annotated[Optional[int], typer.Option("--server-port", help="Server port (default: driver.server_port or 8000)")]


@driver_app.command("start")
def driver_start(
    config: Annotated[Path, typer.Argument(help="Path to acquirium.toml with configured drivers")],
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """Start the drivers declared in the config on the server and exit."""
    cfg = load_config(config).data
    _push_drivers_to_server(cfg, server_url, server_port)


@driver_app.command("list")
def driver_list(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """List drivers running on the server."""
    import requests

    cfg = load_config(config).data
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

    cfg = load_config(config).data
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
# app subcommand
# ---------------------------------------------------------------------------

app_app = typer.Typer(help="Check and manage apps on an Acquirium server.", add_completion=False)
app.add_typer(app_app, name="app")


def _read_apps(base: str, name: str | None = None) -> dict:
    """Fetch deployment inspection data with bounded waits and CLI errors."""
    import requests
    from urllib.parse import quote

    path = "/apps" if name is None else f"/apps/{quote(name, safe='')}"
    try:
        response = requests.get(f"{base}{path}", timeout=30)
        if response.status_code == 404 and name is not None:
            typer.echo(f"Unknown app {name!r} at {base}", err=True)
            raise typer.Exit(1)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as error:
        typer.echo(f"Could not inspect apps at {base}: {error}", err=True)
        raise typer.Exit(1)


@app_app.command("list")
def app_list(
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw result document")] = False,
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """List deployed apps, including apps with no matches or planning errors."""
    result = _read_apps(_server_base_url(_load_config(config), server_url, server_port))
    if as_json:
        typer.echo(json.dumps(result, indent=2))
        return
    if not result["apps"]:
        typer.echo("No apps deployed.")
    for item in result["apps"]:
        typer.echo(f"{item['name']}  {item['status']}  grouping={item['grouping']}  bindings={item['binding_count']}")
        if item["error"]:
            typer.echo(f"  Error: {item['error']}")


@app_app.command("inspect")
def app_inspect(
    name: Annotated[str, typer.Argument(help="Deployed app name, as shown by 'app list'")],
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw result document")] = False,
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """Show declared output schemas, settings, resolved streams, and progress."""
    result = _read_apps(_server_base_url(_load_config(config), server_url, server_port), name)
    if as_json:
        typer.echo(json.dumps(result, indent=2))
        return
    item = result["app"]
    definition = item["definition"]
    typer.echo(f"{item['name']}: {item['status']}")
    typer.echo(f"Entrypoint: {item['entrypoint']}")
    typer.echo(f"Grouping: {item['grouping']}")
    typer.echo(f"Plan: graph revision {item['graph_revision']}, current={item['plan_current']}")
    for key in ("every", "lookback", "lookahead", "batch_delay", "min_interval"):
        value = definition[key]
        rendered = f"{value / 1_000_000:g}s" if isinstance(value, int) else value or "disabled"
        typer.echo(f"{key}: {rendered}")
    typer.echo(f"backfill: {definition['backfill']}")
    typer.echo(f"parameters: {json.dumps(definition['parameters'], sort_keys=True)}")
    if item["error"]:
        typer.echo(f"Error: {item['error']}")
    typer.echo("Declared outputs:")
    for port, spec in definition["outputs"].items():
        typer.echo(f"  {port}: {spec['value_kind']}, unit={spec['unit'] or '(unspecified)'}")
        schema = ", ".join(f"{column}: {field['type']} (non-null)"
                           for column, field in item["output_schemas"][port].items())
        typer.echo(f"    schema: {schema}")
        typer.echo(f"    metadata: {json.dumps(spec, sort_keys=True)}")
    typer.echo(f"Bindings: {item['binding_count']}")
    for binding in item["bindings"]:
        typer.echo(f"  {binding['binding_signature']}: {binding['status']}")
        typer.echo(f"    inputs: {json.dumps(binding['inputs'], sort_keys=True)}")
        typer.echo(f"    outputs: {json.dumps(binding['outputs'], sort_keys=True)}")
        typer.echo(f"    revision: {binding['consumed_revision']} / {binding['current_revision']}; last success: {binding['last_success']}")
        if binding["error"]:
            typer.echo(f"    Error: {binding['error']}")


def _load_app_target(spec: str) -> tuple[object, str | None]:
    """Load ``module:Class`` or ``./file.py:Class``, with its directory.

    The directory is returned so the caller can tell the server where the
    file lives: the server imports by module name, and a path like
    ``../app.py`` means nothing once only the name is sent.
    """
    if ":" not in spec:
        raise ValueError("app spec must be module_or_file:ClassName")
    module_part, target_name = spec.rsplit(":", 1)
    path = Path(module_part)
    source_dir: str | None = None
    if "/" in module_part or path.suffix == ".py" or path.exists():
        path = path.resolve()
        if not path.is_file():
            raise ValueError(f"app file not found: {module_part}")
        if not path.stem.isidentifier():
            raise ValueError(f"app file name must be a Python module name: {path.name}")
        source_dir = str(path.parent)
        if source_dir not in sys.path:
            sys.path.insert(0, source_dir)
        module = importlib.import_module(path.stem)
    else:
        module = importlib.import_module(module_part)
    try:
        return getattr(module, target_name), source_dir
    except AttributeError:
        raise ValueError(f"app {target_name!r} was not found in {module.__name__!r}") from None


@app_app.command("check")
def app_check(
    spec: Annotated[str, typer.Argument(help="App class as module:ClassName or ./file.py:ClassName")],
    params: Annotated[Optional[str], typer.Option("--params", help='Constructor parameters as JSON, e.g. \'{"threshold": 3}\'')] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Show only the first N rows of each output; 0 shows every row")] = 5,
    local: Annotated[bool, typer.Option("--local", help="Run the app in this process, so breakpoint() and tracebacks land in this terminal")] = False,
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw result document")] = False,
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml (for server address)")] = None,
    server_url: _ServerUrlOpt = None,
    server_port: _ServerPortOpt = None,
) -> None:
    """Run an app against stored data and print what it computed, saving nothing.

    The app is not deployed, its derived streams are not created, and no
    progress is recorded. The server imports the class by module path, so it
    must be importable there as it is here.

    Each output prints its first 5 computed rows; pass ``-n 0`` for every row.

    With ``--local`` the app runs here instead of on the server, reading its
    inputs over the API. Use it to debug: ``breakpoint()`` opens a console in
    this terminal, and a failing transform raises a traceback here rather
    than being reported as a per-binding error.
    """
    import requests

    if limit < 0:
        typer.echo("--limit must not be negative", err=True)
        raise typer.Exit(1)
    cfg = load_config(config).data
    base = _server_base_url(cfg, server_url, server_port)
    try:
        target, source_dir = _load_app_target(spec)
        parameters = json.loads(params) if params else {}
    except (ValueError, json.JSONDecodeError) as exc:
        typer.echo(f"{exc}", err=True)
        raise typer.Exit(1)

    from acquirium.Materialization.planner import Deployment

    if local:
        # The app runs here, so the server never imports it and an exception
        # from transform() is left to reach the terminal with its traceback.
        from acquirium.Client.acquirium import Acquirium
        from acquirium.Materialization import local as local_check

        host, port, use_ssl, _ = _driver_connect_cfg(cfg.get("driver", {}))
        client = Acquirium(server_url=server_url or host, server_port=server_port or port,
                           use_ssl=use_ssl)
        result = local_check.check_app(client, target, parameters=parameters,
                                       limit=None if limit == 0 else limit)
        _render_check(result, limit, as_json=as_json)
        raise typer.Exit(1 if _check_failures(result) else 0)

    try:
        definition = json.loads(Deployment.from_class(target, parameters=parameters).to_json())
    except Exception as exc:
        typer.echo(f"{type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(1)
    query: dict[str, Any] = {} if limit == 0 else {"limit": limit}
    if source_dir:
        # The server imports by module name; tell it where the file it was
        # named by actually lives, in case it is not otherwise importable.
        query["search_path"] = source_dir
    try:
        resp = requests.post(f"{base}/apps/check", json=definition, params=query, timeout=300)
    except requests.RequestException as exc:
        typer.echo(f"Could not reach server at {base}: {exc}", err=True)
        raise typer.Exit(1)
    if not resp.ok:
        detail = resp.json().get("detail", resp.text) if resp.text else resp.reason
        typer.echo(f"Check failed: {detail}", err=True)
        if "could not import" in str(detail) and source_dir:
            typer.echo(
                f"\nThe server could not read {source_dir}. That happens when it runs on\n"
                f"another machine or in a container: a check runs the app on the server, so\n"
                f"the file has to exist there. Copy it somewhere the server imports from\n"
                f"(its config directory), or run it here with --local.",
                err=True,
            )
        raise typer.Exit(1)

    _render_check(resp.json(), limit, as_json=as_json)
    if _check_failures(resp.json()):
        raise typer.Exit(1)


def _render_check(result: dict, limit: int, *, as_json: bool = False) -> None:
    """Print a check result; identical whether it ran here or on the server."""
    if as_json:
        typer.echo(json.dumps(result, indent=2))
        return
    typer.echo(f"{result['app']}: {len(result['bindings'])} input group(s) matched")
    for index, entry in enumerate(result["bindings"], start=1):
        typer.echo(f"\n[{index}] inputs")
        for alias, streams in entry["inputs"].items():
            rows = (entry.get("input_rows") or {}).get(alias)
            count = f", {rows} rows read" if rows is not None else ""
            typer.echo(f"      {alias}: {len(streams)} stream(s){count}")
            shown = streams if limit == 0 else streams[:limit]
            for item in shown:
                typer.echo(f"        - {item.get('label') or item['ref_uri']}")
            if len(shown) < len(streams):
                typer.echo(f"        … {len(streams) - len(shown)} more stream(s)")
        # Entity columns say what this call's row is about; the stream
        # columns are already listed above.
        row = entry.get("row") or {}
        entities = {k: v for k, v in row.items()
                    if v and "." not in k and not k.endswith("_ref") and f"{k}_ref" not in row}
        if entities:
            typer.echo("      " + "  ".join(f"[{k}] {v}" for k, v in entities.items()))
        if entry.get("error"):
            typer.echo(f"    error: {entry['error']}", err=True)
            continue
        for port, out in entry["outputs"].items():
            typer.echo(f"    output {port!r} -> {out['ref_name']} ({out['value_kind']}, {out['rows']} rows)")
            for row in out["values"]:
                typer.echo(f"        {row['time']}  {row['value']}")
            if out.get("truncated"):
                omitted = out["rows"] - len(out["values"])
                typer.echo(f"        … {omitted} more row(s); pass -n 0 for all of them")


def _check_failures(result: dict) -> bool:
    return any(entry.get("error") for entry in result.get("bindings", []))


# ---------------------------------------------------------------------------
# server subcommand
# ---------------------------------------------------------------------------

def _run_uvicorn(
    application: object,
    *,
    host: str,
    port: int,
    reload: bool,
    workers: int,
    sockets: list[object] | None = None,
) -> None:
    """Run uvicorn for both the public CLI and aq.init()'s private mode."""
    import uvicorn

    signal.signal(signal.SIGTERM, _sigterm_as_keyboard_interrupt)
    typer.echo(f"Starting Acquirium server on {host}:{port}")
    if sockets is None:
        uvicorn.run(application, host=host, port=port, reload=reload, workers=workers)
        return

    server = uvicorn.Server(
        uvicorn.Config(application, host=host, port=port, reload=reload, workers=workers)
    )
    server.run(sockets=sockets)


@contextmanager
def _local_runtime_setup(
    *,
    directory: Path,
    host: str,
    port: int,
    config_fingerprint: str | None,
) -> Iterator[tuple[object, int, list[object]]]:
    """Prepare aq.init() discovery around the shared uvicorn runner."""
    import socket
    from uuid import uuid4

    from filelock import FileLock

    with FileLock(directory / "server.lock", timeout=0):
        with socket.socket() as listener:
            listener.bind((host, port))
            actual_port = listener.getsockname()[1]
            os.environ["ACQUIRIUM_SELF_HOST"] = host
            os.environ["ACQUIRIUM_SELF_PORT"] = str(actual_port)

            from acquirium.Server.app import app as application

            info = {
                "port": actual_port,
                "token": uuid4().hex,
                "config": config_fingerprint,
                "exact_only": os.environ.get("ACQUIRIUM_EXACT_ONLY") == "true",
            }

            @application.get("/_local_runtime", include_in_schema=False)
            def local_runtime():
                return info

            registry = directory / "server.json"
            temporary = directory / "server.json.tmp"
            temporary.write_text(json.dumps(info))
            temporary.replace(registry)
            try:
                yield application, actual_port, [listener]
            finally:
                registry.unlink(missing_ok=True)


@app.command("server")
def server_cmd(
    config: Annotated[Optional[Path], typer.Option("--config", "-c", help="Path to acquirium.toml")] = None,
    host: Annotated[Optional[str], typer.Option("--host", help="Bind host")] = None,
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Bind port")] = None,
    reload: Annotated[bool, typer.Option("--reload", help="Enable uvicorn auto-reload (development)")] = False,
    workers: Annotated[Optional[int], typer.Option("--workers", "-w", help="Uvicorn worker processes; must be 1 — the embedded Oxigraph graph store is single-process on every backend")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable DEBUG logs in acquirium.* (server, storage, drivers)")] = False,
    runtime_directory: Annotated[Optional[Path], typer.Option("--runtime-directory", hidden=True)] = None,
) -> None:
    """Start the server, configured drivers, and configured apps.

    Set ``server.enabled = false`` in the config to skip the HTTP server and
    submit its configured drivers to the remote address in the driver section
    instead (the same behavior as ``acquirium driver start``).
    """
    if verbose:
        os.environ["ACQUIRIUM_VERBOSE"] = "1"

    from acquirium.internals._log import configure_logging
    configure_logging(verbose=verbose or os.environ.get("ACQUIRIUM_VERBOSE") == "1")

    loaded_config = (
        load_local_config(config, discover=False)
        if runtime_directory is not None
        else load_config(config)
    )
    cfg = loaded_config.data
    loaded_config.apply_server_env()

    server_cfg = cfg.get("server", {})
    if not server_cfg.get("enabled", True):
        _push_drivers_to_server(cfg)
        return

    # The lifespan, ontology loader, and reload child all consume this exact
    # selection. An empty value deliberately means no config.
    loaded_config.activate()

    if runtime_directory is None:
        effective_host = host if host is not None else server_cfg.get("host", "0.0.0.0")
        effective_port = port if port is not None else server_cfg.get("port", 8000)
    else:
        # aq.init() is always loopback-only and defaults to an ephemeral port.
        effective_host = str(DEFAULT_LOCAL_CONFIG["server"]["host"])
        effective_port = port if port is not None else server_cfg["port"]
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

    if runtime_directory is not None:
        directory = runtime_directory.resolve()
        with _local_runtime_setup(
            directory=directory,
            host=effective_host,
            port=int(effective_port),
            config_fingerprint=loaded_config.fingerprint,
        ) as (application, actual_port, sockets):
            _run_uvicorn(
                application,
                host=effective_host,
                port=actual_port,
                reload=False,
                workers=1,
                sockets=sockets,
            )
        return

    _run_uvicorn(
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
