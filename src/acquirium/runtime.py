"""Start or attach to a local server, with lifetime owned by its first caller."""
from __future__ import annotations

import atexit
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import warnings

from filelock import FileLock, Timeout
import requests

from acquirium.Client.acquirium import Acquirium

_lock = threading.RLock()
_session = None


def _client(address: str, timeout: float | None) -> Acquirium:
    url = urlsplit(address)
    if (url.scheme not in {"http", "https"} or not url.hostname
            or url.username or url.password or url.path not in {"", "/"}
            or url.query or url.fragment):
        raise ValueError("address must be an http(s) server URL without credentials or a path")
    return Acquirium(server_url=url.hostname,
                     server_port=url.port or (443 if url.scheme == "https" else 80),
                     use_ssl=url.scheme == "https", health_timeout=timeout)


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        warnings.warn("Local Acquirium server did not shut down within 30 seconds and was killed.", RuntimeWarning)


def _running(directory: Path) -> bool:
    """The OS releases the server's file lock even after an unclean exit."""
    try:
        with FileLock(directory / "server.lock", timeout=0):
            return False
    except Timeout:
        return True


def _discover(directory: Path) -> dict | None:
    try:
        info = json.loads((directory / "server.json").read_text())
        # Only contact loopback; never treat arbitrary registry contents as a URL.
        port = int(info["port"])
        if not 0 < port < 65536:
            return None
        with requests.Session() as http:
            http.trust_env = False
            response = http.get(f"http://127.0.0.1:{port}/_local_runtime", timeout=0.5)
            response.raise_for_status()
            if response.json() == info:
                return info
    except (OSError, ValueError, KeyError, TypeError, requests.RequestException):
        pass
    return None


def _forget_stale_runtime(directory: Path) -> None:
    """Remove discovery data only after confirming no server holds its lock."""
    if not _running(directory):
        (directory / "server.json").unlink(missing_ok=True)


def _configuration(config, data_dir):
    from acquirium.cli import _load_config

    if config is not None and data_dir is not None:
        raise ValueError("set data_dir in the config file when supplying config")
    path = Path(config).expanduser().resolve() if config is not None else None
    if path is None and data_dir is None and Path("acquirium.toml").is_file():
        path = Path("acquirium.toml").resolve()
    cfg = _load_config(path) if path else {}
    server = cfg.get("server", {})
    if server.get("recreate", False):
        raise ValueError("aq.init() requires recreate=false to preserve existing data")
    if not server.get("enabled", True):
        raise ValueError("aq.init() requires server.enabled=true; use address= to connect remotely")
    if server.get("workers", 1) != 1:
        raise ValueError("aq.init() requires one server worker")
    base = path.parent if path else Path.cwd()
    root = (base / Path(data_dir if data_dir is not None else server.get("data_dir", ".acquirium")).expanduser()).resolve()
    return path, root, cfg


def init(config: str | Path | None = None, *, data_dir: str | Path | None = None, address: str | None = None,
         exact_only: bool | None = None, timeout: float = 600.0) -> Acquirium:
    """Return a client, starting a local server when necessary.

    Scripts using the same resolved data directory share a server. Its first
    caller owns it: shutdown (or normal interpreter exit) stops that server,
    even if other scripts are attached. Attachments only close their client.
    An explicit address connects to an independently managed server.
    With no arguments, load ./acquirium.toml if present. An explicit data_dir
    selects local defaults instead; config paths resolve relative to their file.
    """
    global _session
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if address is not None and (config is not None or data_dir is not None or exact_only is not None):
        raise ValueError("address cannot be combined with local server options")
    config_path, root, cfg = _configuration(config, data_dir) if address is None else (None, None, {})
    if exact_only is None:
        exact_only = cfg.get("server", {}).get("exact_only")
    fingerprint = sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest() if config_path else None
    key = address if address is not None else (str(root), fingerprint)
    with _lock:
        # A forked child must never terminate its parent's server.
        if _session is not None and _session[0] != os.getpid():
            _session = None
        if _session is not None:
            _, previous, client, _, mode = _session
            if previous != key or (exact_only is not None and exact_only != mode):
                raise ValueError("Acquirium is already initialized with different options; call shutdown() first")
            return client
        if address is not None:
            client = _client(address, timeout)
            _session = (os.getpid(), key, client, None, None)
            return client

        directory = root / ".runtime"
        directory.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + timeout
        process = None
        # Serialize discovery/startup across scripts, including the readiness wait.
        try:
            with FileLock(directory / "startup.lock", timeout=timeout):
                if not _running(directory):
                    _forget_stale_runtime(directory)
                    env = os.environ.copy()
                    # Config and explicit options determine storage. Inherited
                    # server paths/recreate flags must not redirect this runtime.
                    for name in list(env):
                        if name.startswith("ACQUIRIUM_"):
                            del env[name]
                    env["ACQUIRIUM_DATA_DIR"] = str(root)
                    env["ACQUIRIUM_EXACT_ONLY"] = str(bool(exact_only)).lower()
                    env["ACQUIRIUM_TIMESERIES_BACKEND"] = "duckdb"
                    command = [sys.executable, "-m", "acquirium._local_server", str(root)]
                    if config_path:
                        command.append(str(config_path))
                        # Let the shared config loader choose the backend.
                        del env["ACQUIRIUM_TIMESERIES_BACKEND"]
                    with (directory / "server.log").open("w") as log:
                        process = subprocess.Popen(
                            command,
                            env=env, stdin=subprocess.DEVNULL, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True,
                        )
                while time.monotonic() < deadline:
                    if process is not None and process.poll() is not None:
                        raise RuntimeError(f"Local Acquirium server exited; see {directory / 'server.log'}")
                    info = _discover(directory)
                    if info is not None and _running(directory):
                        if fingerprint is not None and fingerprint != info["config"]:
                            raise ValueError("The local server is running with a different config; stop its owner before changing configuration")
                        if exact_only is not None and exact_only != info["exact_only"]:
                            raise ValueError("The local server already uses a different exact_only setting")
                        client = _client(f"http://127.0.0.1:{info['port']}", None)
                        _session = (os.getpid(), key, client, process, info["exact_only"])
                        return client
                    time.sleep(0.1)
                raise TimeoutError(f"Local Acquirium server did not become ready; see {directory / 'server.log'}")
        except BaseException:
            if process is not None:
                _stop(process)
            raise


def shutdown() -> None:
    """Disconnect and stop only a server started by this process. Keep its data."""
    global _session
    with _lock:
        session, _session = _session, None
        if session is None or session[0] != os.getpid():
            return
        _, key, client, process, _ = session
        try:
            client.client._http.close()
        finally:
            if process is not None:
                _stop(process)
                # Uvicorn normally removes this itself. Do it here as well:
                # process termination can bypass the server entry point's finally.
                Path(key[0], ".runtime", "server.json").unlink(missing_ok=True)


atexit.register(shutdown)
