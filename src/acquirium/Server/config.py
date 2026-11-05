"""Load Acquirium configuration and resolve its server-side resources.

The CLI sets ``ACQUIRIUM_CONFIG`` to the absolute path of the toml file
when it starts uvicorn. An empty value explicitly means no config, which
lets ``aq.init(data_dir=...)`` ignore an unrelated file in its working
directory without manufacturing an empty config file.

Config shape::

    [ontologies]
    sources = [
        # Plain string: load as-is; the file's declared owl:Ontology IRI
        # becomes the IRI in ontoenv.
        "./local-extensions.ttl",
        # Table form: load the source and rewrite its declared ontology
        # IRI to ``as`` before adding (always with overwrite=True, so it
        # replaces any pre-existing graph at that IRI).
        { source = "https://qudt.org/3.3.0/vocab/unit",
          as = "https://qudt.org/vocab/unit" },
    ]

Relative file paths are resolved against the directory containing the
toml file. URLs and ``urn:`` IRIs are passed through unchanged.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
from collections.abc import MutableMapping
from hashlib import sha256
import json
import os
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_logger = logging.getLogger("acquirium.config")


def load_app_target(spec: str, *, base_dir: Path | None = None) -> object:
    """Load an App or factory from a module/file spec without shadowing modules."""
    if ":" not in spec:
        raise ValueError("app spec must be module_or_file:target")
    module_part, target_name = spec.rsplit(":", 1)
    file_path = Path(module_part)
    is_file = "/" in module_part or file_path.suffix == ".py" or file_path.exists()
    if is_file:
        # Server entries resolve against their config directory; CLI file
        # specs use the caller's working directory when no base is supplied.
        if not file_path.is_absolute():
            file_path = ((base_dir or Path.cwd()) / file_path).resolve()
        if not file_path.is_file():
            raise ValueError(f"app file not found: {module_part}")
        module_name = file_path.stem
        if not module_name.isidentifier():
            raise ValueError(f"app file name must be a Python module name: {file_path.name}")
        # A file stem must identify this file, without shadowing installed
        # modules or reusing a same-named file from another directory.
        importlib.invalidate_caches()
        try:
            found = importlib.util.find_spec(module_name)
        except (ImportError, ValueError):
            found = False
        origin = getattr(found, "origin", None)
        if found is not None and (origin is None or Path(origin).resolve() != file_path):
            raise ValueError(f"app file {file_path} would import as {module_name!r}, which is already "
                             f"{origin or 'another module'}; rename the file or install it as a package")
        module_dir = str(file_path.parent)
        if module_dir not in sys.path:
            # Appended, so app directories never shadow installed modules.
            sys.path.append(module_dir)
        module = importlib.import_module(module_name)
    else:
        module = importlib.import_module(module_part)
    try:
        return getattr(module, target_name)
    except AttributeError as error:
        raise ValueError(f"app target {target_name!r} was not found in {module.__name__!r}") from error


_SERVER_ENV_MAP: dict[str, str] = {
    "data_dir": "ACQUIRIUM_DATA_DIR",
    "engine_path": "ACQUIRIUM_ENGINE_PATH",
    "experiments_path": "ACQUIRIUM_EXPERIMENTS_PATH",
    "engine_workers": "ACQUIRIUM_ENGINE_WORKERS",
    "python_pool_size": "ACQUIRIUM_PYTHON_POOL_SIZE",
    "python_max_worker_jobs": "ACQUIRIUM_PYTHON_MAX_WORKER_JOBS",
    "python_max_worker_age": "ACQUIRIUM_PYTHON_MAX_WORKER_AGE",
    "epoch_requests": "ACQUIRIUM_EPOCH_REQUESTS",
    "max_pending_requests": "ACQUIRIUM_MAX_PENDING_REQUESTS",
    "drain_timeout": "ACQUIRIUM_DRAIN_TIMEOUT",
    "graph_path": "ACQUIRIUM_GRAPH_PATH",
    "embedding_model": "ACQUIRIUM_EMBEDDING_MODEL",
    "exact_only": "ACQUIRIUM_EXACT_ONLY",
    "recreate": "ACQUIRIUM_RECREATE",
    "workers": "ACQUIRIUM_WORKERS",
}


# The managed runtime is deliberately private, persistent, and dependency-free.
# Keep this profile separate from the public ``acquirium server`` defaults,
# which listen on a stable port and may bind beyond loopback.
DEFAULT_LOCAL_CONFIG: dict[str, Any] = {
    "server": {
        "enabled": True,
        "data_dir": ".acquirium",
        "host": "127.0.0.1",
        "port": 0,
        "workers": 1,
        "recreate": False,
        "exact_only": True,
    }
}


@dataclass(frozen=True)
class LoadedConfig:
    """One resolved config shared by the CLI, runtime, and server."""

    data: dict[str, Any]
    path: Path | None
    directory: Path
    fingerprint: str | None

    def activate(self) -> None:
        """Tell server processes exactly which config was selected."""
        os.environ["ACQUIRIUM_CONFIG"] = str(self.path) if self.path else ""

    def apply_server_env(
        self, environ: MutableMapping[str, str] | None = None
    ) -> None:
        """Apply ``[server]`` values without overriding explicit environment."""
        target = os.environ if environ is None else environ
        server = self.data.get("server", {})
        for key, env_var in _SERVER_ENV_MAP.items():
            if key not in server:
                continue
            value = server[key]
            # A PostgreSQL URL names a database, not a file.
            if key in {"data_dir", "engine_path", "experiments_path", "graph_path"} and not str(
                value
            ).startswith(("postgres://", "postgresql://")):
                path = Path(value)
                value = (
                    str((self.directory / path).resolve())
                    if not path.is_absolute()
                    else str(path)
                )
            if isinstance(value, list):
                encoded = ",".join(str(item) for item in value)
            elif isinstance(value, bool):
                encoded = "true" if value else "false"
            else:
                encoded = str(value)
            target.setdefault(env_var, encoded)


def load_config(path: Path | None = None, *, discover: bool = True) -> LoadedConfig:
    """Load one config, optionally discovering ``./acquirium.toml``."""
    selected = path
    if selected is None and discover:
        default = Path("acquirium.toml")
        selected = default if default.is_file() else None
    if selected is not None:
        selected = selected.expanduser().resolve()
        with selected.open("rb") as config_file:
            data = tomllib.load(config_file)
        directory = selected.parent
        # Existing driver payloads consume this compatibility field.
        data["__config_dir"] = str(directory)
        fingerprint = sha256(
            json.dumps(data, sort_keys=True, default=str).encode()
        ).hexdigest()
    else:
        data = {}
        directory = Path.cwd().resolve()
        fingerprint = None
    return LoadedConfig(data, selected, directory, fingerprint)


def load_local_config(
    path: Path | None = None, *, discover: bool = True
) -> LoadedConfig:
    """Load configuration over the managed runtime's explicit defaults."""
    loaded = load_config(path, discover=discover)
    data = dict(loaded.data)
    data["server"] = {
        **DEFAULT_LOCAL_CONFIG["server"],
        **loaded.data.get("server", {}),
    }
    return LoadedConfig(
        data=data,
        path=loaded.path,
        directory=loaded.directory,
        fingerprint=loaded.fingerprint,
    )


def load_active_config() -> LoadedConfig:
    """Load the config explicitly selected by the launcher.

    If the launcher has not selected one, retain the historical cwd lookup.
    Presence of an empty ``ACQUIRIUM_CONFIG`` suppresses that lookup.
    """
    if "ACQUIRIUM_CONFIG" not in os.environ:
        return load_config()
    value = os.environ["ACQUIRIUM_CONFIG"]
    return (
        load_config(Path(value), discover=False)
        if value
        else load_config(discover=False)
    )


@dataclass(frozen=True)
class OntologySource:
    """One entry from ``[ontologies] sources``.

    - ``source`` is a URL or absolute file path (relative paths get
      resolved against the toml file's directory at load time).
    - ``rename_to`` is the canonical IRI to rewrite the loaded graph's
      declared ``owl:Ontology`` to; ``None`` means "use the IRI declared
      in the source file."
    """
    source: str
    rename_to: str | None = None


@dataclass(frozen=True)
class OntologyConfig:
    sources: tuple[OntologySource, ...] = ()


def _resolve_source(src: str, base_dir: Path) -> str:
    """URLs and urn: stay as-is; relative file paths resolve against base_dir."""
    if "://" in src or src.startswith("urn:"):
        return src
    p = Path(src)
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return str(p)


def _parse_entry(entry: object, base_dir: Path) -> OntologySource | None:
    if isinstance(entry, str):
        return OntologySource(source=_resolve_source(entry, base_dir))
    if isinstance(entry, dict):
        d: dict = entry  # narrow `dict[Never, Never]` away for the type checker
        src = d.get("source")
        if not isinstance(src, str):
            _logger.warning("ontologies.sources entry missing 'source' string: %r", entry)
            return None
        rename = d.get("as")
        if rename is not None and not isinstance(rename, str):
            _logger.warning(
                "ontologies.sources entry has non-string 'as': %r (ignored)", entry
            )
            rename = None
        return OntologySource(
            source=_resolve_source(src, base_dir),
            rename_to=rename,
        )
    _logger.warning("ontologies.sources: ignoring unrecognized entry %r", entry)
    return None


def load_ontology_config() -> OntologyConfig:
    """Read ontology settings from the acquirium.toml pointed to by
    ``ACQUIRIUM_CONFIG`` (set by the CLI). Returns an empty config when
    no file is reachable — bundled ontology defaults still load."""
    try:
        loaded = load_active_config()
    except Exception as exc:
        _logger.warning("failed to load Acquirium config: %s", exc)
        return OntologyConfig()

    raw_sources = loaded.data.get("ontologies", {}).get("sources", [])
    parsed = (
        source
        for source in (_parse_entry(entry, loaded.directory) for entry in raw_sources)
        if source is not None
    )
    return OntologyConfig(sources=tuple(parsed))
