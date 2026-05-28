"""Reading ontology config straight out of ``acquirium.toml``.

The CLI sets ``ACQUIRIUM_CONFIG`` to the absolute path of the toml file
when it starts uvicorn. The server reads ontology-related settings
directly from that file, not via environment variables — ontology
configuration changes infrequently and a single source of truth keeps
config drift out of the picture.

Returned values:

- ``sources``: list of fully-resolved URLs or absolute file paths from
  ``[ontologies] sources = [...]``. Relative file paths are resolved
  against the directory containing the toml file.
- ``dependencies``: list of strings from ``[server] ontology_dependencies``
  (also accepted as a comma-separated string for backward compat).

If ``ACQUIRIUM_CONFIG`` is unset or the file can't be read, both lists
are empty — bundled ontology defaults still load.
"""

from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

_logger = logging.getLogger("acquirium.config")


@dataclass
class OntologyConfig:
    sources: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)


def load_ontology_config() -> OntologyConfig:
    """Read ontology settings from the acquirium.toml pointed to by
    ``ACQUIRIUM_CONFIG`` (set by the CLI). Returns empty lists when no
    config file is reachable."""
    cfg_path_str = os.getenv("ACQUIRIUM_CONFIG")
    if not cfg_path_str:
        return OntologyConfig()
    cfg_path = Path(cfg_path_str)
    if not cfg_path.exists():
        _logger.warning("ACQUIRIUM_CONFIG points to missing file: %s", cfg_path)
        return OntologyConfig()
    try:
        with cfg_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as exc:
        _logger.warning("failed to parse %s: %s", cfg_path, exc)
        return OntologyConfig()

    base_dir = cfg_path.resolve().parent

    sources: list[str] = []
    ontologies = data.get("ontologies")
    if isinstance(ontologies, dict):
        raw_sources = ontologies.get("sources")
        if isinstance(raw_sources, list):
            for src in raw_sources:
                s = str(src)
                if "://" in s or s.startswith("urn:"):
                    sources.append(s)
                    continue
                p = Path(s)
                if not p.is_absolute():
                    p = (base_dir / p).resolve()
                sources.append(str(p))

    dependencies: list[str] = []
    server_cfg = data.get("server")
    if isinstance(server_cfg, dict):
        dep = server_cfg.get("ontology_dependencies")
        if isinstance(dep, list):
            dependencies = [str(d) for d in dep]

    return OntologyConfig(sources=sources, dependencies=dependencies)
