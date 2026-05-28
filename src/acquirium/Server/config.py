"""Reading ontology config straight out of ``acquirium.toml``.

The CLI sets ``ACQUIRIUM_CONFIG`` to the absolute path of the toml file
when it starts uvicorn. The server reads ontology sources directly from
that file, not via environment variables — ontology configuration
changes infrequently and a single source of truth keeps config drift
out of the picture.

``sources`` is a list of fully-resolved URLs or absolute file paths from
``[ontologies] sources = [...]``. Relative file paths are resolved
against the directory containing the toml file. If ``ACQUIRIUM_CONFIG``
is unset or the file can't be read, the list is empty — bundled ontology
defaults still load.
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


def _resolve_source(src: str, base_dir: Path) -> str:
    """URLs and urn: stay as-is; relative file paths are resolved against base_dir."""
    if "://" in src or src.startswith("urn:"):
        return src
    p = Path(src)
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return str(p)


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
    raw_sources = data.get("ontologies", {}).get("sources", [])
    sources = [_resolve_source(str(s), base_dir) for s in raw_sources]
    return OntologyConfig(sources=sources)
