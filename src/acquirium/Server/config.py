"""Reading ontology config straight out of ``acquirium.toml``.

The CLI sets ``ACQUIRIUM_CONFIG`` to the absolute path of the toml file
when it starts uvicorn. The server reads ontology sources directly from
that file, not via environment variables — ontology configuration
changes infrequently and a single source of truth keeps config drift
out of the picture.

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
    # Merged *into* the QUDT unit vocabulary instead of being registered
    # as a standalone ontology. Use this to define site-specific units
    # and to alias existing ones, without replacing the bundled QUDT
    # graph. See OxigraphGraphStore.unit_vocabulary.
    unit_extensions = ["./svcw-units.ttl"]

Relative file paths are resolved against the directory containing the
toml file. URLs and ``urn:`` IRIs are passed through unchanged.
"""

from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

_logger = logging.getLogger("acquirium.config")


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
    #: Files merged *into* the QUDT unit vocabulary rather than registered
    #: as standalone ontologies. See ``[ontologies] unit_extensions``.
    unit_extensions: tuple[str, ...] = ()


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
    ontologies = data.get("ontologies", {})
    raw_sources = ontologies.get("sources", [])
    parsed = (
        s for s in (_parse_entry(e, base_dir) for e in raw_sources) if s is not None
    )

    raw_extensions = ontologies.get("unit_extensions", [])
    if isinstance(raw_extensions, str):
        raw_extensions = [raw_extensions]
    extensions: list[str] = []
    for entry in raw_extensions:
        if not isinstance(entry, str):
            _logger.warning("ontologies.unit_extensions: ignoring non-string %r", entry)
            continue
        extensions.append(_resolve_source(entry, base_dir))

    return OntologyConfig(sources=tuple(parsed), unit_extensions=tuple(extensions))
