"""Persistent ownership metadata for named RDF graphs.

The registry deliberately lives beside, rather than inside, the RDF dataset.
It prevents Acquirium's own catalog records from becoming input data for SHACL
inference or validation.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock
from urllib.parse import quote


# Preserve the existing main graph as the plant graph during migration.
PLANT_GRAPH_URI = "urn:acquirium#MainGraph"
# The plant model is a first-class deployment source. Public callers must use
# this ID instead of relying on an omitted source owner.
PLANT_SOURCE_ID = "plant"
ACQUIRIUM_GRAPH_URI = "urn:acquirium:graph:data:acquirium"


@dataclass(frozen=True)
class GraphRecord:
    """A named graph's role and owner in one Acquirium deployment."""

    uri: str
    role: str
    owner: str


class GraphRegistry:
    """Registry of graphs that are inputs to the deployment data union."""

    def __init__(self, path: str | Path, *, plant_graph_uri: str = PLANT_GRAPH_URI) -> None:
        self.path = Path(path)
        self.plant_graph_uri = plant_graph_uri
        self._lock = RLock()
        self._records = self._load()
        self._ensure_core_graphs()

    def data_graphs(self) -> list[GraphRecord]:
        """Return all data-input graphs in stable URI order."""
        with self._lock:
            return sorted(
                (record for record in self._records.values() if record.role == "data"),
                key=lambda record: record.uri,
            )

    def source_graph(self, source_id: str) -> GraphRecord:
        """Return (and persist when new) the data graph owned by ``source_id``."""
        if not source_id:
            raise ValueError("source_id must not be empty")
        if source_id == PLANT_SOURCE_ID:
            return self._records["plant"]
        key = f"source:{source_id}"
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                return existing
            record = GraphRecord(
                uri=f"urn:acquirium:graph:data:source:{quote(source_id, safe='')}",
                role="data",
                owner=key,
            )
            self._records[key] = record
            self._write()
            return record

    def remove_source(self, source_id: str) -> GraphRecord | None:
        """Remove a source's registry entry and return its former graph record."""
        with self._lock:
            removed = self._records.pop(f"source:{source_id}", None)
            if removed is not None:
                self._write()
            return removed

    def _ensure_core_graphs(self) -> None:
        changed = False
        for key, uri in (("plant", self.plant_graph_uri), ("acquirium", ACQUIRIUM_GRAPH_URI)):
            if key not in self._records:
                self._records[key] = GraphRecord(uri=uri, role="data", owner=key)
                changed = True
        if changed:
            self._write()

    def _load(self) -> dict[str, GraphRecord]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text())
            records = raw.get("records", {})
            if not isinstance(records, dict):
                raise ValueError("records is not an object")
            return {
                key: GraphRecord(**value)
                for key, value in records.items()
                if isinstance(key, str) and isinstance(value, dict)
            }
        except (OSError, TypeError, ValueError) as exc:
            raise RuntimeError(f"invalid graph registry at {self.path}: {exc}") from exc

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "records": {key: asdict(record) for key, record in self._records.items()},
        }
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", dir=self.path.parent, text=True,
        )
        try:
            with os.fdopen(fd, "w") as temporary:
                json.dump(payload, temporary, sort_keys=True)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, self.path)
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
