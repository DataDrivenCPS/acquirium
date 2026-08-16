"""Write an app's provenance to its own provenance graph — without loops.

Provenance is derived bookkeeping, and writing it must never wake the
pollers that decide when apps rebuild their queries (an app writing its
own provenance would otherwise wake itself, forever). Three guards, all in
:class:`ProvenanceWriter`:

1. **Own graph.** Triples go to ``provenance_source_id(app_source_id)`` —
   the reserved ``<source>:prov`` source, whose writes the graph store
   excludes from ``data_version`` — via ``insert_graph(replace=True)``.
   Never ``sparql_update``: that path forces a full closure rebuild.
2. **Content hash.** The writer keeps the hash of the last graph it wrote
   and skips identical rewrites. The hash is **seeded from the store** on
   first use (read back from the graph), so a restart doesn't re-write —
   and re-bump the global version — for nothing.
3. **Cadence floor.** ``prov:used`` grows as runs read new streams; each
   change is a real write. A minimum spacing bounds the write rate; the
   union converges after a few runs, so steady state is zero writes.

The relations written (see ``internals_namespaces``): ``acq:mayUse`` for
every stream the query resolved to (declared, from ``Query.provenance()``),
``prov:used`` for every stream a run actually read (observed, unioned),
and ``prov:wasGeneratedBy`` on each declared output point.
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import TYPE_CHECKING, Any, Iterable

from rdflib import RDF, Graph, Literal, URIRef

from acquirium.internals.app_utils import app_source_id, app_uri_for
from acquirium.internals.internals_namespaces import (
    MAY_USE,
    PROV_ACTIVITY,
    PROV_ENTITY,
    PROV_USED,
    PROV_WAS_GENERATED_BY,
    PROVENANCE_HASH,
)
from acquirium.Storage.graph_registry import provenance_source_id

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium

logger = logging.getLogger("acquirium.apps.provenance")

#: Minimum spacing between two provenance writes for one app.
DEFAULT_MIN_WRITE_INTERVAL = 60.0


def provenance_graph(
    app_name: str,
    *,
    may_use: Iterable[str],
    used: Iterable[str],
    output_points: Iterable[str],
) -> Graph:
    """Build the provenance graph for one app (deterministic for hashing)."""
    app_uri = URIRef(app_uri_for(app_name))
    g = Graph()
    g.add((app_uri, RDF.type, PROV_ACTIVITY))
    for ref in sorted(set(str(u) for u in may_use if u)):
        g.add((app_uri, MAY_USE, URIRef(ref)))
    for ref in sorted(set(str(u) for u in used if u)):
        g.add((app_uri, PROV_USED, URIRef(ref)))
    for point in sorted(set(str(p) for p in output_points if p)):
        g.add((URIRef(point), RDF.type, PROV_ENTITY))
        g.add((URIRef(point), PROV_WAS_GENERATED_BY, app_uri))
    return g


def graph_hash(g: Graph) -> str:
    """Order-independent content hash of a graph."""
    lines = sorted(f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in g)
    return hashlib.sha1("\n".join(lines).encode()).hexdigest()


class ProvenanceWriter:
    """Owns one app's provenance state and its loop-safe write path."""

    def __init__(
        self,
        app_name: str,
        acquirium_cli: "Acquirium",
        *,
        min_write_interval: float = DEFAULT_MIN_WRITE_INTERVAL,
    ):
        self.app_name = app_name
        self.aq = acquirium_cli
        self.source_id = provenance_source_id(app_source_id(app_name))
        self.min_write_interval = float(min_write_interval)
        self.may_use: set[str] = set()
        self.used: set[str] = set()
        self.output_points: set[str] = set()
        self._last_hash: str | None = None
        self._seeded = False
        self._last_write = 0.0
        self._dirty = False

    # ─────────────── state updates ───────────────

    def set_declared(self, ref_uris: Iterable[str]) -> None:
        """Replace the declared (query-resolved) stream set."""
        new = set(str(u) for u in ref_uris if u)
        if new != self.may_use:
            self.may_use = new
            self._dirty = True

    def set_outputs(self, point_uris: Iterable[str]) -> None:
        new = set(str(p) for p in point_uris if p)
        if new != self.output_points:
            self.output_points = new
            self._dirty = True

    def add_observed(self, ref_uris: Iterable[str]) -> None:
        """Union a run's observed reads into the ``prov:used`` set."""
        before = len(self.used)
        self.used.update(str(u) for u in ref_uris if u)
        if len(self.used) != before:
            self._dirty = True

    # ─────────────── writing ───────────────

    def _seed_hash_from_store(self) -> None:
        """Read the hash of the provenance last written (survives restarts)."""
        self._seeded = True
        try:
            rows = self.aq.client.sparql_query(
                f"SELECT ?h WHERE {{ <{app_uri_for(self.app_name)}> <{PROVENANCE_HASH}> ?h }}",
                include_dependencies=False,
            ).get("rows", [])
        except Exception:
            return
        if rows and rows[0] and rows[0][0]:
            self._last_hash = str(rows[0][0])

    def flush(self, *, force: bool = False) -> bool:
        """Write the current provenance if it changed and the floor allows.

        Returns True when a write happened. Safe to call after every run.
        """
        if not self._dirty and not force:
            return False
        now = time.monotonic()
        if not force and now - self._last_write < self.min_write_interval:
            return False
        if not self._seeded:
            self._seed_hash_from_store()
        graph = provenance_graph(
            self.app_name, may_use=self.may_use, used=self.used,
            output_points=self.output_points,
        )
        digest = graph_hash(graph)
        if digest == self._last_hash:
            self._dirty = False
            return False
        # The hash rides in the same graph so a restart can seed from it.
        graph.add((URIRef(app_uri_for(self.app_name)), PROVENANCE_HASH, Literal(digest)))
        try:
            self.aq.insert_graph(
                graph.serialize(format="turtle"), format="turtle",
                replace=True, source_id=self.source_id,
            )
        except Exception:
            logger.exception("provenance write failed for '%s'; will retry", self.app_name)
            return False
        self._last_hash = digest
        self._last_write = now
        self._dirty = False
        logger.debug(
            "provenance written for '%s' (mayUse=%d used=%d outputs=%d)",
            self.app_name, len(self.may_use), len(self.used), len(self.output_points),
        )
        return True

    def status(self) -> dict[str, Any]:
        return {
            "may_use": len(self.may_use),
            "used": len(self.used),
            "outputs": len(self.output_points),
            "pending": self._dirty,
        }
