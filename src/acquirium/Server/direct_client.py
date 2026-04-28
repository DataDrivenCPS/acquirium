"""In-process Acquirium adapter backed directly by a Manager instance.

Drivers and other in-process consumers receive an ``Acquirium`` object and
make calls like ``aq.insert_timeseries_batch()``.  In the normal remote case
those calls go over HTTP.  ``DirectAcquirium`` satisfies the same interface
but dispatches to ``Manager`` methods in the same process — no network round-
trip, no requirement that the HTTP server be up before the driver can start.

Usage::

    from acquirium.Server.direct_client import DirectAcquirium

    direct = DirectAcquirium(manager)
    driver = MyDriver(direct, cfg)
    driver.setup()
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from acquirium.Client.acquirium import Acquirium
from acquirium.Server.insert_stats import insert_stats

import warnings

if TYPE_CHECKING:
    from acquirium.Server.manager import Manager


class _DirectClient:
    """Minimal AcquiriumClient-compatible shim backed by a Manager."""

    def __init__(self, manager: "Manager", origin: str) -> None:
        self._manager = manager
        self._origin = origin

    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = False,
        wait_for_embedding: bool = False,
    ) -> None:
        self._manager.insert_graph(
            rdf_graph=rdf_graph,
            format=format,
            replace=replace,
            wait_for_embedding=wait_for_embedding,
        )

    def sparql_query(self, query: str, use_union: bool = True) -> dict:
        return self._manager.sparql_dict(query, use_union=use_union)

    def register_datasource(self, source_id: str) -> str:
        return self._manager.register_datasource(source_id)

    def insert_timeseries(
        self,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        point_uri: Optional[str] = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        n = self._manager.insert_timeseries(
            source_id=source_id,
            ref_name=ref_name,
            rows=rows,
            point_uri=point_uri,
            replace=replace,
        )
        insert_stats.record(origin=self._origin, rows=len(rows), streams=[ref_name])
        return {"ok": True, "rows_inserted": n}

    def insert_timeseries_batch(
        self,
        source_id: str,
        streams: dict[str, list[tuple[datetime, Any]]],
    ) -> dict[str, Any]:
        total = self._manager.insert_timeseries_batch(source_id, streams)
        insert_stats.record(
            origin=self._origin,
            rows=sum(len(rows) for rows in streams.values()),
            streams=list(streams.keys()),
        )
        return {"ok": True, "rows_inserted": total}

    def graph_version(self) -> int:
        return self._manager.graph_version()

    def resolve_unit(self, identifier: str) -> dict:
        return self._manager.resolve_unit_info(identifier)

    def resolve_text(
        self,
        text: str,
        kind: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> list[dict]:
        return self._manager.resolve_text(text, kind=kind, top_k=top_k, min_score=min_score)


class DirectAcquirium(Acquirium):
    """Acquirium adapter that dispatches to a Manager in the same process.

    Accepts the same interface as ``Acquirium`` so any Driver subclass works
    without modification.  Skips the ``Acquirium.__init__`` (which would open
    an HTTP connection) and replaces ``self.client`` with a ``_DirectClient``.

    Args:
        manager: The running Manager instance to dispatch calls to.
    """

    def __init__(
        self,
        manager: "Manager",
        origin: str = "inprocess",
        insert_batch_rows: int = 50_000,
    ) -> None:
        object.__init__(self)          # bypass Acquirium.__init__ / AcquiriumClient
        self._manager = manager
        self.client = _DirectClient(manager, origin=origin)
        self.insert_batch_rows = int(insert_batch_rows)
        if self.insert_batch_rows <= 0:
            raise ValueError("insert_batch_rows must be greater than zero")

    # Override graph_version to avoid the HTTP path inherited from Acquirium
    def graph_version(self) -> int:
        return self._manager.graph_version()

    # Override register_stream to call _resolve_qudt_uri via manager directly
    # (the inherited version works via self.client, which already dispatches to
    # manager — so the inherited implementation is fine as-is.  No override needed.)

    # Override insert_graph so drivers calling aq.insert_graph() also work
    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = True,
        wait_for_embedding: bool = False,
    ) -> None:
        self._manager.insert_graph(
            rdf_graph=rdf_graph,
            format=format,
            replace=replace,
            wait_for_embedding=wait_for_embedding,
        )
