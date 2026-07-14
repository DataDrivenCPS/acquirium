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
from rdflib import URIRef

from acquirium.Client.acquirium import Acquirium
from acquirium.Server.insert_stats import insert_stats
from acquirium.internals.models import split_record_uri_inputs

import logging
import warnings

logger = logging.getLogger("acquirium.direct_client")

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
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
    ) -> None:
        self._manager.insert_graph(
            rdf_graph=rdf_graph,
            format=format,
            replace=replace,
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
        logger.debug(
            "DirectClient.insert_timeseries source=%s ref_name=%s rows=%d replace=%s origin=%s",
            source_id, ref_name, len(rows), replace, self._origin,
        )
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
        logger.debug(
            "DirectClient.insert_timeseries_batch source=%s streams=%d total_rows=%d origin=%s",
            source_id, len(streams), sum(len(rows) for rows in streams.values()), self._origin,
        )
        total = self._manager.insert_timeseries_batch(source_id, streams)
        insert_stats.record(
            origin=self._origin,
            rows=sum(len(rows) for rows in streams.values()),
            streams=list(streams.keys()),
        )
        return {"ok": True, "rows_inserted": total}

    def insert_timeseries_arrow(self, source_id: str, table: "pa.Table") -> dict[str, Any]:
        streams = table.column("ref_name").unique().to_pylist()
        logger.debug(
            "DirectClient.insert_timeseries_arrow source=%s arrow_rows=%d unique_streams=%d origin=%s",
            source_id, len(table), len(streams), self._origin,
        )
        total = self._manager.insert_timeseries_arrow(source_id, table)
        insert_stats.record(origin=self._origin, rows=total, streams=streams)
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
        context: Optional[list[str]] = None,
    ) -> list[dict]:
        return self._manager.resolve_text(
            text,
            kind=kind,
            top_k=top_k,
            min_score=min_score,
            context=context,
        )

    def resolve_record(
        self,
        fields: dict[str, tuple[str, Optional[str]]],
        top_k: int = 5,
        min_score: float = 0.5,
        context: Optional[list[str]] = None,
    ) -> dict[str, list[dict]]:
        return self._manager.resolve_record(
            fields,
            top_k=top_k,
            min_score=min_score,
            context=context,
        )

    def resolve_record_uris(
        self,
        fields: dict[str, tuple[Any, Optional[str]]],
        min_score: float = 0.5,
    ) -> dict[str, str | URIRef | None]:
        out, to_resolve, context = split_record_uri_inputs(fields)
        if to_resolve:
            matches = self.resolve_record(
                to_resolve,
                top_k=1,
                min_score=min_score,
                context=context or None,
            )
            for name in to_resolve:
                m = matches.get(name) or []
                out[name] = m[0]["uri"] if m else None
        return out


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

    # Override insert_graph so drivers calling aq.insert_graph() also work
    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = True,
    ) -> None:
        self._manager.insert_graph(
            rdf_graph=rdf_graph,
            format=format,
            replace=replace,
        )
