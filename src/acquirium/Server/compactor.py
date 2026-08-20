"""Compactor: periodic background pass over ContinuousStore's change-key
manifests.

continuous_batch.md: "The compactor deletes manifests through safe(ref) and
advances the retained floor." All of that logic lives in
:meth:`~acquirium.Storage.continuous.types.ContinuousStore.compact`; this
module is just the FastAPI-lifespan-owned loop that calls it on an interval
and remembers the last report for the metrics endpoint.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from acquirium.Storage.continuous.types import CompactReport, ContinuousStore

logger = logging.getLogger("acquirium.server.compactor")


class Compactor:
    """Runs ``continuous.compact()`` every ``interval_seconds`` until stopped."""

    def __init__(
        self,
        continuous: "ContinuousStore",
        *,
        interval_seconds: float = 60.0,
        chunk_rows: int = 100_000,
    ):
        self._continuous = continuous
        self._interval_seconds = interval_seconds
        self._chunk_rows = chunk_rows
        self._task: asyncio.Task | None = None
        self._stopped = False
        self.last_report: "CompactReport | None" = None

    async def start(self) -> None:
        self._stopped = False
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                await asyncio.sleep(self._interval_seconds)
            except asyncio.CancelledError:
                return
            try:
                self.last_report = await asyncio.to_thread(
                    self._continuous.compact, chunk_rows=self._chunk_rows
                )
                logger.debug(
                    "compaction: deleted %d manifest row(s) across %d ref(s)",
                    self.last_report.manifest_rows_deleted,
                    self.last_report.refs_advanced,
                )
            except Exception:
                logger.exception("compaction pass failed")
