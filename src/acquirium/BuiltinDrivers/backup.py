from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import polars as pl

from acquirium.Driver import PollingIngestDriver
from acquirium.internals.internals_namespaces import ACQUIRIUM_REF_NAME, ACQUIRIUM_SOURCE_ID

logger = logging.getLogger("acquirium.backup")

_STREAM_QUERY = f"""
SELECT ?ref ?source_id ?ref_name
WHERE {{
  ?ref <{ACQUIRIUM_SOURCE_ID}> ?source_id ;
       <{ACQUIRIUM_REF_NAME}>  ?ref_name .
}}
"""


@dataclass(frozen=True)
class _BackupStream:
    ref_uri: str
    source_id: str
    ref_name: str


def _parse_source_client(cfg: dict) -> Any:
    """Build an Acquirium HTTP client from driver config."""
    from acquirium.Client.acquirium import Acquirium

    raw = cfg.get("source_url", "")
    if not raw:
        raise ValueError("BackupDriver requires 'source_url' in driver config")

    parsed = urlparse(raw if "://" in raw else f"http://{raw}")
    host = parsed.hostname or "localhost"
    port = parsed.port or int(cfg.get("source_port", 8000))
    use_ssl = cfg.get("source_ssl", parsed.scheme == "https")
    return Acquirium(server_url=host, server_port=port, use_ssl=use_ssl)


def _sparql_str(value: Any) -> str | None:
    if value is None:
        return None
    raw = value.get("value") if isinstance(value, dict) else value
    return str(raw) if raw is not None else None


class BackupDriver(PollingIngestDriver):
    """True mirror of a source Acquirium instance.

    Copies the source server's RDF graph and all timeseries data to the
    destination (``self.aq``).  Source identities — ``source_id``,
    ``ref_name``, and the derived ``ref_URI`` — are preserved exactly, so the
    destination becomes a byte-for-byte replica.

    Watermarks are stored in ``self.state`` so only new rows are fetched on
    each tick.  The WAL and exponential backoff inherited from
    ``IngestDriver`` protect against destination outages.

    Required driver config key:

      ``source_url``   Full URL of the source Acquirium server
                       (e.g. ``http://edge-node:8000``)

    Optional:

      ``source_port``  Port override (default: parsed from ``source_url`` or 8000)
      ``source_ssl``   ``true`` to use HTTPS for the source (default: auto-detected)
      ``batch_size``   Max rows fetched per stream per tick (default: 50 000)
    """

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self._batch_size = int(cfg.get("batch_size", 50_000))
        self._source = _parse_source_client(cfg)
        self._streams: list[_BackupStream] = []
        self._sync_graph()
        self._scan_streams()

    def on_graph_change(self) -> None:
        self._sync_graph()
        self._scan_streams()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sync_graph(self) -> None:
        """Export the source graph and insert it verbatim on the destination."""
        try:
            turtle = self._source.client.export_graph()
        except Exception as exc:
            logger.warning("BackupDriver: could not export source graph: %s", exc)
            return
        try:
            self.aq.insert_graph(turtle, format="turtle", replace=True)
        except Exception as exc:
            logger.warning("BackupDriver: could not insert graph on destination: %s", exc)

    def _scan_streams(self) -> None:
        """Discover all streams on the source and register their datasources on the destination."""
        try:
            rows = self._source.client.sparql_query(_STREAM_QUERY).get("rows", [])
        except Exception as exc:
            logger.warning("BackupDriver: source SPARQL query failed: %s", exc)
            return

        streams: list[_BackupStream] = []
        for row in rows:
            try:
                ref_uri, source_id, ref_name = row
                s = _BackupStream(
                    ref_uri=_sparql_str(ref_uri) or "",
                    source_id=_sparql_str(source_id) or "",
                    ref_name=_sparql_str(ref_name) or "",
                )
                if s.ref_uri and s.source_id and s.ref_name:
                    streams.append(s)
            except Exception:
                logger.warning("BackupDriver: could not parse stream row %r", row, exc_info=True)

        self._streams = streams
        for sid in {s.source_id for s in streams}:
            try:
                self.aq.register_datasource(sid)
            except Exception as exc:
                logger.warning("BackupDriver: register_datasource(%r) failed: %s", sid, exc)

        logger.info("BackupDriver: tracking %d stream(s)", len(streams))

    def collect(self) -> pl.DataFrame:
        empty = pl.DataFrame(
            schema={
                "source_id": pl.Utf8,
                "ts": pl.Datetime("us", "UTC"),
                "ref_name": pl.Utf8,
                "value": pl.Utf8,
            }
        )
        if not self._streams:
            return empty

        frames: list[pl.DataFrame] = []
        for stream in self._streams:
            wm_key = f"wm:{stream.ref_uri}"
            watermark_iso: str | None = self.state.get(wm_key)

            try:
                df = self._source.client.timeseries_df(
                    stream.ref_uri,
                    start=watermark_iso,
                    order="asc",
                    limit=self._batch_size,
                )
            except Exception as exc:
                logger.warning(
                    "BackupDriver: could not fetch %s: %s", stream.ref_uri, exc
                )
                continue

            if df.is_empty():
                continue

            # Advance watermark to the latest timestamp in this batch.
            new_wm = df["ts"].max()
            self.state.set(wm_key, new_wm.isoformat())

            frames.append(
                df.with_columns([
                    pl.lit(stream.source_id).alias("source_id"),
                    pl.lit(stream.ref_name).alias("ref_name"),
                ]).select(["source_id", "ts", "ref_name", "value"])
            )

        if not frames:
            return empty
        return pl.concat(frames, how="vertical")
