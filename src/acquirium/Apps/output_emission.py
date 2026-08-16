from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import requests

from acquirium.Apps.base import Output


InsertTimeseries = Callable[..., Any]


def normalize_trigger_url(url: str) -> str:
    """Return a trigger URL with an explicit scheme."""
    if "://" not in url:
        return f"http://{url}"
    return url


class PersistSink:
    """Deliver validated app outputs to storage and external webhooks."""

    def __init__(
        self,
        *,
        insert_timeseries: InsertTimeseries,
        logger: logging.Logger | None = None,
    ) -> None:
        self.insert_timeseries = insert_timeseries
        self.logger = logger

    def emit(self, source_id: str, outputs: list[Output]) -> list[dict[str, Any]]:
        effects: list[dict[str, Any]] = []
        for index, out in enumerate(outputs, start=1):
            if out.kind == "timeseries":
                point_uri = out.payload["point_uri"]
                ref_name = out.payload.get("ref_name") or point_uri
                rows = out.payload["rows"]
                if self.logger is not None:
                    self.logger.debug("Output %d: persisting %d timeseries rows to %s", index, len(rows), point_uri)
                self.insert_timeseries(source_id=source_id, ref_name=ref_name, rows=rows, point_uri=point_uri)
                effects.append({"kind": "timeseries_insert", "point_uri": point_uri, "ref_name": ref_name, "rows": len(rows)})
                if self.logger is not None:
                    self.logger.info("Output %d: wrote %d timeseries rows to %s", index, len(rows), point_uri)
            elif out.kind == "event":
                point_uri = out.payload["point_uri"]
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                severity = out.payload.get("severity", "INFO")
                value = json.dumps(
                    {
                        "severity": severity,
                        "message": out.payload.get("message"),
                        "data": out.payload.get("data") or {},
                    },
                    ensure_ascii=True,
                )
                self.insert_timeseries(
                    source_id=source_id,
                    ref_name=point_uri,
                    rows=[(ts, value)],
                    point_uri=point_uri,
                )
                effects.append({"kind": "event_insert", "point_uri": point_uri, "rows": 1})
                if self.logger is not None:
                    self.logger.info("Output %d: emitted %s event to %s", index, severity, point_uri)
            elif out.kind == "trigger":
                url = out.payload.get("url")
                if not url:
                    raise ValueError("trigger output requires url")
                url = normalize_trigger_url(url)
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                payload = {
                    "message": out.payload.get("message"),
                    "ts": ts.isoformat(),
                }
                point_uri = out.payload.get("point_uri")
                if point_uri:
                    payload["point_uri"] = point_uri

                headers = out.payload.get("headers") or {}
                timeout = out.payload.get("timeout") or 5
                if self.logger is not None:
                    self.logger.debug("Output %d: triggering webhook %s", index, url)
                response = requests.post(url, json=payload, headers=headers, timeout=timeout)
                response.raise_for_status()
                effects.append({"kind": "webhook_post", "url": url, "status": response.status_code})
                if self.logger is not None:
                    self.logger.info("Output %d: triggered webhook %s (status %d)", index, url, response.status_code)
        return effects


class PreviewSink:
    """Describe output effects without writing data or calling webhooks."""

    def __init__(self, *, max_rows: int = 20) -> None:
        self.max_rows = max(0, max_rows)

    def emit(self, source_id: str, outputs: list[Output]) -> list[dict[str, Any]]:
        effects: list[dict[str, Any]] = []
        for out in outputs:
            if out.kind == "timeseries":
                rows = out.payload["rows"]
                effects.append({
                    "kind": "would_insert_timeseries",
                    "source_id": source_id,
                    "point_uri": out.payload["point_uri"],
                    "ref_name": out.payload.get("ref_name") or out.payload["point_uri"],
                    "row_count": len(rows),
                    "rows": rows[: self.max_rows],
                    "truncated": len(rows) > self.max_rows,
                })
            elif out.kind == "event":
                effects.append({
                    "kind": "would_insert_event",
                    "source_id": source_id,
                    **out.payload,
                })
            elif out.kind == "trigger":
                effects.append({
                    "kind": "would_post_webhook",
                    "url": normalize_trigger_url(out.payload["url"]),
                    "message": out.payload.get("message"),
                    "point_uri": out.payload.get("point_uri"),
                    "headers": out.payload.get("headers") or {},
                })
        return effects


def emit_outputs(
    source_id: str,
    outputs: list[Output],
    *,
    insert_timeseries: InsertTimeseries,
    logger: logging.Logger | None = None,
) -> None:
    """Compatibility wrapper around :class:`PersistSink`.

    This is shared by both app execution paths:

    - server-side ``AppRunner`` passes ``Manager.insert_timeseries``;
    - external app workers pass ``AcquiriumClient.insert_timeseries``.

    The caller owns transport and storage details. This helper owns the common
    app-output contract: timeseries outputs write their rows, event outputs are
    serialized as one text row, and trigger outputs execute an HTTP webhook.
    """
    PersistSink(insert_timeseries=insert_timeseries, logger=logger).emit(source_id, outputs)
