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


def emit_outputs(
    app_id: str,
    outputs: list[Output],
    *,
    insert_timeseries: InsertTimeseries,
    logger: logging.Logger | None = None,
) -> None:
    """Emit app outputs through the supplied timeseries insertion function.

    This is shared by both app execution paths:

    - server-side ``AppRunner`` passes ``Manager.insert_timeseries``;
    - external app workers pass ``AcquiriumClient.insert_timeseries``.

    The caller owns transport and storage details. This helper owns the common
    app-output contract: timeseries outputs write their rows, event outputs are
    serialized as one text row, and trigger outputs execute an HTTP webhook.
    """
    for index, out in enumerate(outputs, start=1):
        if out.kind == "timeseries":
            point_uri = out.payload["point_uri"]
            rows = out.payload["rows"]
            value_kind = out.payload.get("value_kind", "numeric")
            if logger is not None:
                logger.debug("Output %d: persisting %d timeseries rows to %s", index, len(rows), point_uri)
            insert_timeseries(
                source_id=app_id,
                ref_name=point_uri,
                rows=rows,
                point_uri=point_uri,
                value_kind=value_kind,
            )
            if logger is not None:
                logger.info("Output %d: wrote %d timeseries rows to %s", index, len(rows), point_uri)
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
            insert_timeseries(
                source_id=app_id,
                ref_name=point_uri,
                rows=[(ts, value)],
                point_uri=point_uri,
                value_kind="text",
            )
            if logger is not None:
                logger.info("Output %d: emitted %s event to %s", index, severity, point_uri)
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
            if logger is not None:
                logger.debug("Output %d: triggering webhook %s", index, url)
            response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            response.raise_for_status()
            if logger is not None:
                logger.info("Output %d: triggered webhook %s (status %d)", index, url, response.status_code)
        elif logger is not None:
            logger.warning("Output %d: ignoring unsupported output kind %r", index, out.kind)
