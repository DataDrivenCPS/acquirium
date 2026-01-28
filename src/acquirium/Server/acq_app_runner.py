from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any
import requests

from acquirium.Apps.base import App, AppContext, Output
from acquirium.Client.acquirium import Acquirium
from acquirium.internals.app_utils import make_stream_ref_uri
from acquirium.Server.manager import Manager

@dataclass
class AppRunner:
    manager: Manager
    aq: Acquirium

    def __post_init__(self) -> None:
        self._apps: dict[str, App] = {}

    def register(self, app: App) -> None:
        if not getattr(app, "name", None):
            raise ValueError("App must define .name")
        self._apps[app.name] = app

    def run_app(self, app_id: str, *, start=None, end=None, params: dict[str, Any] | None = None) -> list[Output]:
        if app_id not in self._apps:
            raise KeyError(f"Unknown app_id={app_id}")

        app = self._apps[app_id]
        started_at = datetime.now(timezone.utc)
        query_bundle = app.build_query(self.aq)
        if isinstance(query_bundle, dict):
            queries = query_bundle
            query = queries.get("default") or (next(iter(queries.values())) if queries else None)
        else:
            query = query_bundle
            queries = {"default": query}

        ctx = AppContext(
            app_id=app_id,
            started_at=started_at,
            start=start,
            end=end,
            query=query,
            params=params or {},
            queries=queries,
        )

        outputs = app.run(ctx)
        self._persist(outputs)
        return outputs

    def _persist(self, outputs: list[Output]) -> None:
        for out in outputs:
            if out.kind == "timeseries":
                point_uri = out.payload["point_uri"]
                ref_uri = out.payload.get("ref_uri") or make_stream_ref_uri(point_uri)
                rows = out.payload["rows"]
                self.manager.insert_timeseries(ref_uri=ref_uri, rows=rows, point_uri=point_uri)
            elif out.kind == "event":
                point_uri = out.payload["point_uri"]
                ref_uri = out.payload.get("ref_uri") or make_stream_ref_uri(point_uri)
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                value = json.dumps(
                    {
                        "severity": out.payload.get("severity"),
                        "message": out.payload.get("message"),
                        "data": out.payload.get("data") or {},
                    },
                    ensure_ascii=True,
                )
                self.manager.insert_timeseries(ref_uri=ref_uri, rows=[(ts, value)], point_uri=point_uri)
            elif out.kind == "trigger":
                url = out.payload.get("url")
                if not url:
                    raise ValueError("trigger output requires url")
                if "://" not in url:
                    url = f"http://{url}"
                message = out.payload.get("message")
                headers = out.payload.get("headers") or {}
                timeout = out.payload.get("timeout") or 5
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                payload = {
                    "message": message,
                    "ts": ts.isoformat(),
                }
                point_uri = out.payload.get("point_uri")
                if point_uri:
                    payload["point_uri"] = point_uri
                response = requests.post(url, json=payload, headers=headers, timeout=timeout)
                response.raise_for_status()
