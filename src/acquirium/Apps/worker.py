from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
import sys
import requests
import time

from acquirium import Acquirium
from acquirium.Apps.base import Output, App
from acquirium.internals.app_utils import make_stream_ref_uri
from acquirium.internals.models import AppContext

# Configure logging for container output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("acquirium.worker")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _normalize_url(url: str) -> str:
    if "://" not in url:
        return f"http://{url}"
    return url


def _run_once(app: App, ctx: AppContext, aq: Acquirium, run_count: int = 0) -> int:
    """Execute a single run of the app and persist outputs."""
    run_count += 1
    logger.info("Run #%d starting for app '%s'", run_count, ctx.app_id)
    start_time = time.time()

    try:
        outputs = app.run(ctx)
        elapsed = time.time() - start_time
        logger.info("Run #%d completed in %.3fs, produced %d outputs", run_count, elapsed, len(outputs))
        _persist_outputs(aq, outputs)
        return run_count
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("Run #%d failed after %.3fs: %s", run_count, elapsed, e)
        raise


def _load_app_from_file(path: str, class_name: str | None) -> App:
    logger.info("Loading app from file: %s", path)
    spec = importlib.util.spec_from_file_location("acquirium_app", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load app file {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if class_name:
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ValueError(f"App class {class_name} not found in {path}")
        app = cls()
        logger.info("Loaded app class '%s' from file", class_name)
        return app
    for name, obj in module.__dict__.items():
        if isinstance(obj, type) and issubclass(obj, App) and obj is not App:
            app = obj()
            logger.info("Loaded app class '%s' (auto-discovered) from file", name)
            return app
    raise ValueError("No App subclass found in app file")


def _load_app(module: str, class_name: str) -> App:
    logger.info("Loading app from module: %s.%s", module, class_name)
    mod = importlib.import_module(module)
    cls = getattr(mod, class_name)
    app = cls()
    logger.info("Loaded app '%s' v%s", getattr(app, 'name', class_name), getattr(app, 'version', '?'))
    return app


def _persist_outputs(aq: Acquirium, outputs: list[Output]) -> None:
    for i, out in enumerate(outputs):
        if out.kind == "timeseries":
            point_uri = out.payload["point_uri"]
            ref_uri = out.payload.get("ref_uri") or make_stream_ref_uri(point_uri)
            rows = out.payload["rows"]
            logger.debug("Output %d: persisting %d timeseries rows to %s", i + 1, len(rows), point_uri)
            aq.client.insert_timeseries(ref_uri=ref_uri, rows=rows, point_uri=point_uri)
            logger.info("Output %d: wrote %d timeseries rows to %s", i + 1, len(rows), point_uri)
        elif out.kind == "event":
            point_uri = out.payload["point_uri"]
            ref_uri = out.payload.get("ref_uri") or make_stream_ref_uri(point_uri)
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
            aq.client.insert_timeseries(ref_uri=ref_uri, rows=[(ts, value)], point_uri=point_uri)
            logger.info("Output %d: emitted %s event to %s", i + 1, severity, point_uri)
        elif out.kind == "trigger":
            url = out.payload.get("url")
            if not url:
                raise ValueError("trigger output requires url")
            url = _normalize_url(url)
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
            logger.debug("Output %d: triggering webhook %s", i + 1, url)
            response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            response.raise_for_status()
            logger.info("Output %d: triggered webhook %s (status %d)", i + 1, url, response.status_code)


def main() -> None:
    module = os.getenv("ACQUIRIUM_APP_MODULE")
    class_name = os.getenv("ACQUIRIUM_APP_CLASS")
    app_file = os.getenv("ACQUIRIUM_APP_FILE")
    app_root = os.getenv("ACQUIRIUM_APP_ROOT")
    if app_root:
        sys.path.insert(0, app_root)

    server_url = os.getenv("ACQUIRIUM_SERVER_URL", "localhost")
    server_port = int(os.getenv("ACQUIRIUM_SERVER_PORT", "8000"))
    use_ssl = os.getenv("ACQUIRIUM_USE_SSL", "false").lower() == "true"
    lexicon_path = os.getenv("ACQUIRIUM_LEXICON_PATH")
    app_id = os.getenv("ACQUIRIUM_APP_ID")

    params_raw = os.getenv("ACQUIRIUM_APP_PARAMS", "{}")
    try:
        params: dict[str, Any] = json.loads(params_raw)
    except json.JSONDecodeError:
        params = {}

    if app_file:
        app = _load_app_from_file(app_file, class_name)
    else:
        if not module or not class_name:
            raise ValueError("ACQUIRIUM_APP_MODULE and ACQUIRIUM_APP_CLASS are required")
        app = _load_app(module, class_name)
    if not app_id:
        app_id = app.name

    aq = Acquirium(server_url=server_url, server_port=server_port, use_ssl=use_ssl, lexicon_path=lexicon_path)

    query_bundle = app.build_query(aq)
    if isinstance(query_bundle, dict):
        queries = query_bundle
        query = queries.get("default") or (next(iter(queries.values())) if queries else None)
    else:
        query = query_bundle
        queries = {"default": query}

    ctx = AppContext(
        app_id=app_id,
        started_at=datetime.now(timezone.utc),
        start=_parse_dt(os.getenv("ACQUIRIUM_RUN_START")),
        end=_parse_dt(os.getenv("ACQUIRIUM_RUN_END")),
        query=query,
        params=params,
        queries=queries,
    )

    keep_alive = os.getenv("ACQUIRIUM_KEEP_ALIVE", "false").lower() == "true"
    interval = float(os.getenv("ACQUIRIUM_KEEP_ALIVE_INTERVAL", "10"))

    if not keep_alive:
        _run_once(app, ctx, aq)
        return

    while True:
        _run_once(app, ctx, aq)
        time.sleep(max(interval, 0.0))


if __name__ == "__main__":
    main()
