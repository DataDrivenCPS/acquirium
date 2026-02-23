from __future__ import annotations

import importlib
import importlib.util
import inspect
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


def _filter_params_for_signature(sig: inspect.Signature, params: dict[str, Any]) -> tuple[dict[str, Any], list[str], bool]:
    params = params or {}
    accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if accepts_kwargs:
        return dict(params), [], True
    accepted = {k: v for k, v in params.items() if k in sig.parameters}
    missing: list[str] = []
    for name, p in sig.parameters.items():
        if name in {"self", "cls"}:
            continue
        if p.default is inspect.Parameter.empty and p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            if name not in accepted:
                missing.append(name)
    return accepted, missing, False


def _call_with_params(fn: Any, params: dict[str, Any], *, label: str, allow_params_arg: bool = True) -> Any:
    params = params or {}
    sig = inspect.signature(fn)

    if allow_params_arg and "params" in sig.parameters:
        p = sig.parameters["params"]
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            try:
                return fn(params)
            except TypeError:
                pass

    kwargs, missing, _ = _filter_params_for_signature(sig, params)
    if missing:
        raise TypeError(f"{label} missing required params: {', '.join(missing)}")
    return fn(**kwargs)


def _configure_app(app: App, params: dict[str, Any]) -> None:
    if not params:
        return
    for hook in ("configure", "set_params", "apply_params"):
        fn = getattr(app, hook, None)
        if callable(fn):
            try:
                _call_with_params(fn, params, label=f"{app.__class__.__name__}.{hook}")
                return
            except TypeError as exc:
                logger.warning("App %s %s failed: %s", app.__class__.__name__, hook, exc)
    for key, value in params.items():
        try:
            setattr(app, key, value)
        except Exception as exc:
            logger.debug("Unable to set param %s on %s: %s", key, app.__class__.__name__, exc)


def _instantiate_app_class(cls: type[App], params: dict[str, Any]) -> App:
    if not issubclass(cls, App):
        raise ValueError(f"{cls.__name__} is not an App subclass")

    factory = getattr(cls, "from_params", None)
    if callable(factory) and params:
        try:
            app = _call_with_params(factory, params, label=f"{cls.__name__}.from_params")
            if not isinstance(app, App):
                raise ValueError(f"{cls.__name__}.from_params did not return an App")
            _configure_app(app, params)
            return app
        except Exception as exc:
            logger.warning("from_params failed for %s: %s; falling back to constructor", cls.__name__, exc)

    sig = inspect.signature(cls.__init__)
    kwargs, missing, accepts_kwargs = _filter_params_for_signature(sig, params)
    if missing:
        raise ValueError(
            f"{cls.__name__} missing required params: {', '.join(missing)}. "
            "Provide ACQUIRIUM_APP_PARAMS or run_app(params=...)."
        )
    if params and not accepts_kwargs:
        unused = set(params) - set(kwargs)
        if unused:
            logger.debug("Ignoring unused params for %s: %s", cls.__name__, sorted(unused))

    try:
        app = cls(**kwargs)
    except TypeError as exc:
        raise ValueError(f"Failed to instantiate {cls.__name__}") from exc

    _configure_app(app, params)
    return app


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


def _load_app_from_file(path: str, class_name: str | None, params: dict[str, Any]) -> App:
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
        app = _instantiate_app_class(cls, params)
        logger.info("Loaded app class '%s' from file", class_name)
        return app

    app_instance = getattr(module, "APP", None) or getattr(module, "app", None)
    if isinstance(app_instance, App):
        _configure_app(app_instance, params)
        logger.info("Loaded app instance from file")
        return app_instance

    for factory_name in ("build_app", "create_app", "make_app", "get_app"):
        factory = getattr(module, factory_name, None)
        if callable(factory):
            app = _call_with_params(factory, params, label=f"{factory_name}()")
            if not isinstance(app, App):
                raise ValueError(f"{factory_name} did not return an App")
            _configure_app(app, params)
            logger.info("Loaded app via factory '%s' from file", factory_name)
            return app

    candidates: list[tuple[str, type[App]]] = []
    for name, obj in module.__dict__.items():
        if isinstance(obj, type) and issubclass(obj, App) and obj is not App:
            candidates.append((name, obj))
    if candidates:
        name, cls = candidates[0]
        if len(candidates) > 1:
            logger.warning("Multiple App subclasses found, using %s", name)
        app = _instantiate_app_class(cls, params)
        logger.info("Loaded app class '%s' (auto-discovered) from file", name)
        return app
    raise ValueError("No App subclass found in app file")


def _load_app(module: str, class_name: str, params: dict[str, Any]) -> App:
    logger.info("Loading app from module: %s.%s", module, class_name)
    mod = importlib.import_module(module)
    cls = getattr(mod, class_name)
    app = _instantiate_app_class(cls, params)
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
    logger.info("=" * 60)
    logger.info("Acquirium Worker Starting")
    logger.info("=" * 60)

    module = os.getenv("ACQUIRIUM_APP_MODULE")
    class_name = os.getenv("ACQUIRIUM_APP_CLASS")
    app_file = os.getenv("ACQUIRIUM_APP_FILE")
    app_root = os.getenv("ACQUIRIUM_APP_ROOT")
    app_id = os.getenv("ACQUIRIUM_APP_ID")

    logger.info("Configuration:")
    logger.info("  App ID: %s", app_id or "(auto)")
    logger.info("  Module: %s", module or "(none)")
    logger.info("  Class: %s", class_name or "(auto-discover)")
    logger.info("  File: %s", app_file or "(none)")

    if app_root:
        sys.path.insert(0, app_root)
        logger.debug("Added %s to Python path", app_root)

    server_url = os.getenv("ACQUIRIUM_SERVER_URL", "localhost")
    server_port = int(os.getenv("ACQUIRIUM_SERVER_PORT", "8000"))
    use_ssl = os.getenv("ACQUIRIUM_USE_SSL", "false").lower() == "true"

    logger.info("  Server: %s://%s:%d", "https" if use_ssl else "http", server_url, server_port)

    params_raw = os.getenv("ACQUIRIUM_APP_PARAMS", "{}")
    try:
        params: dict[str, Any] = json.loads(params_raw)
    except json.JSONDecodeError:
        params = {}
    if params:
        logger.info("  Params: %s", list(params.keys()))

    # Load the app
    logger.info("-" * 40)
    if app_file:
        app = _load_app_from_file(app_file, class_name, params)
    else:
        if not module or not class_name:
            raise ValueError("ACQUIRIUM_APP_MODULE and ACQUIRIUM_APP_CLASS are required")
        app = _load_app(module, class_name, params)
    if not app_id:
        app_id = app.name

    # Connect to server
    logger.info("-" * 40)
    logger.info("Connecting to Acquirium server...")
    aq = Acquirium(server_url=server_url, server_port=server_port, use_ssl=use_ssl)
    logger.info("Connected to server")

    # Build query
    logger.info("Building query...")
    query_bundle = app.build_query(aq)
    if isinstance(query_bundle, dict):
        queries = query_bundle
        query = queries.get("default") or (next(iter(queries.values())) if queries else None)
        logger.info("Built %d named queries: %s", len(queries), list(queries.keys()))
    else:
        query = query_bundle
        queries = {"default": query}
        logger.info("Built single query")

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

    logger.info("-" * 40)
    if not keep_alive:
        logger.info("Running in one-shot mode")
        _run_once(app, ctx, aq, run_count=0)
        logger.info("One-shot run complete, exiting")
        return

    logger.info("Running in keep-alive mode (interval=%.1fs)", interval)
    logger.info("=" * 60)

    run_count = 0
    while True:
        try:
            run_count = _run_once(app, ctx, aq, run_count=run_count)
        except Exception:
            logger.exception("Error during run #%d, will retry after interval", run_count + 1)
            run_count += 1
        logger.info("Sleeping %.1fs until next run...", interval)
        time.sleep(max(interval, 0.0))


if __name__ == "__main__":
    main()
