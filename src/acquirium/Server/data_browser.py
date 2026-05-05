from __future__ import annotations

import json
import re
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional
from urllib.parse import urlencode

import pyarrow as pa
from dateutil import parser as dtparser
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from acquirium.Server.app import app


_RELATIVE_TIME_RE = re.compile(
    r"^(?P<sign>[+-])(?P<amount>\d+(?:\.\d+)?)(?P<unit>ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)$",
    re.IGNORECASE,
)


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    s = s.strip()
    if s.lower() == "now":
        return datetime.now(timezone.utc)
    rel = _RELATIVE_TIME_RE.match(s)
    if rel:
        amount = float(rel.group("amount"))
        unit = rel.group("unit").lower()
        seconds_by_unit = {
            "ms": 0.001,
            "millisecond": 0.001,
            "milliseconds": 0.001,
            "s": 1,
            "sec": 1,
            "secs": 1,
            "second": 1,
            "seconds": 1,
            "m": 60,
            "min": 60,
            "mins": 60,
            "minute": 60,
            "minutes": 60,
            "h": 3600,
            "hr": 3600,
            "hrs": 3600,
            "hour": 3600,
            "hours": 3600,
            "d": 86400,
            "day": 86400,
            "days": 86400,
            "w": 604800,
            "week": 604800,
            "weeks": 604800,
        }
        delta = timedelta(seconds=amount * seconds_by_unit[unit])
        now = datetime.now(timezone.utc)
        return now - delta if rel.group("sign") == "-" else now + delta
    parsed = dtparser.isoparse(s)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _json_timeseries_rows(batches: Iterator[pa.RecordBatch]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in batches:
        table = pa.Table.from_batches([batch])
        ts_col = table.column("ts")
        value_col = table.column("value")
        uri_col = table.column("uri")
        for i in range(len(table)):
            ts = ts_col[i].as_py()
            rows.append(
                {
                    "ts": ts.isoformat() if ts is not None else None,
                    "value": value_col[i].as_py(),
                    "uri": uri_col[i].as_py(),
                }
            )
    return rows


def _json_timeseries_columns(
    batches: Iterator[pa.RecordBatch],
    *,
    ts_format: str = "unix_ms",
) -> dict[str, list[Any]]:
    ts_values: list[Any] = []
    value_values: list[Any] = []
    for batch in batches:
        table = pa.Table.from_batches([batch])
        ts_col = table.column("ts")
        value_col = table.column("value")
        for i in range(len(table)):
            ts = ts_col[i].as_py()
            if ts_format == "iso":
                ts_values.append(ts.isoformat() if ts is not None else None)
            else:
                ts_values.append(int(ts.timestamp() * 1000) if ts is not None else None)
            value_values.append(value_col[i].as_py())
    return {"ts": ts_values, "value": value_values}


def _with_query(url: str, **params: str) -> str:
    return f"{url}?{urlencode(params)}"


def _absolute_url(request: Request, path: str) -> str:
    return f"{str(request.base_url).rstrip('/')}{path}"


_ENCODED_SOURCE_PREFIX = "~b64~"


def _encoded_source_id(source_id: str) -> str:
    if all(ch.isalnum() or ch in "._-~" for ch in source_id):
        return source_id
    token = urlsafe_b64encode(source_id.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{_ENCODED_SOURCE_PREFIX}{token}"


def _decoded_source_id(source_id: str) -> str:
    if not source_id.startswith(_ENCODED_SOURCE_PREFIX):
        return source_id
    token = source_id[len(_ENCODED_SOURCE_PREFIX):]
    padding = "=" * (-len(token) % 4)
    try:
        return urlsafe_b64decode(f"{token}{padding}".encode("ascii")).decode("utf-8")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid source id token: {token}") from exc


def _source_url(request: Request, source_id: str) -> str:
    return _absolute_url(request, f"/source/{_encoded_source_id(source_id)}")


def _source_streams_url(request: Request, source_id: str) -> str:
    return _absolute_url(request, f"/source/{_encoded_source_id(source_id)}/streams")


def _data_url_params(
    *,
    limit: int,
    start: Optional[str],
    end: Optional[str],
    order: str,
    format: str,
    ts_format: str,
) -> dict[str, str]:
    params = {"limit": str(limit), "order": order, "format": format, "ts_format": ts_format}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    return params


def _validate_stream_data_options(*, limit: int, order: str, format: str, ts_format: str) -> None:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be greater than zero")
    if limit > 100_000:
        raise HTTPException(status_code=400, detail="limit must be <= 100000")
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
    if format not in ("columns", "rows"):
        raise HTTPException(status_code=400, detail="format must be 'columns' or 'rows'")
    if ts_format not in ("unix_ms", "iso"):
        raise HTTPException(status_code=400, detail="ts_format must be 'unix_ms' or 'iso'")


def _deduplicate_streams(streams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for stream in streams:
        key = str(stream["ref_uri"])
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = dict(stream)
            order.append(key)
            continue

        for field in ("point_uri", "label", "stored_at", "earliest", "latest"):
            if existing.get(field) is None and stream.get(field) is not None:
                existing[field] = stream[field]
        existing["row_count"] = max(int(existing.get("row_count") or 0), int(stream.get("row_count") or 0))
    return [deduped[key] for key in order]


class PrettyJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            separators=(",", ": "),
            default=str,
        ).encode("utf-8")


@app.get("/source", response_class=PrettyJSONResponse)
def browse_sources(request: Request) -> dict[str, Any]:
    sources = app.state.manager.list_sources()
    return {
        "kind": "source-index",
        "count": len(sources),
        "sources": [
            {
                **source,
                "url": _source_url(request, source["source_id"]),
                "streams_url": _source_streams_url(request, source["source_id"]),
            }
            for source in sources
        ],
    }


@app.get("/source/{source_id}/streams", name="browse_source_streams", response_class=PrettyJSONResponse)
def browse_source_streams(
    request: Request,
    source_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 1000,
    order: str = "desc",
    format: str = "columns",
    ts_format: str = "unix_ms",
) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    _validate_stream_data_options(limit=limit, order=order, format=format, ts_format=ts_format)
    streams = _deduplicate_streams(app.state.manager.list_source_streams(source_id))
    if not streams and app.state.manager.get_source(source_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown source_id: {source_id}")
    data_params = _data_url_params(
        limit=limit,
        start=start,
        end=end,
        order=order,
        format=format,
        ts_format=ts_format,
    )
    return {
        "kind": "stream-index",
        "source_id": source_id,
        "count": len(streams),
        "data_url_defaults": data_params,
        "streams": [
            {
                **stream,
                "url": _with_query(str(request.url_for("browse_stream_by_ref")), ref_uri=stream["ref_uri"]),
                "data_url": _with_query(
                    str(request.url_for("browse_stream_data_by_ref")),
                    ref_uri=stream["ref_uri"],
                    **data_params,
                ),
            }
            for stream in streams
        ],
    }


def _stream_data_response(
    stream: dict[str, Any],
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 1000,
    order: str = "desc",
    format: str = "columns",
    ts_format: str = "unix_ms",
) -> dict[str, Any]:
    _validate_stream_data_options(limit=limit, order=order, format=format, ts_format=ts_format)
    parsed_start = _parse_dt(start)
    parsed_end = _parse_dt(end)
    ref_uri = stream["ref_uri"]
    batches = app.state.manager.timeseries_batch(
        uri=ref_uri,
        start=parsed_start,
        end=parsed_end,
        limit=limit,
        order=order,  # type: ignore[arg-type]
        batch_size=min(limit, 50_000),
    )
    payload: dict[str, Any]
    if format == "rows":
        payload = {"encoding": "rows", "rows": _json_timeseries_rows(batches)}
    else:
        payload = {
            "encoding": "columns",
            "ts_format": ts_format,
            "columns": ["ts", "value"],
            "data": _json_timeseries_columns(batches, ts_format=ts_format),
        }
    response = {
        "kind": "stream-data",
        "source_id": stream["source_id"],
        "ref_name": stream["ref_name"],
        "ref_uri": ref_uri,
        "point_uri": stream["point_uri"],
        "limit": limit,
        "order": order,
        **payload,
    }
    if start is not None:
        response["start"] = start
        response["start_resolved"] = parsed_start
    if end is not None:
        response["end"] = end
        response["end_resolved"] = parsed_end
    return response


def _stream_detail_response(request: Request, stream: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "stream",
        **stream,
        "url": _with_query(str(request.url_for("browse_stream_by_ref")), ref_uri=stream["ref_uri"]),
        "data_url": _with_query(str(request.url_for("browse_stream_data_by_ref")), ref_uri=stream["ref_uri"]),
    }


@app.get("/streams/by-ref", name="browse_stream_by_ref", response_class=PrettyJSONResponse)
def browse_stream_by_ref(request: Request, ref_uri: str) -> dict[str, Any]:
    stream = app.state.manager.get_stream_by_ref_uri(ref_uri)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream reference: {ref_uri}")
    return _stream_detail_response(request, stream)


@app.get("/streams/data", name="browse_stream_data_by_ref", response_class=PrettyJSONResponse)
def browse_stream_data_by_ref(
    ref_uri: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 1000,
    order: str = "desc",
    format: str = "columns",
    ts_format: str = "unix_ms",
) -> dict[str, Any]:
    stream = app.state.manager.get_stream_by_ref_uri(ref_uri)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream reference: {ref_uri}")
    return _stream_data_response(
        stream,
        start=start,
        end=end,
        limit=limit,
        order=order,
        format=format,
        ts_format=ts_format,
    )


@app.get("/source/{source_id}/streams/by-ref", response_class=PrettyJSONResponse)
def browse_source_stream_by_ref(request: Request, source_id: str, ref_uri: str) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    stream = app.state.manager.get_source_stream_by_ref_uri(source_id, ref_uri)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream reference for source {source_id}: {ref_uri}")
    return _stream_detail_response(request, stream)


@app.get("/source/{source_id}/streams/data", response_class=PrettyJSONResponse)
def browse_source_stream_data_by_ref(
    source_id: str,
    ref_uri: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 1000,
    order: str = "desc",
    format: str = "columns",
    ts_format: str = "unix_ms",
) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    stream = app.state.manager.get_source_stream_by_ref_uri(source_id, ref_uri)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream reference for source {source_id}: {ref_uri}")
    return _stream_data_response(
        stream,
        start=start,
        end=end,
        limit=limit,
        order=order,
        format=format,
        ts_format=ts_format,
    )


@app.get("/source/{source_id}/streams/{ref_name:path}/data", name="browse_source_stream_data", response_class=PrettyJSONResponse)
def browse_source_stream_data(
    source_id: str,
    ref_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 1000,
    order: str = "desc",
    format: str = "columns",
    ts_format: str = "unix_ms",
) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    stream = app.state.manager.get_source_stream(source_id, ref_name)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream: {source_id}/{ref_name}")
    return _stream_data_response(
        stream,
        start=start,
        end=end,
        limit=limit,
        order=order,
        format=format,
        ts_format=ts_format,
    )


@app.get("/source/{source_id}/streams/{ref_name:path}", name="browse_source_stream", response_class=PrettyJSONResponse)
def browse_source_stream(request: Request, source_id: str, ref_name: str) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    stream = app.state.manager.get_source_stream(source_id, ref_name)
    if stream is None:
        raise HTTPException(status_code=404, detail=f"Unknown stream: {source_id}/{ref_name}")
    return _stream_detail_response(request, stream)


@app.get("/source/{source_id}", name="browse_source", response_class=PrettyJSONResponse)
def browse_source(request: Request, source_id: str) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    source = app.state.manager.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Unknown source_id: {source_id}")
    return {
        "kind": "source",
        **source,
        "streams_url": _source_streams_url(request, source_id),
    }
