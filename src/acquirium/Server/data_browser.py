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
from fastapi.responses import HTMLResponse, JSONResponse

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
    value_mode: str,
) -> dict[str, str]:
    params = {
        "limit": str(limit),
        "order": order,
        "format": format,
        "ts_format": ts_format,
        "value_mode": value_mode,
    }
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    return params


def _validate_stream_data_options(*, limit: int, order: str, format: str, ts_format: str, value_mode: str) -> None:
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
    if value_mode not in ("default", "numeric", "text", "coalesce"):
        raise HTTPException(status_code=400, detail="value_mode must be default, numeric, text, or coalesce")


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


@app.get("/browser", response_class=HTMLResponse)
def browser_ui() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Acquirium Browser</title>
  <style>
    body { font: 14px system-ui, sans-serif; margin: 24px; color: #1f2937; }
    main { max-width: 1100px; margin: 0 auto; }
    h1 { font-size: 20px; margin: 0 0 16px; }
    label { display: grid; gap: 4px; font-size: 12px; color: #4b5563; }
    select, input, button { font: inherit; padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 4px; background: white; }
    button { cursor: pointer; background: #f8fafc; }
    button:disabled { cursor: default; opacity: .5; }
    .row { display: flex; gap: 8px; align-items: end; flex-wrap: wrap; margin-bottom: 12px; }
    .grow { min-width: 260px; flex: 1; }
    canvas { width: 100%; height: 360px; border: 1px solid #cbd5e1; display: block; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; }
    th, td { padding: 6px 8px; border-bottom: 1px solid #e5e7eb; text-align: left; }
    th { font-size: 12px; color: #4b5563; }
    #status { min-height: 20px; color: #4b5563; }
  </style>
</head>
<body>
<main>
  <h1>Acquirium Browser</h1>
  <div class="row">
    <label class="grow">Source<select id="source"></select></label>
    <label class="grow">Search<input id="streamSearch" type="search" placeholder="ref name contains"></label>
    <label class="grow">Stream<select id="stream"></select></label>
  </div>
  <div class="row">
    <label>Start<input id="start" type="text" placeholder="-1h or ISO"></label>
    <label>End<input id="end" type="text" placeholder="now or ISO"></label>
    <label>Rows<input id="limit" type="number" min="1" max="100000" value="1000"></label>
    <label>Mode<select id="valueMode"><option>coalesce</option><option>default</option><option>numeric</option><option>text</option></select></label>
    <button id="load">Load</button>
    <button id="download" disabled>Download CSV</button>
  </div>
  <div id="status"></div>
  <canvas id="plot" width="1100" height="360"></canvas>
  <table>
    <thead><tr><th>timestamp</th><th>value</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
</main>
<script>
const sourceEl = document.querySelector("#source");
const streamEl = document.querySelector("#stream");
const streamSearchEl = document.querySelector("#streamSearch");
const statusEl = document.querySelector("#status");
const rowsEl = document.querySelector("#rows");
const canvas = document.querySelector("#plot");
const ctx = canvas.getContext("2d");
let currentRows = [];
let currentStreams = [];

async function getJSON(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function setStatus(text) { statusEl.textContent = text; }

function clearPlot() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function niceNumber(value) {
  if (!Number.isFinite(value)) return "";
  const abs = Math.abs(value);
  if (abs >= 1000 || (abs > 0 && abs < 0.01)) return value.toExponential(2);
  return String(Number(value.toPrecision(4)));
}

function timeLabel(ms) {
  const d = new Date(ms);
  return d.toISOString().replace("T", " ").slice(0, 16);
}

function drawPlot(rows) {
  clearPlot();
  const points = rows
    .map((r) => ({ x: r.ts, y: Number(r.value) }))
    .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y));
  ctx.strokeStyle = "#cbd5e1";
  ctx.fillStyle = "#4b5563";
  ctx.font = "12px system-ui, sans-serif";
  const left = 70;
  const top = 20;
  const right = canvas.width - 20;
  const bottom = canvas.height - 44;
  ctx.strokeRect(left, top, right - left, bottom - top);
  if (points.length < 2) return;
  const minX = Math.min(...points.map((p) => p.x));
  const maxX = Math.max(...points.map((p) => p.x));
  const minY = Math.min(...points.map((p) => p.y));
  const maxY = Math.max(...points.map((p) => p.y));
  const sx = (x) => left + ((x - minX) / (maxX - minX || 1)) * (right - left);
  const sy = (y) => top + (1 - (y - minY) / (maxY - minY || 1)) * (bottom - top);
  ctx.strokeStyle = "#e5e7eb";
  ctx.fillStyle = "#4b5563";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const value = minY + ((maxY - minY) * i) / 4;
    const y = sy(value);
    ctx.beginPath();
    ctx.moveTo(left - 4, y);
    ctx.lineTo(right, y);
    ctx.stroke();
    ctx.fillText(niceNumber(value), left - 8, y);
  }
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  for (let i = 0; i <= 4; i++) {
    const value = minX + ((maxX - minX) * i) / 4;
    const x = sx(value);
    ctx.beginPath();
    ctx.moveTo(x, bottom);
    ctx.lineTo(x, bottom + 4);
    ctx.stroke();
    ctx.fillText(timeLabel(value), x, bottom + 8);
  }
  ctx.strokeStyle = "#2563eb";
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((p, i) => {
    const x = sx(p.x);
    const y = sy(p.y);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function showRows(rows) {
  rowsEl.innerHTML = "";
  for (const row of rows.slice(0, 50)) {
    const tr = document.createElement("tr");
    const ts = document.createElement("td");
    const val = document.createElement("td");
    ts.textContent = Number.isFinite(row.ts) ? new Date(row.ts).toISOString() : "";
    val.textContent = row.value ?? "";
    tr.append(ts, val);
    rowsEl.append(tr);
  }
}

function csvEscape(value) {
  const text = value == null ? "" : String(value);
  return /[",\\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function downloadCSV() {
  const stream = currentStreams.find((item) => item.ref_uri === streamEl.value);
  const valueHeader = stream?.ref_name || "value";
  const lines = [`timestamp,${csvEscape(valueHeader)}`];
  for (const row of currentRows) {
    const ts = Number.isFinite(row.ts) ? new Date(row.ts).toISOString() : "";
    lines.push(`${csvEscape(ts)},${csvEscape(row.value)}`);
  }
  const blob = new Blob([lines.join("\\n") + "\\n"], { type: "text/csv" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "acquirium-stream.csv";
  a.click();
  URL.revokeObjectURL(a.href);
}

async function loadSources() {
  setStatus("Loading sources...");
  const body = await getJSON("/source");
  sourceEl.innerHTML = "";
  for (const source of body.sources) {
    const option = document.createElement("option");
    option.value = source.source_id;
    option.textContent = `${source.label || source.source_id} (${source.stream_count} streams)`;
    sourceEl.append(option);
  }
  await loadStreams();
}

async function loadStreams() {
  const sourceId = sourceEl.value;
  if (!sourceId) {
    setStatus("No sources");
    return;
  }
  setStatus("Loading streams...");
  const body = await getJSON(`/source/${encodeURIComponent(sourceId)}/streams?limit=1`);
  currentStreams = body.streams || [];
  streamSearchEl.value = "";
  renderStreams();
  setStatus(`${body.count} streams`);
}

function renderStreams() {
  const needle = streamSearchEl.value.trim().toLowerCase();
  const streams = currentStreams.filter((stream) => String(stream.ref_name || "").toLowerCase().includes(needle));
  streamEl.innerHTML = "";
  for (const stream of streams) {
    const option = document.createElement("option");
    option.value = stream.ref_uri;
    option.textContent = `${stream.ref_name} (${stream.row_count} rows)`;
    streamEl.append(option);
  }
  setStatus(`${streams.length} of ${currentStreams.length} streams`);
}

async function loadData() {
  const refUri = streamEl.value;
  if (!refUri) return;
  const limit = document.querySelector("#limit").value || "1000";
  const valueMode = document.querySelector("#valueMode").value;
  const start = document.querySelector("#start").value.trim();
  const end = document.querySelector("#end").value.trim();
  setStatus("Loading data...");
  const params = new URLSearchParams({
    ref_uri: refUri,
    limit,
    order: "asc",
    format: "columns",
    ts_format: "unix_ms",
    value_mode: valueMode,
  });
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  const url = `/streams/data?${params.toString()}`;
  const body = await getJSON(url);
  const ts = body.data.ts || [];
  const values = body.data.value || [];
  currentRows = ts.map((t, i) => ({ ts: t, value: values[i] }));
  drawPlot(currentRows);
  showRows(currentRows);
  document.querySelector("#download").disabled = currentRows.length === 0;
  setStatus(`${currentRows.length} rows loaded`);
}

sourceEl.addEventListener("change", loadStreams);
streamSearchEl.addEventListener("input", renderStreams);
document.querySelector("#load").addEventListener("click", loadData);
document.querySelector("#download").addEventListener("click", downloadCSV);
loadSources().catch((err) => setStatus(err.message));
</script>
</body>
</html>"""


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
    value_mode: str = "default",
) -> dict[str, Any]:
    source_id = _decoded_source_id(source_id)
    _validate_stream_data_options(
        limit=limit,
        order=order,
        format=format,
        ts_format=ts_format,
        value_mode=value_mode,
    )
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
        value_mode=value_mode,
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
    value_mode: str = "default",
) -> dict[str, Any]:
    _validate_stream_data_options(
        limit=limit,
        order=order,
        format=format,
        ts_format=ts_format,
        value_mode=value_mode,
    )
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
        value_mode=value_mode,
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
        "value_mode": value_mode,
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
    value_mode: str = "default",
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
        value_mode=value_mode,
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
    value_mode: str = "default",
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
        value_mode=value_mode,
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
    value_mode: str = "default",
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
        value_mode=value_mode,
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
