"""
Dummy alert receiver for Acquirium trigger outputs.

Runs a small HTTP server that listens on localhost:10000 and prints
incoming POST /alerts payloads to stdout.
"""

from __future__ import annotations

import argparse
import datetime
import json
from typing import Any, Dict, Tuple, Union

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

bench_file = "scripts/benchmark/scalability/results_100_2.txt"

app = FastAPI()


def _parse_json_body(raw: bytes) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        decoded = raw.decode("utf-8")
        return json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"_raw": raw.decode("utf-8", errors="replace")}


def _parse_iso(ts: Any) -> Union[datetime.datetime, None]:
    if not isinstance(ts, str):
        return None
    try:
        return datetime.datetime.fromisoformat(ts)
    except ValueError:
        return None


@app.post("/alerts")
async def alerts(request: Request) -> JSONResponse:
    raw = await request.body()
    payload = _parse_json_body(raw)

    data_ts_raw = (
        payload.get("message", {})
        .get("data", {})
        .get("timestamp", "no_timestamp")
    )
    msg_ts_raw = payload.get("ts", "no_timestamp")

    data_ts = _parse_iso(data_ts_raw)
    msg_ts = _parse_iso(msg_ts_raw)
    # print(f"Received alert at {msg_ts}: {payload}")

    if data_ts is not None and msg_ts is not None:
        latency_ms = (msg_ts - data_ts).total_seconds() * 1000.0
        with open(bench_file, "a", encoding="utf-8") as f:
            f.write(f"{latency_ms}\n")

    return JSONResponse({"status": "ok"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy Acquirium alert receiver (FastAPI)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10000)
    args = parser.parse_args()

    print(f"Alert receiver listening on http://{args.host}:{args.port}/alerts")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
