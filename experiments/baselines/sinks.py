"""Sink adapters: the same replay batches delivered to each baseline system.

Each adapter takes the rows the replay produces, ``(ts, sid, value, previous)``
where ``previous`` is the value a correction replaces (None for a new
reading), and expresses them in the system's own terms. Each also builds
the six views in that system's SQL, reads the view contents back as
``(view -> {sid or None -> frame(time, value)})`` for the oracle, and
reports CPU from the container's cgroup accounting plus whatever engine
counters the system exposes.

Column names avoid ``stream`` and ``value``, both reserved in Calcite-based
SQL (Flink, Feldera): a reading is ``(ts, sid, kind, val)``. ``kind`` is
``conc``, ``flow``, ``ph`` or ``other``, derived from the point's quantity
kind in the plant model, since the baselines have no graph to query.
"""
from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import polars as pl

THRESHOLD = 60.0
EMPTY = pl.DataFrame({"time": pl.Series([], dtype=pl.Datetime("us", "UTC")), "value": pl.Series([], dtype=pl.Float64)})
VIEWS = ["v1", "v2", "v3", "v4", "v5", "v6"]
PER_STREAM = {"v1", "v2", "v3", "v6"}

Rows = list[tuple[datetime, str, float, float | None]]


def cgroup_cpu_seconds(container: str) -> float:
    """CPU seconds the container's cgroup has consumed so far (cgroup v2)."""
    out = subprocess.run(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"],
                         capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6
    raise RuntimeError(f"no usage_usec in cpu.stat of {container}")


def process_cpu_seconds(pid: int) -> float:
    """CPU seconds of one process and its threads, from /proc (Linux only)."""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / ticks  # utime, stime


def _frame(rows: list[tuple[Any, Any]], text: bool = False) -> pl.DataFrame:
    if not rows:
        return EMPTY if not text else pl.DataFrame({"time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                                                     "value": pl.Series([], dtype=pl.Utf8)})
    times = []
    for t, _ in rows:
        if isinstance(t, str):
            t = datetime.fromisoformat(t.replace(" ", "T"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        times.append(t.astimezone(timezone.utc))
    values = [v for _, v in rows]
    frame = pl.DataFrame({"time": times, "value": values if text else [float(v) for v in values]})
    return frame.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC"))).sort("time")


def _group(view: str, rows: list[tuple[Any, Any, Any]]) -> dict[str | None, pl.DataFrame]:
    """Rows of (sid, ts, val) into the oracle's shape."""
    text = view == "v3"
    if view in PER_STREAM:
        by_sid: dict[str | None, list] = {}
        for sid, ts, val in rows:
            by_sid.setdefault(sid, []).append((ts, val))
        return {sid: _frame(items, text) for sid, items in by_sid.items()}
    return {None: _frame([(ts, val) for _, ts, val in rows], text)}


# ---------------------------------------------------------------------------
# TimescaleDB: continuous aggregates where SQL allows, materialized views elsewhere
# ---------------------------------------------------------------------------

TIMESCALE_SCHEMA = """
DROP SCHEMA IF EXISTS siv CASCADE; CREATE SCHEMA siv; SET search_path = siv, public;
CREATE TABLE readings (ts timestamptz NOT NULL, sid text NOT NULL, kind text NOT NULL, val double precision, PRIMARY KEY (sid, ts));
SELECT create_hypertable('readings', 'ts');
-- V1, V5 (per-stream half), V6: continuous aggregates, refreshed incrementally from the invalidation log.
CREATE MATERIALIZED VIEW v1 WITH (timescaledb.continuous) AS SELECT sid, time_bucket('5 min', ts) AS ts, avg(val) AS val FROM readings WHERE kind = 'conc' GROUP BY 1, 2 WITH NO DATA;
CREATE MATERIALIZED VIEW v5s WITH (timescaledb.continuous) AS SELECT sid, time_bucket('5 min', ts) AS ts, avg(val) AS val FROM readings WHERE kind = 'flow' GROUP BY 1, 2 WITH NO DATA;
CREATE MATERIALIZED VIEW v6 WITH (timescaledb.continuous) AS SELECT sid, time_bucket('1 day', ts) AS ts, max(val) - min(val) AS val FROM readings WHERE kind = 'ph' GROUP BY 1, 2 WITH NO DATA;
-- V2 needs a window function, which continuous aggregates do not allow: a plain materialized view, fully recomputed on refresh.
CREATE MATERIALIZED VIEW v2 AS SELECT sid, ts, avg(val) OVER (PARTITION BY sid ORDER BY ts RANGE BETWEEN interval '59 min' PRECEDING AND CURRENT ROW) AS val FROM v1;
CREATE VIEW v3 AS SELECT sid, ts, 'high'::text AS val FROM v2 WHERE val > {threshold};
CREATE MATERIALIZED VIEW v4 AS SELECT time_bucket('1 hour', ts) AS ts, count(*)::double precision AS val FROM v3 GROUP BY 1;
CREATE VIEW v5 AS SELECT ts, sum(val) AS val FROM v5s GROUP BY ts;
"""

TIMESCALE_REFRESH = [
    "CALL refresh_continuous_aggregate('siv.v1', NULL, NULL)",
    "CALL refresh_continuous_aggregate('siv.v5s', NULL, NULL)",
    "CALL refresh_continuous_aggregate('siv.v6', NULL, NULL)",
    "REFRESH MATERIALIZED VIEW siv.v2",
    "REFRESH MATERIALIZED VIEW siv.v4",
]


class TimescaleSink:
    name = "timescale"
    container = "siv-timescale"

    def __init__(self, dsn: str, kinds: dict[str, str], *, refresh_every: int = 1) -> None:
        import psycopg
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.kinds = kinds
        self.refresh_every = refresh_every
        self.batches = 0

    def reset(self) -> None:
        with self.conn.cursor() as cur:
            for statement in TIMESCALE_SCHEMA.format(threshold=THRESHOLD).split(";"):
                if statement.strip():
                    cur.execute(statement)
        self.batches = 0

    def write(self, rows: Rows, kind: str) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO siv.readings (ts, sid, kind, val) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (sid, ts) DO UPDATE SET val = EXCLUDED.val",
                [(ts, sid, self.kinds.get(sid, "other"), val) for ts, sid, val, _ in rows])
        self.batches += 1
        if self.batches % self.refresh_every == 0:
            self.refresh()

    def refresh(self) -> None:
        with self.conn.cursor() as cur:
            for statement in TIMESCALE_REFRESH:
                cur.execute(statement)

    def finish(self) -> None:
        self.refresh()

    def outputs(self) -> dict[str, dict[str | None, pl.DataFrame]]:
        out = {}
        with self.conn.cursor() as cur:
            for view in VIEWS:
                if view in PER_STREAM:
                    cur.execute(f"SELECT sid, ts, val FROM siv.{view} ORDER BY sid, ts")
                    out[view] = _group(view, cur.fetchall())
                else:
                    cur.execute(f"SELECT ts, val FROM siv.{view} ORDER BY ts")
                    out[view] = _group(view, [(None, ts, val) for ts, val in cur.fetchall()])
        return out

    def counters(self) -> dict[str, float]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0) FROM pg_stat_all_tables "
                        "WHERE schemaname IN ('siv', '_timescaledb_internal')")
            written = float(cur.fetchone()[0])
        return {"rows_written": written, "cpu_container_seconds": cgroup_cpu_seconds(self.container)}

    def output_rows(self) -> int:
        return sum(frame.height for view in self.outputs().values() for frame in view.values())


# ---------------------------------------------------------------------------
# Feldera: one pipeline, six materialized views, insert/delete envelopes
# ---------------------------------------------------------------------------

FELDERA_PROGRAM = """
CREATE TABLE readings (ts TIMESTAMP NOT NULL, sid VARCHAR NOT NULL, kind VARCHAR NOT NULL, val DOUBLE) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW v1 AS SELECT sid, window_start AS ts, AVG(val) AS val FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL 5 MINUTES)) WHERE kind = 'conc' GROUP BY sid, window_start;
CREATE MATERIALIZED VIEW v2 AS SELECT sid, ts, AVG(val) OVER (PARTITION BY sid ORDER BY ts RANGE BETWEEN INTERVAL 59 MINUTES PRECEDING AND CURRENT ROW) AS val FROM v1;
CREATE MATERIALIZED VIEW v3 AS SELECT sid, ts, 'high' AS val FROM v2 WHERE val > {threshold};
CREATE MATERIALIZED VIEW v4 AS SELECT window_start AS ts, COUNT(*) AS val FROM TABLE(TUMBLE(TABLE v3, DESCRIPTOR(ts), INTERVAL 1 HOUR)) GROUP BY window_start;
CREATE MATERIALIZED VIEW v5 AS SELECT ts, SUM(val) AS val FROM (SELECT sid, window_start AS ts, AVG(val) AS val FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL 5 MINUTES)) WHERE kind = 'flow' GROUP BY sid, window_start) GROUP BY ts;
CREATE MATERIALIZED VIEW v6 AS SELECT sid, window_start AS ts, MAX(val) - MIN(val) AS val FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL 1 DAY)) WHERE kind = 'ph' GROUP BY sid, window_start;
"""


class FelderaSink:
    name = "feldera"
    container = "siv-feldera"

    def __init__(self, base: str, kinds: dict[str, str], *, pipeline: str = "siv", workers: int = 4) -> None:
        self.base = base.rstrip("/") + "/v0"
        self.kinds = kinds
        self.pipeline = pipeline
        self.workers = workers
        self.sent = 0

    def _call(self, method: str, path: str, body: Any = None, *, raw: bool = False, ctype: str = "application/json"):
        data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data=data, method=method, headers={"Content-Type": ctype})
        try:
            with urllib.request.urlopen(request, timeout=600) as response:
                text = response.read().decode()
                return response.status, (text if raw else (json.loads(text) if text else None))
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()

    def _info(self) -> dict:
        code, info = self._call("GET", f"/pipelines/{self.pipeline}")
        return info if code == 200 and isinstance(info, dict) else {}

    def _wait(self, predicate: Callable[[dict], bool], timeout: float, what: str) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            info = self._info()
            if predicate(info):
                return info
            time.sleep(1)
        raise TimeoutError(f"feldera: {what} did not happen within {timeout:.0f}s; last status "
                           f"{self._info().get('program_status')}/{self._info().get('deployment_status')}")

    def reset(self) -> None:
        if self._info():
            self._call("POST", f"/pipelines/{self.pipeline}/stop?force=true")
            self._wait(lambda i: not i or i.get("deployment_status") == "Stopped", 120, "stop")
            self._call("POST", f"/pipelines/{self.pipeline}/clear")
            self._wait(lambda i: not i or i.get("storage_status") == "Cleared", 120, "clear")
            for _ in range(30):
                if self._call("DELETE", f"/pipelines/{self.pipeline}")[0] in (200, 204, 404):
                    break
                time.sleep(2)
        code, body = self._call("PUT", f"/pipelines/{self.pipeline}", {
            "name": self.pipeline, "program_code": FELDERA_PROGRAM.format(threshold=THRESHOLD),
            "runtime_config": {"workers": self.workers}})
        if code not in (200, 201):
            raise RuntimeError(f"feldera: create failed {code} {body}")
        info = self._wait(lambda i: i.get("program_status") not in ("Pending", "CompilingSql", "SqlCompiled", "CompilingRust"),
                          900, "compilation")
        if info.get("program_status") != "Success":
            raise RuntimeError(f"feldera: compilation failed: {json.dumps(info.get('program_error'))[:800]}")
        self._call("POST", f"/pipelines/{self.pipeline}/start")
        self._wait(lambda i: i.get("deployment_status") == "Running", 300, "start")
        self.sent = 0

    def write(self, rows: Rows, kind: str) -> None:
        lines = []
        for ts, sid, val, previous in rows:
            record = {"ts": ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), "sid": sid,
                      "kind": self.kinds.get(sid, "other")}
            if previous is not None:
                lines.append(json.dumps({"delete": {**record, "val": previous}}))
            lines.append(json.dumps({"insert": {**record, "val": val}}))
        code, body = self._call("POST", f"/pipelines/{self.pipeline}/ingress/readings?format=json&update_format=insert_delete",
                                "\n".join(lines).encode())
        if code != 200:
            raise RuntimeError(f"feldera: ingress failed {code} {str(body)[:300]}")
        self.sent += len(lines)

    def stats(self) -> dict:
        code, stats = self._call("GET", f"/pipelines/{self.pipeline}/stats")
        return stats.get("global_metrics", {}) if code == 200 and isinstance(stats, dict) else {}

    def finish(self, timeout: float = 120) -> None:
        """Wait until the pipeline has processed everything it was sent."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            metrics = self.stats()
            if "total_input_records" not in metrics:
                time.sleep(3)  # unknown metric names on this version: give the pipeline a moment
                return
            if metrics.get("buffered_input_records", 0) == 0 and \
               metrics.get("total_processed_records", 0) >= metrics.get("total_input_records", 0):
                return
            time.sleep(0.5)

    def query(self, sql: str) -> list[dict]:
        code, text = self._call("GET", f"/pipelines/{self.pipeline}/query?sql={urllib.parse.quote(sql)}&format=json", raw=True)
        if code != 200:
            raise RuntimeError(f"feldera: query failed {code} {text[:300]}")
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def outputs(self) -> dict[str, dict[str | None, pl.DataFrame]]:
        out = {}
        for view in VIEWS:
            rows = self.query(f"SELECT * FROM {view}")
            if view in PER_STREAM:
                out[view] = _group(view, [(r["sid"], r["ts"], r["val"]) for r in rows])
            else:
                out[view] = _group(view, [(None, r["ts"], r["val"]) for r in rows])
        return out

    def counters(self) -> dict[str, float]:
        metrics = self.stats()
        return {"cpu_engine_seconds": metrics.get("cpu_msecs", 0) / 1000.0,
                "records_processed": float(metrics.get("total_processed_records", 0)),
                "cpu_container_seconds": cgroup_cpu_seconds(self.container)}

    def output_rows(self) -> int:
        return sum(int(self.query(f"SELECT COUNT(*) AS n FROM {view}")[0]["n"]) for view in VIEWS)


# ---------------------------------------------------------------------------
# Feldera as a pipeline: base data and views durable in TimescaleDB
# ---------------------------------------------------------------------------

PIPELINE_SCHEMA = """
DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; SET search_path = {schema}, public;
CREATE TABLE readings (ts timestamptz NOT NULL, sid text NOT NULL, kind text NOT NULL, val double precision, PRIMARY KEY (sid, ts));
SELECT create_hypertable('readings', 'ts');
CREATE TABLE v1 (sid text NOT NULL, ts timestamptz NOT NULL, val double precision, PRIMARY KEY (sid, ts));
CREATE TABLE v2 (sid text NOT NULL, ts timestamptz NOT NULL, val double precision, PRIMARY KEY (sid, ts));
CREATE TABLE v3 (sid text NOT NULL, ts timestamptz NOT NULL, val text, PRIMARY KEY (sid, ts));
CREATE TABLE v4 (ts timestamptz NOT NULL, val double precision, PRIMARY KEY (ts));
CREATE TABLE v5 (ts timestamptz NOT NULL, val double precision, PRIMARY KEY (ts));
CREATE TABLE v6 (sid text NOT NULL, ts timestamptz NOT NULL, val double precision, PRIMARY KEY (sid, ts));
"""


class FelderaPipelineSink:
    """Feldera the way it is deployed: readings land in a database, Feldera
    computes the view deltas, a writer applies them back to the database, and
    users read the views from the database. Each batch is written to the
    TimescaleDB base table and to Feldera's ingress; a separate writer
    process (`feldera_writer.py`) follows the egress of every view and
    applies inserts and deletes to the view tables. Cost is the engine, the
    database container and the writer process."""
    name = "feldera"
    container = "siv-feldera"
    db_container = "siv-timescale"

    def __init__(self, base: str, dsn: str, kinds: dict[str, str], *, schema: str = "siv_fp",
                 pipeline: str = "siv", workers: int = 4, status_dir: Path | None = None) -> None:
        import psycopg
        self.engine = FelderaSink(base, kinds, pipeline=pipeline, workers=workers)
        self.base = base
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.kinds = kinds
        self.schema = schema
        self.status_path = str((status_dir or Path("/tmp")) / f"feldera-writer-{os.getpid()}.json")
        self.writer: subprocess.Popen | None = None

    def _status(self) -> dict:
        try:
            with open(self.status_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def reset(self) -> None:
        self.stop()
        self.engine.reset()
        with self.conn.cursor() as cur:
            for statement in PIPELINE_SCHEMA.format(schema=self.schema).split(";"):
                if statement.strip():
                    cur.execute(statement)
        if os.path.exists(self.status_path):
            os.remove(self.status_path)
        import sys
        self.writer = subprocess.Popen(
            [sys.executable, "-m", "experiments.baselines.feldera_writer", "--feldera", self.base,
             "--pipeline", self.engine.pipeline, "--dsn", self.dsn, "--schema", self.schema, "--status", self.status_path])
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = self._status()
            if status and all(v.get("subscribed") for v in status.values()):
                return
            if self.writer.poll() is not None:
                raise RuntimeError(f"feldera writer exited with {self.writer.returncode}")
            time.sleep(0.2)
        raise TimeoutError("feldera writer did not subscribe to every view within 60 s")

    def write(self, rows: Rows, kind: str) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {self.schema}.readings (ts, sid, kind, val) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (sid, ts) DO UPDATE SET val = EXCLUDED.val",
                [(ts, sid, self.kinds.get(sid, "other"), val) for ts, sid, val, _ in rows])
        self.engine.write(rows, kind)

    def _table_counts(self) -> dict[str, int]:
        with self.conn.cursor() as cur:
            counts = {}
            for view in VIEWS:
                cur.execute(f"SELECT count(*) FROM {self.schema}.{view}")
                counts[view] = int(cur.fetchone()[0])
        return counts

    def finish(self, timeout: float = 180) -> None:
        """Wait for the engine to drain, then for the writer to catch up with it."""
        self.engine.finish()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            engine = {view: int(self.engine.query(f"SELECT COUNT(*) AS n FROM {view}")[0]["n"]) for view in VIEWS}
            if self._table_counts() == engine:
                return
            time.sleep(0.5)
        raise TimeoutError(f"feldera writer did not catch up: db {self._table_counts()} engine {engine}")

    def outputs(self) -> dict[str, dict[str | None, pl.DataFrame]]:
        out = {}
        with self.conn.cursor() as cur:
            for view in VIEWS:
                if view in PER_STREAM:
                    cur.execute(f"SELECT sid, ts, val FROM {self.schema}.{view} ORDER BY sid, ts")
                    out[view] = _group(view, cur.fetchall())
                else:
                    cur.execute(f"SELECT ts, val FROM {self.schema}.{view} ORDER BY ts")
                    out[view] = _group(view, [(None, ts, val) for ts, val in cur.fetchall()])
        return out

    def counters(self) -> dict[str, float]:
        engine = self.engine.counters()
        with self.conn.cursor() as cur:
            cur.execute("SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0) FROM pg_stat_all_tables "
                        "WHERE schemaname = %s AND relname <> 'readings'", (self.schema,))
            written = float(cur.fetchone()[0])
        writer = process_cpu_seconds(self.writer.pid) if self.writer and self.writer.poll() is None else 0.0
        db = cgroup_cpu_seconds(self.db_container)
        return {"rows_written": written, "cpu_engine_seconds": engine["cpu_engine_seconds"],
                "cpu_feldera_container_seconds": engine["cpu_container_seconds"], "cpu_writer_seconds": writer,
                "cpu_db_seconds": db, "cpu_process_seconds": engine["cpu_engine_seconds"] + writer,
                "cpu_seconds": engine["cpu_engine_seconds"] + writer + db,
                "records_processed": engine["records_processed"]}

    def output_rows(self) -> int:
        return sum(self._table_counts().values())

    def stop(self) -> None:
        if self.writer and self.writer.poll() is None:
            self.writer.terminate()
            try:
                self.writer.wait(10)
            except subprocess.TimeoutExpired:
                self.writer.kill()
        self.writer = None


# ---------------------------------------------------------------------------
# Flink SQL: filesystem source and sinks shared through a mounted directory
# ---------------------------------------------------------------------------

FLINK_SINK_OPTIONS = ("'connector' = 'filesystem', 'format' = 'csv', "
                      "'sink.rolling-policy.rollover-interval' = '10 s', 'sink.rolling-policy.check-interval' = '2 s'")


def flink_statements(lateness_minutes: int, root: str = "/data") -> list[str]:
    sinks = {"v1": "sid STRING, ts TIMESTAMP(3), val DOUBLE", "v2": "sid STRING, ts TIMESTAMP(3), val DOUBLE",
             "v3": "sid STRING, ts TIMESTAMP(3), val STRING", "v4": "ts TIMESTAMP(3), val BIGINT",
             "v5": "ts TIMESTAMP(3), val DOUBLE", "v6": "sid STRING, ts TIMESTAMP(3), val DOUBLE"}
    statements = ["DROP TABLE IF EXISTS readings"] + [f"DROP TABLE IF EXISTS {v}_out" for v in VIEWS] + \
                 [f"DROP VIEW IF EXISTS {v}" for v in VIEWS + ["v5s"]]
    statements.append(
        f"CREATE TABLE readings (ts TIMESTAMP(3), sid STRING, kind STRING, val DOUBLE, "
        f"WATERMARK FOR ts AS ts - INTERVAL '{lateness_minutes}' MINUTE) WITH ('connector' = 'filesystem', "
        f"'path' = '{root}/in', 'format' = 'csv', 'source.monitor-interval' = '100 ms')")
    statements += [f"CREATE TABLE {v}_out ({cols}) WITH ({FLINK_SINK_OPTIONS}, 'path' = '{root}/out/{v}')"
                   for v, cols in sinks.items()]
    statements += [
        "CREATE VIEW v1 AS SELECT sid, window_start AS ts, window_time AS wt, AVG(val) AS val "
        "FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL '5' MINUTES)) WHERE kind = 'conc' "
        "GROUP BY sid, window_start, window_end, window_time",
        "CREATE VIEW v2 AS SELECT sid, ts, wt, AVG(val) OVER (PARTITION BY sid ORDER BY wt "
        "RANGE BETWEEN INTERVAL '59' MINUTE PRECEDING AND CURRENT ROW) AS val FROM v1",
        f"CREATE VIEW v3 AS SELECT sid, ts, wt, 'high' AS val FROM v2 WHERE val > {THRESHOLD}",
        "CREATE VIEW v4 AS SELECT window_start AS ts, COUNT(*) AS val "
        "FROM TABLE(TUMBLE(TABLE v3, DESCRIPTOR(wt), INTERVAL '1' HOUR)) GROUP BY window_start, window_end",
        "CREATE VIEW v5s AS SELECT sid, window_start AS ts, window_time AS wt, AVG(val) AS val "
        "FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL '5' MINUTES)) WHERE kind = 'flow' "
        "GROUP BY sid, window_start, window_end, window_time",
        "CREATE VIEW v5 AS SELECT window_start AS ts, SUM(val) AS val "
        "FROM TABLE(TUMBLE(TABLE v5s, DESCRIPTOR(wt), INTERVAL '5' MINUTES)) GROUP BY window_start, window_end",
        "CREATE VIEW v6 AS SELECT sid, window_start AS ts, MAX(val) - MIN(val) AS val "
        "FROM TABLE(TUMBLE(TABLE readings, DESCRIPTOR(ts), INTERVAL '1' DAY)) WHERE kind = 'ph' "
        "GROUP BY sid, window_start, window_end",
    ]
    return statements


FLINK_JOB = ("EXECUTE STATEMENT SET BEGIN "
             "INSERT INTO v1_out SELECT sid, ts, val FROM v1; INSERT INTO v2_out SELECT sid, ts, val FROM v2; "
             "INSERT INTO v3_out SELECT sid, ts, val FROM v3; INSERT INTO v4_out SELECT ts, val FROM v4; "
             "INSERT INTO v5_out SELECT ts, val FROM v5; INSERT INTO v6_out SELECT sid, ts, val FROM v6; END")


class FlinkSink:
    """Flink has no key on readings: a correction arrives as one more row for
    the same timestamp, and a reading later than the watermark allows is
    dropped. Both are reported as mismatches against the oracle."""
    name = "flink"
    containers = ("siv-flink-jobmanager", "siv-flink-taskmanager")

    def __init__(self, gateway: str, rest: str, data_dir: Path, kinds: dict[str, str], *,
                 lateness_minutes: int = 10, parallelism: int = 1) -> None:
        # One reader: with several, files are consumed out of order, the
        # watermark runs ahead of unread batches and their rows are dropped
        # as late, which would charge Flink for an artifact of the harness.
        self.gateway = gateway.rstrip("/") + "/v1"
        self.rest = rest.rstrip("/")
        self.data = Path(data_dir)
        self.kinds = kinds
        self.lateness = lateness_minutes
        self.parallelism = parallelism
        self.session: str | None = None
        self.batches = 0
        self.last_ts: datetime | None = None
        self.root = self.data  # one fresh subdirectory per reset, so an old job cannot pollute it

    def _call(self, method: str, path: str, body: Any = None) -> dict:
        request = urllib.request.Request(self.gateway + path, data=json.dumps(body).encode() if body is not None else None,
                                         method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                return json.loads(response.read().decode() or "{}")
        except urllib.error.HTTPError as error:
            return {"error": error.code, "body": error.read().decode()[:600]}

    def sql(self, statement: str) -> list:
        op = self._call("POST", f"/sessions/{self.session}/statements", {"statement": statement})
        if "operationHandle" not in op:
            raise RuntimeError(f"flink: statement rejected: {statement[:80]}: {op}")
        handle = op["operationHandle"]
        for _ in range(3000):
            status = self._call("GET", f"/sessions/{self.session}/operations/{handle}/status").get("status")
            if status in ("FINISHED", "ERROR", "CANCELED"):
                break
            time.sleep(0.2)
        if status != "FINISHED":
            raise RuntimeError(f"flink: statement {status}: {statement[:120]} (see docker logs siv-flink-sql-gateway)")
        result = self._call("GET", f"/sessions/{self.session}/operations/{handle}/result/0?rowFormat=JSON")
        return [row.get("fields") for row in (result.get("results") or {}).get("data", [])] if "error" not in result else []

    def _jobs(self, states=("RUNNING", "CREATED", "RESTARTING", "CANCELLING")) -> list[dict]:
        jobs = json.loads(urllib.request.urlopen(self.rest + "/jobs/overview").read())["jobs"]
        return [job for job in jobs if job["state"] in states]

    def reset(self) -> None:
        for job in self._jobs():
            urllib.request.urlopen(urllib.request.Request(f"{self.rest}/jobs/{job['jid']}", method="PATCH"))
        for _ in range(60):
            if not self._jobs():
                break
            time.sleep(1)
        else:
            raise RuntimeError("flink: previous jobs did not cancel")
        # Flink's files belong to the container's user; remove them from inside.
        subprocess.run(["docker", "exec", self.containers[1], "sh", "-c", "rm -rf /data/run-*"], check=False)
        for old in self.data.glob("run-*"):
            shutil.rmtree(old, ignore_errors=True)
        stamp = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S")
        self.root = self.data / stamp
        for sub in ("in", "out"):
            (self.root / sub).mkdir(parents=True, exist_ok=True)
            os.chmod(self.root / sub, 0o777)
        os.chmod(self.root, 0o777)
        self.session = self._call("POST", "/sessions", {"properties": {
            "execution.runtime-mode": "streaming", "execution.checkpointing.interval": "5 s",
            "table.exec.source.idle-timeout": "5 s", "parallelism.default": str(self.parallelism)}})["sessionHandle"]
        for statement in flink_statements(self.lateness, f"/data/{stamp}"):
            self.sql(statement)
        self.sql(FLINK_JOB)
        self.batches = 0
        self.last_ts = None

    FILE_SPACING = 0.25  # seconds between batch files: one new file per discovery scan

    def write(self, rows: Rows, kind: str) -> None:
        # The file source enumerates new files in no particular order, so two
        # batches discovered in one scan can be read newest first, advance the
        # watermark, and turn the older batch's rows into dropped late data.
        # Spacing the files out keeps delivery in order; it costs wall time,
        # not CPU, and the wall time of this adapter is reported as such.
        if self.batches:
            time.sleep(self.FILE_SPACING)
        self.batches += 1
        path = self.root / "in" / f".batch-{self.batches:06d}.csv"
        with path.open("w") as f:
            for ts, sid, val, _ in rows:
                stamp = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                f.write(f"{stamp},{sid},{self.kinds.get(sid, 'other')},{val!r}\n")
                self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)
        os.rename(path, path.with_name(path.name[1:]))

    def finish(self, settle_seconds: float = 45) -> None:
        """Advance the watermark past every window, then let the sinks roll their files."""
        marker = (self.last_ts or datetime(2025, 1, 1, tzinfo=timezone.utc)) + timedelta(days=1, minutes=self.lateness + 5)
        self.write([(marker, "marker", 0.0, None)], "marker")
        time.sleep(settle_seconds)

    def outputs(self) -> dict[str, dict[str | None, pl.DataFrame]]:
        out = {}
        for view in VIEWS:
            rows = []
            for file in sorted(glob.glob(str(self.root / "out" / view / "*"))):
                if "inprogress" in file or os.path.basename(file).startswith("."):
                    continue
                with open(file) as f:
                    for line in f:
                        parts = [part.strip('"') for part in line.rstrip("\n").split(",")]
                        if view in PER_STREAM:
                            sid, ts, val = parts[0], parts[1], parts[2]
                        else:
                            sid, ts, val = None, parts[0], parts[1]
                        rows.append((sid, ts, val if view == "v3" else float(val)))
            out[view] = _group(view, rows)
        return out

    def counters(self) -> dict[str, float]:
        return {"cpu_container_seconds": sum(cgroup_cpu_seconds(c) for c in self.containers)}

    def output_rows(self) -> int:
        return sum(frame.height for view in self.outputs().values() for frame in view.values())
