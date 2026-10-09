"""Apply Feldera's view deltas to TimescaleDB tables; one process, one thread per view.

Feldera emits each view as a change stream: after every step, the rows the
step inserted into and deleted from the view. This process subscribes to
the egress of every view before the replay starts and applies each chunk to
the corresponding table in one transaction, deletes first, then upserts, so
the tables always hold a consistent step. It is a separate process so that
its CPU can be charged to the Feldera pipeline alongside the engine and the
database.

    python -m experiments.baselines.feldera_writer --feldera http://127.0.0.1:8085 \\
        --pipeline siv --dsn postgresql://... --schema siv_fp --status /tmp/writer.json
"""
from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.request
from datetime import datetime, timezone

KEYS = {"v1": ("sid", "ts"), "v2": ("sid", "ts"), "v3": ("sid", "ts"), "v4": ("ts",), "v5": ("ts",), "v6": ("sid", "ts")}


def _ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace(" ", "T")).replace(tzinfo=timezone.utc)


class Writer:
    def __init__(self, feldera: str, pipeline: str, dsn: str, schema: str, status_path: str) -> None:
        self.base = feldera.rstrip("/") + "/v0"
        self.pipeline = pipeline
        self.dsn = dsn
        self.schema = schema
        self.status_path = status_path
        self.lock = threading.Lock()
        self.status = {view: {"subscribed": False, "chunks": 0, "inserted": 0, "deleted": 0, "sequence": -1}
                       for view in KEYS}

    def _write_status(self) -> None:
        with self.lock:
            tmp = self.status_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.status, f)
        import os
        os.replace(tmp, self.status_path)

    def apply(self, conn, view: str, changes: list[dict]) -> tuple[int, int]:
        key = KEYS[view]
        deletes, inserts = [], []
        for change in changes:
            if "delete" in change:
                row = change["delete"]
                deletes.append(tuple(_ts(row[c]) if c == "ts" else row[c] for c in key))
            elif "insert" in change:
                row = change["insert"]
                values = [row.get("sid"), _ts(row["ts"]), row["val"]] if "sid" in key else [_ts(row["ts"]), row["val"]]
                inserts.append(tuple(values))
        columns = ("sid, ts, val" if "sid" in key else "ts, val")
        table = f"{self.schema}.{view}"
        with conn.transaction(), conn.cursor() as cur:
            if deletes:
                where = " AND ".join(f"{c} = %s" for c in key)
                cur.executemany(f"DELETE FROM {table} WHERE {where}", deletes)
            if inserts:
                placeholders = ", ".join("%s" for _ in columns.split(", "))
                cur.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
                                f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET val = EXCLUDED.val", inserts)
        return len(inserts), len(deletes)

    def follow(self, view: str) -> None:
        import psycopg
        conn = psycopg.connect(self.dsn, autocommit=True, options="-c timezone=UTC")
        request = urllib.request.Request(f"{self.base}/pipelines/{self.pipeline}/egress/{view}?format=json",
                                         data=b"", method="POST")
        with urllib.request.urlopen(request) as response:
            for raw in response:
                line = raw.decode().strip()
                if not line:
                    continue
                chunk = json.loads(line)
                state = self.status[view]
                if not state["subscribed"]:
                    state["subscribed"] = True
                    self._write_status()
                changes = chunk.get("json_data") or []
                if changes:
                    inserted, deleted = self.apply(conn, view, changes)
                    state["inserted"] += inserted
                    state["deleted"] += deleted
                state["chunks"] += 1
                state["sequence"] = chunk.get("sequence_number", state["sequence"])
                self._write_status()

    def run(self) -> None:
        threads = [threading.Thread(target=self.follow, args=(view,), daemon=True, name=view) for view in KEYS]
        for thread in threads:
            thread.start()
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feldera", required=True)
    parser.add_argument("--pipeline", default="siv")
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", default="siv_fp")
    parser.add_argument("--status", required=True)
    args = parser.parse_args(argv)
    Writer(args.feldera, args.pipeline, args.dsn, args.schema, args.status).run()


if __name__ == "__main__":
    main()
