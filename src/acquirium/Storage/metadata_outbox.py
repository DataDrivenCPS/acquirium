"""Transactional handoff of resolved graph writes from the recording ledger."""
from __future__ import annotations

import json


def execute(conn, statement, parameters=()):
    if conn.__class__.__module__.startswith("psycopg"):
        statement = statement.replace("?", "%s")
    return conn.execute(statement, list(parameters or ()))


def enqueue(conn, update):
    if isinstance(update, dict):
        update = json.dumps(update)
    version = execute(conn, "UPDATE experiment_metadata_state SET version=version+1 RETURNING version").fetchone()[0]
    execute(conn, "INSERT INTO experiment_metadata_outbox (version, update_text) VALUES (?, ?)", [version, update])

