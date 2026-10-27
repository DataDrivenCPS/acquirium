"""Local Acquirium servers for tests, started with aq.init() as scripts start them."""
import os
from contextlib import contextmanager

import pytest

from acquirium.Storage.timescale_store import TimescaleStore


@contextmanager
def local_server_storage(directory, backend="duckdb"):
    """Storage for local servers in ``directory``: yields ``(options, open_store)``.

    ``aq.init(**options)`` starts a server on it. ``open_store()`` opens the
    server's SQL store directly, for use only while no server is running.
    ``backend="timescale"`` keeps the tables in a new schema of the
    ACQUIRIUM_TEST_PG_DSN database, dropped afterwards.
    """
    if backend == "duckdb":
        from acquirium.Storage.duckdb_store import DuckDBStore
        yield dict(data_dir=directory, exact_only=True), lambda: DuckDBStore(directory / "timeseries.duckdb")
        return
    import json
    import uuid
    import psycopg
    from psycopg.conninfo import make_conninfo

    dsn = os.getenv("ACQUIRIUM_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ACQUIRIUM_TEST_PG_DSN is required for a Timescale-backed server")
    schema = f"experiment_contract_{uuid.uuid4().hex}"
    isolated = make_conninfo(dsn, options=f"-c search_path={schema}")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
    config = directory / "acquirium.toml"
    config.write_text(
        "[server]\n"
        'data_dir = "data"\n'
        'timeseries_backend = "timescale"\n'
        f"pg_dsn = {json.dumps(isolated)}\n"  # a JSON string is a TOML string
    )
    try:
        yield dict(config=config, exact_only=True), lambda: TimescaleStore(dsn=isolated)
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def acquirium_without_experiments(options):
    """The local server for ``options``, with no Studies or runs.

    aq.init() returns the server already running for these options, or starts
    it again on the same data after a test that needed its own server stopped
    it.
    """
    import acquirium as aq

    ac = aq.init(**options)
    reset_experiments(ac.client)
    return ac


def reset_experiments(client):
    """Finish every running run, then delete every Study and run."""
    for study in client.list_experiment_studies():
        for run in client.list_experiments(study["template_id"], status="running"):
            client.finish_experiment(run["run_id"], failed=True)
    client.clear_experiments(confirm=True)
