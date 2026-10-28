"""Local Acquirium servers for tests, started with aq.init() as scripts start them."""
import os
from contextlib import contextmanager
from urllib.parse import quote

import pytest

from acquirium.Storage.ledger import open_ledger


def isolated_postgres_url(schema):
    """A ``postgres://`` URL for ACQUIRIUM_TEST_PG_DSN that uses only ``schema``.

    Escapement and the experiment ledger both take URLs; the schema keeps a
    test's tables away from anything else in the database.
    """
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.getenv("ACQUIRIUM_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ACQUIRIUM_TEST_PG_DSN is required for a PostgreSQL-backed test")
    params = conninfo_to_dict(dsn)
    user = quote(params.get("user", ""), safe="")
    password = params.get("password")
    auth = f"{user}:{quote(password, safe='')}@" if password else (f"{user}@" if user else "")
    host = params.get("host", "localhost")
    port = f":{params['port']}" if params.get("port") else ""
    return (f"postgres://{auth}{host}{port}/{params.get('dbname', '')}"
            f"?options={quote(f'-csearch_path={schema}', safe='')}")


@contextmanager
def postgres_schema():
    """A new schema in the ACQUIRIUM_TEST_PG_DSN database, dropped afterwards;
    yields its URL."""
    import uuid
    import psycopg

    schema = f"acquirium_test_{uuid.uuid4().hex}"
    url = isolated_postgres_url(schema)
    with psycopg.connect(os.environ["ACQUIRIUM_TEST_PG_DSN"], autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
    try:
        yield url
    finally:
        with psycopg.connect(os.environ["ACQUIRIUM_TEST_PG_DSN"], autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


@contextmanager
def local_server_storage(directory, backend="duckdb"):
    """Storage for local servers in ``directory``: yields ``(options, open_ledger)``.

    ``aq.init(**options)`` starts a server on it. ``open_ledger()`` opens the
    server's experiment ledger directly, for use only while no server is
    running. ``backend="postgres"`` keeps Escapement and the ledger in a new
    schema of the ACQUIRIUM_TEST_PG_DSN database, dropped afterwards.
    """
    if backend == "duckdb":
        yield dict(data_dir=directory, exact_only=True), lambda: open_ledger(directory / "experiments.duckdb")
        return
    import json

    with postgres_schema() as url:
        config = directory / "acquirium.toml"
        config.write_text(
            "[server]\n"
            'data_dir = "data"\n'
            f"engine_path = {json.dumps(url)}\n"  # a JSON string is a TOML string
        )
        yield dict(config=config, exact_only=True), lambda: open_ledger(url)


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
