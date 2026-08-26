"""The revision-frontier behavior must be identical on both SQL backends."""
from datetime import datetime, timezone
import os
import uuid

import pyarrow as pa
import pytest
import psycopg
from psycopg.conninfo import make_conninfo

from acquirium.Materialization import AllAvailable, Binding, InProcessExecutor, RevisionStore, Scheduler, StreamDescriptor, Transformation, outputs
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.timescale_store import TimescaleStore


class Copy(Transformation):
    start = AllAvailable()
    outputs = {"out": outputs.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": source["value"]})


@pytest.fixture(params=["duckdb", "timescale"])
def materialization_store(request, tmp_path):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "contract.duckdb")
    else:
        dsn = os.getenv("ACQUIRIUM_TEST_PG_DSN")
        if not dsn:
            pytest.skip("ACQUIRIUM_TEST_PG_DSN is required for the Timescale contract target")
        # The API integration suite shares its database with the server.  Use
        # an isolated schema so ``recreate`` cannot drop the server's tables.
        schema = f"materialization_contract_{uuid.uuid4().hex}"
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{schema}"')
        # Use only the private schema.  Including ``public`` would cause
        # ``CREATE TABLE IF NOT EXISTS`` to reuse the live server's tables.
        # TimescaleStore qualifies the extension function it needs from public.
        isolated_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
        store = TimescaleStore(dsn=isolated_dsn, recreate=True)
    try:
        yield store
    finally:
        store.close()
        if request.param == "timescale":
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.integration
def test_materialization_revision_frontier_contract(materialization_store):
    """Inputs, output visibility, and the durable frontier agree per backend."""
    store = materialization_store
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:input", [(timestamp, 2.0)], value_kind="numeric")
    binding = Binding(
        "copy", "digest", {"source": (StreamDescriptor("urn:input"),)},
        {"out": ("urn:output", Copy.outputs["out"])},
    )
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions, InProcessExecutor())

    assert scheduler.run_once(binding, Copy())
    assert not scheduler.run_once(binding, Copy())
    assert revisions.current_revision() == 2
    assert list(store.timeseries("urn:output", value_mode="numeric"))[0].column("value").to_pylist() == [2.0]

    # A correction is another revision and is seen exactly once by the same
    # binding, on both implementations.
    store.upsert_rows("urn:input", [(timestamp, 5.0)], value_kind="numeric")
    assert scheduler.run_once(binding, Copy())
    assert not scheduler.run_once(binding, Copy())
    assert revisions.current_revision() == 4
    assert list(store.timeseries("urn:output", value_mode="numeric"))[0].column("value").to_pylist() == [5.0]
