"""Shared fixtures.

Every test runs with plain ``pytest``: servers are started in-process with
aq.init(), as scripts start them. Tests that need PostgreSQL skip unless
ACQUIRIUM_TEST_PG_DSN is set, and MQTT tests unless ACQUIRIUM_TEST_MQTT is.
"""

import polars as pl
import pytest

from tests.local_server import acquirium_without_experiments, local_server_storage


_CSV_SOURCE_ID = "test-csv-data"


@pytest.fixture(scope="module")
def own_acquirium(tmp_path_factory):
    """A server for this module alone, on empty storage.

    For tests that query the whole model or use fixed URIs, which would see
    or clobber other tests' data on the shared server. aq.init() runs one
    server per process, so this stops the shared one; the next test that uses
    it starts it again on the same data.
    """
    import acquirium as aq

    aq.shutdown()
    with local_server_storage(tmp_path_factory.mktemp("module")) as (options, _):
        try:
            yield aq.init(**options)
        finally:
            aq.shutdown()


@pytest.fixture(scope="module")
def acquirium_client_csv(own_acquirium):
    """The module's own server with the CSV test model and its readings."""
    acq = own_acquirium
    acq.insert_graph_file("tests/test_model_csv.ttl", source_id="plant")
    acq.register_datasource(_CSV_SOURCE_ID)
    acq.register_streams([
        {
            "source_id": _CSV_SOURCE_ID,
            "ref_name": f"point_{i}",
            "point_uri": f"urn:ex/point_{i}",
            "value_kind": "text" if i >= 9 else "numeric",
        }
        for i in range(1, 11)
    ])
    value_cols = [f"point_{i}" for i in range(1, 11)]
    df = pl.read_csv("tests/sample_data.csv")
    long = (
        df.with_columns([
            pl.col("Timestamp").str.to_datetime(time_zone="UTC"),
            *[pl.col(c).cast(pl.String) for c in value_cols],
        ])
        .unpivot(on=value_cols, index="Timestamp", variable_name="ref_name", value_name="value")
        .rename({"Timestamp": "ts"})
    )
    acq.insert_timeseries_arrow(_CSV_SOURCE_ID, long.to_arrow())
    return acq


@pytest.fixture
def samples():
    """Experiment samples sent to an in-memory Escapement."""
    from acquirium.core import Core
    from acquirium.ExperimentSamples import EscapementSamples

    core = Core(":memory:")
    try:
        yield EscapementSamples(core)
    finally:
        core.close(drain=False)


@pytest.fixture(params=["duckdb", "postgres"])
def experiment_ledger(request, tmp_path, samples):
    """An experiment ledger on each SQL backend, sending samples to an
    in-memory Escapement."""
    from acquirium.Storage.ledger import open_ledger
    from tests.local_server import postgres_schema

    if request.param == "duckdb":
        ledger = open_ledger(tmp_path / "experiments.duckdb", samples=samples)
        try:
            yield ledger
        finally:
            ledger.close()
    else:
        with postgres_schema() as url:
            ledger = open_ledger(url, samples=samples)
            try:
                yield ledger
            finally:
                ledger.close()


@pytest.fixture(scope="session")
def local_acquirium(tmp_path_factory):
    """Options for the one local server the whole run shares."""
    import acquirium as aq

    with local_server_storage(tmp_path_factory.mktemp("acquirium")) as (options, _):
        try:
            yield options
        finally:
            aq.shutdown()


@pytest.fixture
def ac(local_acquirium):
    """The shared local server, with no Studies or runs."""
    return acquirium_without_experiments(local_acquirium)


@pytest.fixture
def separate_server():
    """Stop the shared server so a test can start its own with aq.init().

    aq.init() runs one server per process. The next test that uses the shared
    server starts it again on the same data.
    """
    import acquirium as aq

    aq.shutdown()
    yield
    aq.shutdown()
