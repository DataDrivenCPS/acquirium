"""Shared fixtures for integration tests.

These fixtures require running services (TimescaleDB, Mosquitto, acquirium server).
Start services with `make testing-up` before running integration tests.
"""

import os

import polars as pl
import pytest

from acquirium import Acquirium
from acquirium.Storage.timescale_store import TimescaleStore
from acquirium.Storage.values import assign_stream_value_kind


PG_DSN = os.getenv(
    "ACQUIRIUM_TEST_PG_DSN",
    "postgresql://acquirium:acquirium@localhost:5432/acquirium_test",
)
ACQUIRIUM_TEST_SERVER_HOST = os.getenv("ACQUIRIUM_TEST_SERVER_HOST", "localhost")
ACQUIRIUM_TEST_SERVER_PORT = int(os.getenv("ACQUIRIUM_TEST_SERVER_PORT", "8000"))
TEST_POINT_URI = "urn:test:integration_point"
TEST_REF_URI = "urn:test:integration_ref"
SAMPLE_SOURCE_ID = "LAB"


@pytest.fixture(scope="session")
def pg_dsn():
    return PG_DSN


@pytest.fixture(scope="session")
def acquirium_server_url():
    return f"http://{ACQUIRIUM_TEST_SERVER_HOST}:{ACQUIRIUM_TEST_SERVER_PORT}"


@pytest.fixture(scope="session")
def acquirium_client_kwargs():
    return {
        "server_url": ACQUIRIUM_TEST_SERVER_HOST,
        "server_port": ACQUIRIUM_TEST_SERVER_PORT,
        "use_ssl": False,
    }


_CSV_SOURCE_ID = "test-csv-data"


@pytest.fixture(scope="module")
def acquirium_client_csv(acquirium_client_kwargs):
    acq = Acquirium(**acquirium_client_kwargs)
    acq.insert_graph("tests/test_model_csv.ttl")
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


@pytest.fixture(scope="module")
def ts_store(pg_dsn):
    """Direct TimescaleStore connection for storage-layer tests."""
    store = TimescaleStore(dsn=pg_dsn, connect_timeout=10, recreate=False)
    yield store
    store.close()


@pytest.fixture
def clean_point(ts_store):
    """Provides a test point URI and cleans up its data after each test."""
    yield TEST_POINT_URI
    with ts_store.conn.cursor() as cur:
        cur.execute("DELETE FROM timeseries WHERE ref_uri = %s", [TEST_POINT_URI])
        cur.execute("DELETE FROM streams WHERE point_uri = %s", [TEST_POINT_URI])
        cur.execute("DELETE FROM logs WHERE point_uri = %s", [TEST_POINT_URI])


def insert_sample_csv_streams(acq, *, source_id: str = SAMPLE_SOURCE_ID) -> None:
    """Insert the sample CSV rows through the public timeseries API."""
    import polars as pl

    df = pl.read_csv("tests/sample_data.csv", try_parse_dates=True)
    stream_specs = []
    stream_rows: dict = {}
    for i in range(1, 11):
        ref_name = f"point_{i}"
        rows = list(df.select("Timestamp", ref_name).iter_rows())
        stream_specs.append({
            "source_id": source_id,
            "ref_name": ref_name,
            "point_uri": f"urn:ex/point_{i}",
            "value_kind": assign_stream_value_kind(value for _, value in rows),
        })
        stream_rows[ref_name] = rows
    acq.register_streams(stream_specs)
    acq.insert_timeseries_batch(source_id, stream_rows)
