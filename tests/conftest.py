"""Shared fixtures for integration tests.

These fixtures require running services (TimescaleDB, Mosquitto, acquirium server).
Start services with `make testing-up` before running integration tests.
"""

import pytest

from acquirium.Storage.timescale_store import TimescaleStore


PG_DSN = "postgresql://acquirium:acquirium@localhost:5432/acquirium_test"
ACQUIRIUM_TEST_SERVER_HOST = "localhost"
ACQUIRIUM_TEST_SERVER_PORT = 8000
TEST_POINT_URI = "urn:test:integration_point"
TEST_REF_URI = "urn:test:integration_ref"


@pytest.fixture(scope="session")
def pg_dsn():
    return PG_DSN


@pytest.fixture(scope="session")
def acquirium_server_url():
    return "http://localhost:8000"


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
