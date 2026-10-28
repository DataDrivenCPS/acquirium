"""The model history tests of tests/unit/test_model_history.py on Postgres.

Needs the test TimescaleDB (``make testing-up``, ``ACQUIRIUM_TEST_PG_DSN``).
"""
import pytest

from acquirium.Storage.timescale_store import TimescaleStore
from tests.unit.test_model_history import (  # noqa: F401  (collected here)
    clock,
    store_opener,
    test_a_fix_replaces_the_model_from_its_start,
    test_a_late_entry_keeps_a_later_recorded_change,
    test_a_registration_over_all_time_returns_after_a_removal,
    test_a_restart_rebuilds_live_graphs_that_missed_a_write,
    test_an_update_over_an_interval_runs_on_each_version,
    test_blank_nodes_keep_their_labels,
    test_random_writes_match_the_definition,
    test_the_changed_region_of_a_write,
    test_the_first_undated_write_holds_from_minus_infinity,
)


@pytest.fixture
def open_store(tmp_path, monkeypatch, clock, pg_dsn):
    fresh = [True]

    def timeseries():
        # The first open of a test starts from empty tables; reopens keep them.
        store = TimescaleStore(dsn=pg_dsn, recreate=fresh[0])
        fresh[0] = False
        return store

    open_, close = store_opener(tmp_path, monkeypatch, timeseries)
    yield open_
    close()
