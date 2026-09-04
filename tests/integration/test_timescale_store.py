"""Integration tests for TimescaleStore — direct access to TimescaleDB.

Requires: TimescaleDB running at localhost:5432 (via `make testing-up`).
"""

import pytest
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import polars as pl

from acquirium.Storage.timescale_store import TimescaleStore
from acquirium.internals.models import LogEntry, TimeIntervalModel
from rdflib import URIRef

TEST_POINT = "urn:test:integration_point"
TEST_REF = "urn:test:integration_ref"


# ── Storage keys ───────────────────────────────────────────


class TestRefIdKeys:
    def test_timeseries_table_keyed_by_integer_ref_id(self, ts_store, clean_point):
        ts_store.upsert_rows(
            clean_point,
            [(datetime(2025, 1, 1, tzinfo=timezone.utc), 1.0)],
            value_kind="numeric",
        )

        mapping = ts_store.sql_query(
            f"SELECT ref_id FROM ref_ids WHERE ref_uri = '{clean_point}'"
        )["rows"]
        assert len(mapping) == 1
        ref_id = mapping[0][0]
        assert isinstance(ref_id, int)

        stored = ts_store.sql_query(
            f"SELECT ref_id FROM timeseries WHERE ref_id = {ref_id}"
        )["rows"]
        assert [tuple(row) for row in stored] == [(ref_id,)]

    def test_ref_id_is_stable_across_writes(self, ts_store, clean_point):
        ts_store.upsert_rows(clean_point, [(datetime(2025, 1, 1, tzinfo=timezone.utc), 1.0)])
        first = ts_store.sql_query(f"SELECT ref_id FROM ref_ids WHERE ref_uri = '{clean_point}'")["rows"]
        ts_store.upsert_rows(clean_point, [(datetime(2025, 1, 2, tzinfo=timezone.utc), 2.0)])
        ts_store.bulk_insert_polars(pl.DataFrame({
            "ref_uri": [clean_point],
            "ts": [datetime(2025, 1, 3, tzinfo=timezone.utc)],
            "value": [3.0],
            "value_kind": ["numeric"],
        }))
        again = ts_store.sql_query(f"SELECT ref_id FROM ref_ids WHERE ref_uri = '{clean_point}'")["rows"]
        assert again == first
        assert ts_store.timeseries_info(clean_point).row_count == 3

    def test_unknown_ref_reads_empty(self, ts_store):
        assert list(ts_store.timeseries("urn:test:never_written")) == []
        assert ts_store.timeseries_info("urn:test:never_written").row_count == 0


# ── Mutation Tests ─────────────────────────────────────────


class TestUpsertRows:
    def test_basic(self, ts_store, clean_point):
        rows = [
            (datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc), 10.0),
            (datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc), 20.0),
            (datetime(2025, 1, 1, 2, 0, tzinfo=timezone.utc), 30.0),
        ]
        count = ts_store.upsert_rows(clean_point, rows)
        assert count == 3

        info = ts_store.timeseries_info(clean_point)
        assert info.row_count == 3

    def test_duplicate_timestamp_updates(self, ts_store, clean_point):
        ts = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
        ts_store.upsert_rows(clean_point, [(ts, 100.0)], value_kind="numeric")
        ts_store.upsert_rows(clean_point, [(ts, 999.0)], value_kind="numeric")

        batches = list(ts_store.timeseries(clean_point))
        assert len(batches) == 1
        assert batches[0].column("value")[0].as_py() == 999.0

    def test_none_value(self, ts_store, clean_point):
        ts = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
        count = ts_store.upsert_rows(clean_point, [(ts, None)])
        assert count == 1

        batches = list(ts_store.timeseries(clean_point))
        assert batches[0].column("value")[0].as_py() is None

    def test_empty_rows(self, ts_store, clean_point):
        count = ts_store.upsert_rows(clean_point, [])
        assert count == 0

    def test_timezone_handling(self, ts_store, clean_point):
        naive_ts = datetime(2025, 6, 1, 10, 0, 0)
        eastern = timezone(timedelta(hours=-5))
        aware_ts = datetime(2025, 6, 1, 15, 0, 0, tzinfo=eastern)

        ts_store.upsert_rows(clean_point, [
            (naive_ts, "naive"),
            (aware_ts, "aware"),
        ], value_kind="text")
        info = ts_store.timeseries_info(clean_point)
        assert info.row_count == 2


class TestReplaceRows:
    def test_replaces_existing(self, ts_store, clean_point):
        old_rows = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), 1.0),
            (datetime(2025, 1, 2, tzinfo=timezone.utc), 2.0),
        ]
        ts_store.upsert_rows(clean_point, old_rows)
        assert ts_store.timeseries_info(clean_point).row_count == 2

        new_rows = [(datetime(2025, 6, 1, tzinfo=timezone.utc), 99.0)]
        ts_store.replace_rows(clean_point, new_rows)

        info = ts_store.timeseries_info(clean_point)
        assert info.row_count == 1


class TestBulkInsertPolars:
    def test_basic(self, ts_store, clean_point):
        df = pl.DataFrame({
            "ref_uri": [clean_point, clean_point],
            "ts": [
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 2, tzinfo=timezone.utc),
            ],
            "value": ["10.0", "20.0"],
        })
        result = ts_store.bulk_insert_polars(df)
        assert result >= 0

        info = ts_store.timeseries_info(clean_point)
        assert info.row_count == 2

    def test_empty_dataframe(self, ts_store, clean_point):
        df = pl.DataFrame({
            "ref_uri": [],
            "ts": [],
            "value": [],
        }).cast({"ref_uri": pl.Utf8, "ts": pl.Datetime("us", "UTC"), "value": pl.Utf8})
        result = ts_store.bulk_insert_polars(df)
        assert result >= 0 or result == -1  # empty df may return 0 or -1

    def test_duplicate_keys_keep_last_value(self, ts_store, clean_point):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        df = pl.DataFrame({
            "ref_uri": [clean_point, clean_point],
            "ts": [ts, ts],
            "value": ["10.0", "20.0"],
            "value_kind": ["numeric", "numeric"],
        })

        result = ts_store.bulk_insert_polars(df)

        assert result == 1
        batches = list(ts_store.timeseries(clean_point))
        values = [b.column("value")[i].as_py() for b in batches for i in range(b.num_rows)]
        assert values == [20.0]


# ── Stream Handle Tests ────────────────────────────────────


class TestStreamHandles:
    TEST_SOURCE = "test_source"
    TEST_REF_NAME = "test_ref"

    def test_ref_uri_is_uuid(self, ts_store, clean_point):
        ref_uri = ts_store.ensure_stream_ref(clean_point, self.TEST_SOURCE, self.TEST_REF_NAME)
        assert isinstance(ref_uri, URIRef)
        assert len(ref_uri) == len("urn:acquirium#") + 36  # UUID5 string form

    def test_deterministic(self, ts_store, clean_point):
        # Same source_id + ref_name always yields the same ref_uri.
        ref_uri1 = ts_store.ensure_stream_ref(clean_point, self.TEST_SOURCE, self.TEST_REF_NAME)
        ref_uri2 = ts_store.ensure_stream_ref(clean_point, self.TEST_SOURCE, self.TEST_REF_NAME)
        assert ref_uri1 == ref_uri2

    def test_different_sources_different_ref_uris(self, ts_store, clean_point):
        ref_uri1 = ts_store.ensure_stream_ref(clean_point, "source_a", self.TEST_REF_NAME)
        ref_uri2 = ts_store.ensure_stream_ref(clean_point, "source_b", self.TEST_REF_NAME)
        assert ref_uri1 != ref_uri2


# ── Query Tests ────────────────────────────────────────────


class TestTimeseries:
    @pytest.fixture(autouse=True)
    def _insert_data(self, ts_store, clean_point):
        rows = [
            (datetime(2025, 1, 1, ref_uri, 0, tzinfo=timezone.utc), float(ref_uri))
            for ref_uri in range(24)
        ]
        ts_store.upsert_rows(clean_point, rows, value_kind="numeric")

    def test_basic_query(self, ts_store, clean_point):
        batches = list(ts_store.timeseries(clean_point))
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 24
        assert batches[0].schema.names == ["ts", "value", "uri"]
        assert batches[0].schema.field("value").type == pa.float64()

    def test_time_range(self, ts_store, clean_point):
        start = datetime(2025, 1, 1, 5, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        batches = list(ts_store.timeseries(clean_point, start=start, end=end))
        total = sum(b.num_rows for b in batches)
        assert total == 6  # hours 5,6,7,8,9,10

    def test_limit(self, ts_store, clean_point):
        batches = list(ts_store.timeseries(clean_point, limit=5))
        total = sum(b.num_rows for b in batches)
        assert total == 5

    def test_order_asc(self, ts_store, clean_point):
        batches = list(ts_store.timeseries(clean_point, order="asc", limit=3))
        values = [b.column("value")[i].as_py() for b in batches for i in range(b.num_rows)]
        assert values == [0.0, 1.0, 2.0]

    def test_order_desc(self, ts_store, clean_point):
        batches = list(ts_store.timeseries(clean_point, order="desc", limit=3))
        values = [b.column("value")[i].as_py() for b in batches for i in range(b.num_rows)]
        assert values == [23.0, 22.0, 21.0]

    def test_empty_point(self, ts_store):
        batches = list(ts_store.timeseries("urn:test:nonexistent_point"))
        assert batches == []


class TestTimeseriesInfo:
    def test_basic(self, ts_store, clean_point):
        rows = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), 1.0),
            (datetime(2025, 1, 2, tzinfo=timezone.utc), 2.0),
            (datetime(2025, 1, 3, tzinfo=timezone.utc), 3.0),
        ]
        ts_store.upsert_rows(clean_point, rows)
        info = ts_store.timeseries_info(clean_point)
        assert info.row_count == 3
        assert info.earliest is not None
        assert info.latest is not None
        assert info.earliest < info.latest

    def test_empty_point(self, ts_store):
        info = ts_store.timeseries_info("urn:test:nonexistent_info")
        assert info.row_count == 0
        assert info.earliest is None

    def test_batch(self, ts_store, clean_point):
        ts_store.upsert_rows(clean_point, [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), 1.0),
        ])
        result = ts_store.timeseries_info_batch([clean_point, "urn:test:missing"])
        assert clean_point in result
        assert "urn:test:missing" in result
        assert result[clean_point].row_count >= 1
        assert result["urn:test:missing"].row_count == 0


# ── Logging Tests ──────────────────────────────────────────


class TestLogs:
    def test_insert_and_query(self, ts_store, clean_point):
        log = LogEntry(
            point_uri=clean_point,
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(),
            message="test log message",
        )
        ts_store.insert_log(log)
        logs = ts_store.query_logs(clean_point)
        assert len(logs) == 1
        assert logs[0].message == "test log message"

    def test_with_observation_period(self, ts_store, clean_point):
        log = LogEntry(
            point_uri=clean_point,
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(
                start=datetime(2025, 6, 15, 9, 0, tzinfo=timezone.utc),
                end=datetime(2025, 6, 15, 11, 0, tzinfo=timezone.utc),
            ),
            message="observed event",
        )
        ts_store.insert_log(log)
        logs = ts_store.query_logs(clean_point)
        assert len(logs) == 1
        assert logs[0].period.start is not None
        assert logs[0].period.end is not None

    def test_no_period(self, ts_store, clean_point):
        log = LogEntry(
            point_uri=clean_point,
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(),
            message="no period",
        )
        ts_store.insert_log(log)
        logs = ts_store.query_logs(clean_point)
        assert len(logs) == 1
        assert logs[0].period.start is None

    def test_time_filter(self, ts_store, clean_point):
        for hour in range(5):
            log = LogEntry(
                point_uri=clean_point,
                timestamp=datetime(2025, 6, 15, hour, 0, tzinfo=timezone.utc),
                period=TimeIntervalModel(),
                message=f"log {hour}",
            )
            ts_store.insert_log(log)

        interval = TimeIntervalModel(
            start=datetime(2025, 6, 15, 1, 0, tzinfo=timezone.utc),
            end=datetime(2025, 6, 15, 3, 0, tzinfo=timezone.utc),
        )
        logs = ts_store.query_logs(clean_point, log_time_interval=interval)
        assert len(logs) == 3  # hours 1, 2, 3

    def test_observation_filter(self, ts_store, clean_point):
        log = LogEntry(
            point_uri=clean_point,
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(
                start=datetime(2025, 6, 15, 8, 0, tzinfo=timezone.utc),
                end=datetime(2025, 6, 15, 12, 0, tzinfo=timezone.utc),
            ),
            message="observed",
        )
        ts_store.insert_log(log)

        overlap = TimeIntervalModel(
            start=datetime(2025, 6, 15, 9, 0, tzinfo=timezone.utc),
            end=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
        )
        logs = ts_store.query_logs(clean_point, obs_time_interval=overlap)
        assert len(logs) == 1

    def test_delete(self, ts_store, clean_point):
        log = LogEntry(
            point_uri=clean_point,
            timestamp=datetime(2025, 6, 15, 10, 0, tzinfo=timezone.utc),
            period=TimeIntervalModel(),
            message="to delete",
        )
        ts_store.insert_log(log)
        assert len(ts_store.query_logs(clean_point)) == 1

        ts_store.delete_logs(clean_point)
        assert len(ts_store.query_logs(clean_point)) == 0


# ── Transaction Tests ──────────────────────────────────────


class TestTransactions:
    def test_commit(self, ts_store, clean_point):
        ts_store.begin()
        ts_store.upsert_rows(clean_point, [
            (datetime(2025, 7, 1, tzinfo=timezone.utc), 42.0),
        ])
        ts_store.commit()
        info = ts_store.timeseries_info(clean_point)
        assert info.row_count >= 1

    def test_rollback(self, ts_store, clean_point):
        # Insert one row and commit to establish baseline
        ts_store.upsert_rows(clean_point, [
            (datetime(2025, 7, 1, tzinfo=timezone.utc), 1.0),
        ])
        baseline = ts_store.timeseries_info(clean_point).row_count

        # Begin transaction, insert, rollback
        ts_store.begin()
        ts_store.upsert_rows(clean_point, [
            (datetime(2025, 7, 2, tzinfo=timezone.utc), 999.0),
        ])
        ts_store.rollback()

        after = ts_store.timeseries_info(clean_point).row_count
        assert after == baseline
