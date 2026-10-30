"""measurement(ref=) and lazy stream statistics against a real server.

Runs on the shared local DuckDB server (see ``tests/conftest.py``).
"""
from datetime import datetime, timedelta, timezone

import pytest

from tests.local_server import acquirium_without_experiments

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
SOURCE = "refstats"


@pytest.fixture(scope="module")
def hist(local_acquirium):
    """Two streams on two points, three rows in one and two in the other."""
    ac = acquirium_without_experiments(local_acquirium)
    ac.register_datasource(SOURCE)
    ac.register_streams([
        {"point_uri": "urn:test:refstats#feed", "source_id": SOURCE, "ref_name": "FT_feed",
         "value_kind": "numeric"},
        {"point_uri": "urn:test:refstats#perm", "source_id": SOURCE, "ref_name": "FT_perm",
         "value_kind": "numeric"},
    ])
    ac.insert_timeseries_batch(SOURCE, {
        "FT_feed": [(T0 + timedelta(minutes=i), float(i)) for i in range(3)],
        "FT_perm": [(T0 + timedelta(minutes=i), 10.0 + i) for i in range(2)],
    })
    return ac


@pytest.fixture
def stats_calls(hist, monkeypatch):
    """Count the client's stream-statistics requests."""
    calls = []
    original = hist.client.timeseries_info_batch

    def counting(uris):
        calls.append(list(uris))
        return original(uris)

    monkeypatch.setattr(hist.client, "timeseries_info_batch", counting)
    return calls


def test_ref_selects_one_stream_by_source_and_name(hist):
    feed = hist.stream(SOURCE, "FT_feed")
    df = hist.query().measurement(ref=feed).dataframe(shape="narrow", compact=False)
    assert df["ref_uri"].unique().to_list() == [feed.ref_uri]
    assert df["value_numeric"].to_list() == [0.0, 1.0, 2.0]


def test_ref_list_selects_each_stream(hist):
    refs = [hist.stream(SOURCE, "FT_feed"), str(hist.stream(SOURCE, "FT_perm"))]
    info = hist.query().measurement(ref=refs).data().stream_info()
    assert sorted(info["ref_name"].to_list()) == ["FT_feed", "FT_perm"]
    assert info["source_id"].unique().to_list() == [SOURCE]


def test_streams_takes_the_same_stream_handles(hist):
    feed, perm = hist.stream(SOURCE, "FT_feed"), hist.stream(SOURCE, "FT_perm")
    data = hist.query().measurement(ref=[feed, perm]).data()
    narrow = data.dataframe(shape="narrow", compact=False, streams=feed)
    assert narrow["ref_uri"].unique().to_list() == [feed.ref_uri]
    latest = data.latest(streams=[perm])
    assert latest["value"].to_list() == [11.0]


@pytest.mark.parametrize("ref", [[], "urn:acquirium:no-such-stream"])
def test_ref_without_a_match_is_empty(hist, ref):
    data = hist.query().measurement(ref=ref).data()
    assert data.aliases == ["data"]
    assert data.bindings == []
    assert data.is_empty()


def test_reading_values_skips_the_stats(hist, stats_calls):
    q = hist.query().measurement(ref=hist.stream(SOURCE, "FT_feed"))
    data = q.data()
    assert data["data"]["value"].to_list() == [0.0, 1.0, 2.0]
    q.dataframe()
    assert stats_calls == []


def test_stats_are_fetched_once_on_first_read(hist, stats_calls):
    refs = [hist.stream(SOURCE, "FT_feed"), hist.stream(SOURCE, "FT_perm")]
    data = hist.query().measurement(ref=refs).data()
    assert stats_calls == []
    assert data.total_rows == 5
    assert data.time_range == (T0, T0 + timedelta(minutes=2))
    assert not data.is_empty()
    assert {b.ref_name: b.row_count for b in data.bindings} == {"FT_feed": 3, "FT_perm": 2}
    assert len(stats_calls) == 1
    assert sorted(stats_calls[0]) == sorted(r.ref_uri for r in refs)


def test_refresh_stats_sees_new_rows(hist, stats_calls):
    hist.register_streams([{"point_uri": "urn:test:refstats#extra", "source_id": SOURCE,
                            "ref_name": "FT_extra", "value_kind": "numeric"}])
    data = hist.query().measurement(ref=hist.stream(SOURCE, "FT_extra")).data()
    assert data.total_rows == 0
    hist.insert_timeseries_batch(SOURCE, {"FT_extra": [(T0, 1.0)]})
    assert data.total_rows == 0  # the cached snapshot
    data.refresh_stats()
    assert data.total_rows == 1
    assert len(stats_calls) == 2
