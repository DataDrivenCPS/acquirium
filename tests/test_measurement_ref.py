"""measurement(stream=) and lazy stream statistics against a real server.

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
    ac.register_streams([
        {"property_uri": "urn:test:refstats#feed", "source_id": SOURCE, "stream_name": "FT_feed",
         "value_kind": "numeric"},
        {"property_uri": "urn:test:refstats#perm", "source_id": SOURCE, "stream_name": "FT_perm",
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

    def counting(uris, **kwargs):
        calls.append(list(uris))
        return original(uris, **kwargs)

    monkeypatch.setattr(hist.client, "timeseries_info_batch", counting)
    return calls


@pytest.fixture
def exists_calls(hist, monkeypatch):
    """Count the client's which-streams-have-rows requests."""
    calls = []
    original = hist.client.timeseries_exists_batch

    def counting(uris, **kwargs):
        calls.append(list(uris))
        return original(uris, **kwargs)

    monkeypatch.setattr(hist.client, "timeseries_exists_batch", counting)
    return calls


def test_ref_selects_one_stream_by_source_and_name(hist):
    feed = hist.stream(SOURCE, "FT_feed")
    df = hist.query().measurement(stream=feed).dataframe(shape="narrow", compact=False)
    assert df["stream_uri"].unique().to_list() == [feed.stream_uri]
    assert df["value_numeric"].to_list() == [0.0, 1.0, 2.0]


def test_ref_list_selects_each_stream(hist):
    refs = [hist.stream(SOURCE, "FT_feed"), str(hist.stream(SOURCE, "FT_perm"))]
    info = hist.query().measurement(stream=refs).data().stream_info()
    assert sorted(info["stream_name"].to_list()) == ["FT_feed", "FT_perm"]
    assert info["source_id"].unique().to_list() == [SOURCE]


def test_streams_takes_the_same_stream_handles(hist):
    feed, perm = hist.stream(SOURCE, "FT_feed"), hist.stream(SOURCE, "FT_perm")
    data = hist.query().measurement(stream=[feed, perm]).data()
    narrow = data.dataframe(shape="narrow", compact=False, streams=feed)
    assert narrow["stream_uri"].unique().to_list() == [feed.stream_uri]
    latest = data.latest(streams=[perm])
    assert latest["value"].to_list() == [11.0]


def test_query_data_keeps_distinct_properties_on_one_stream(hist):
    points = {"urn:test:refstats#shared-a": "Probe A", "urn:test:refstats#shared-b": "Probe B"}
    hist.register_streams([
        {"property_uri": point, "label": label, "source_id": SOURCE,
         "stream_name": "FT_shared", "value_kind": "numeric"}
        for point, label in points.items()
    ])
    hist.insert_timeseries(SOURCE, "FT_shared", [(T0, 7.0)])
    stream = hist.stream(SOURCE, "FT_shared")
    query = hist.query().measurement(stream=stream)
    for view in ("stored", "current"):
        data = query.data(view=view)
        assert {binding.property_uri: binding.property_label for binding in data.bindings} == points
        rows = data.dataframe(shape="narrow", compact=False)
        assert dict(rows.select("property_uri", "value_numeric").iter_rows()) == dict.fromkeys(points, 7.0)


@pytest.mark.parametrize("ref", [[], "urn:acquirium:no-such-stream"])
def test_ref_without_a_match_is_empty(hist, ref):
    data = hist.query().measurement(stream=ref).data()
    assert data.aliases == ["data"]
    assert data.bindings == []
    assert data.is_empty()


def test_reading_values_skips_the_stats(hist, stats_calls):
    q = hist.query().measurement(stream=hist.stream(SOURCE, "FT_feed"))
    data = q.data()
    assert data["data"]["value"].to_list() == [0.0, 1.0, 2.0]
    q.dataframe()
    assert stats_calls == []


def test_stats_are_fetched_once_on_first_read(hist, stats_calls):
    refs = [hist.stream(SOURCE, "FT_feed"), hist.stream(SOURCE, "FT_perm")]
    data = hist.query().measurement(stream=refs).data()
    assert stats_calls == []
    assert data.total_rows == 5
    assert data.time_range == (T0, T0 + timedelta(minutes=2))
    assert not data.is_empty()
    assert {b.stream_name: b.row_count for b in data.bindings} == {"FT_feed": 3, "FT_perm": 2}
    assert len(stats_calls) == 1
    assert sorted(stats_calls[0]) == sorted(r.stream_uri for r in refs)


def test_is_empty_asks_which_streams_have_rows(hist, stats_calls, exists_calls):
    hist.register_streams([{"property_uri": "urn:test:refstats#none", "source_id": SOURCE,
                            "stream_name": "FT_none", "value_kind": "numeric"}])
    feed, perm, none = (hist.stream(SOURCE, n) for n in ("FT_feed", "FT_perm", "FT_none"))
    data = hist.query().measurement(stream=[feed, perm, none]).data()
    assert not data.is_empty()
    assert hist.query().measurement(stream=none).data().is_empty()
    assert stats_calls == []
    assert len(exists_calls) == 2
    assert sorted(exists_calls[0]) == sorted(s.stream_uri for s in (feed, perm, none))


def test_refresh_stats_keeps_the_sample_epoch(hist, stats_calls, exists_calls):
    hist.register_streams([{"property_uri": "urn:test:refstats#extra", "source_id": SOURCE,
                            "stream_name": "FT_extra", "value_kind": "numeric"}])
    query = hist.query().measurement(stream=hist.stream(SOURCE, "FT_extra"))
    data = query.data()
    assert data.is_empty()
    assert data.total_rows == 0
    hist.insert_timeseries_batch(SOURCE, {"FT_extra": [(T0, 1.0)]})
    assert data.total_rows == 0  # the cached snapshot
    data.refresh_stats()
    assert data.is_empty()
    assert data.total_rows == 0
    assert len(stats_calls) == 2
    assert len(exists_calls) == 2
    assert data.dataframe().is_empty()

    # A new object uses the latest epoch for counts and values.
    current = query.data()
    assert not current.is_empty()
    assert current.total_rows == 1
    assert current.dataframe().height == 1
