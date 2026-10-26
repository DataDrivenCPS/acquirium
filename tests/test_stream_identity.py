"""Stream identity on bindings and describing streams without sample statistics.

Runs on the shared local DuckDB server (see ``tests/conftest.py``).
"""
from datetime import datetime, timedelta, timezone

import pytest

from tests.local_server import acquirium_without_experiments

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def hist(local_acquirium):
    """A historian datasource with one stream on a point and one without."""
    ac = acquirium_without_experiments(local_acquirium)
    ac.register_datasource("hist")
    ac.register_streams([
        {"point_uri": "urn:test:feed", "source_id": "hist", "ref_name": "FT_feed",
         "label": "feed flow", "unit": "M3-PER-HR", "value_kind": "numeric"},
        {"source_id": "hist", "ref_name": "bare", "unit": "KiloW", "value_kind": "numeric"},
    ])
    ac.insert_timeseries_batch("hist", {
        "FT_feed": [(T0 + timedelta(minutes=i), float(i)) for i in range(3)],
    })
    return ac


@pytest.fixture
def ac(hist):
    return hist


@pytest.fixture
def no_stats(ac, monkeypatch):
    """Fail the test on any timeseries_info_batch call; returns the call log."""
    calls = []

    def spy(uris):
        calls.append(list(uris))
        raise AssertionError("timeseries_info_batch must not be called")

    monkeypatch.setattr(ac.client, "timeseries_info_batch", spy)
    return calls


def _unit(b):
    return b.property_unit or b.ref_unit


def test_binding_carries_source_id_and_ref_name(ac):
    feed = str(ac.stream("hist", "FT_feed"))
    [b] = ac.query().measurement(label="feed flow").data().bindings
    assert b.ref_uri == feed
    assert b.source_id == "hist"
    assert b.ref_name == "FT_feed"
    assert b.point_label == "feed flow"
    assert _unit(b).endswith("/unit/M3-PER-HR")


def test_stream_without_point_has_ref_name_and_unit(ac):
    bare = str(ac.stream("hist", "bare"))
    [b] = ac.query().measurement(ref=bare).data(stats=False).bindings
    assert b.ref_uri == bare
    assert b.source_id == "hist"
    assert b.ref_name == "bare"
    assert _unit(b).endswith("/unit/KiloW")


def test_stats_off_makes_no_stats_call(ac, no_stats):
    feed = str(ac.stream("hist", "FT_feed"))
    d = ac.query().measurement(label="feed flow").data(stats=False)
    [b] = d.bindings
    assert (b.point_uri, b.point_label, b.source_id, b.ref_name) == (
        "urn:test:feed", "feed flow", "hist", "FT_feed")
    assert b.ref_uri == feed
    assert _unit(b).endswith("/unit/M3-PER-HR")
    assert (b.row_count, b.earliest, b.latest) == (None, None, None)
    d.metadata(include_ref_uris=True)
    repr(d)
    assert no_stats == []


def test_stats_off_still_materializes(ac, no_stats):
    d = ac.query().measurement(label="feed flow").data(stats=False)
    df = d.dataframe(shape="narrow", compact=False)
    assert df.height == 3
    assert no_stats == []


def test_stats_off_readers_raise_until_materialized(ac, no_stats):
    d = ac.query().measurement(label="feed flow").data(stats=False)
    for read in (lambda: d.total_rows, lambda: d.time_range, d.is_empty):
        with pytest.raises(RuntimeError, match="stats=True"):
            read()
    d.dataframe()
    assert not d.is_empty()  # checks the loaded rows, not the stats
    assert no_stats == []


@pytest.mark.parametrize("names", [["FT_feed"], ["FT_feed", "bare"]])
def test_select_by_ref(ac, no_stats, names):
    refs = [ac.stream("hist", n) for n in names]
    bindings = ac.query().measurement(ref=refs).data(stats=False).bindings
    assert sorted(b.ref_uri for b in bindings) == sorted(str(r) for r in refs)
    assert sorted(b.ref_name for b in bindings) == sorted(names)
    assert no_stats == []


def test_select_by_source_and_ref_name(ac):
    """(source_id, ref_name) -> stream: ac.stream computes the ref URI locally."""
    d = ac.query().measurement(ref=ac.stream("hist", "FT_feed")).data()
    [b] = d.bindings
    assert (b.source_id, b.ref_name, b.point_uri) == ("hist", "FT_feed", "urn:test:feed")
    assert d.total_rows == 3
    assert d.dataframe(shape="narrow", compact=False).height == 3


def test_query_dataframe_by_name_skips_stats(ac, no_stats):
    df = ac.query().measurement(ref=ac.stream("hist", "FT_feed")).dataframe(
        shape="narrow", compact=False)
    assert df.height == 3
    assert no_stats == []


def test_select_by_unknown_ref_is_empty(ac, no_stats):
    unknown = str(ac.stream("hist", "no-such-stream"))
    assert ac.query().measurement(ref=[unknown]).data(stats=False).bindings == []
    assert ac.query().measurement(ref=[]).data(stats=False).bindings == []
    assert no_stats == []


def test_metadata_ref_columns(ac):
    d = ac.query().measurement(label="feed flow").data(stats=False)
    plain = d.metadata()
    assert plain.columns == ["data_alias", "point_label", "point_uri"]
    with_refs = d.metadata(include_ref_uris=True)
    assert with_refs.columns == ["data_alias", "point_label", "point_uri",
                                 "ref_uri", "source_id", "ref_name"]
    assert with_refs.row(0, named=True)["source_id"] == "hist"
    assert with_refs.row(0, named=True)["ref_name"] == "FT_feed"
    # the same columns once the data is materialized
    d.dataframe()
    assert d.metadata().columns == plain.columns
    assert d.metadata(include_ref_uris=True).select("source_id", "ref_name").row(0) == ("hist", "FT_feed")


def test_default_data_fills_stats(ac):
    [b] = ac.query().measurement(label="feed flow").data().bindings
    assert b.row_count == 3
    assert b.earliest == T0
    assert b.latest == T0 + timedelta(minutes=2)


def test_query_metadata_unchanged(ac):
    """The new internal columns stay hidden from Query.metadata()."""
    cols = ac.query().measurement(label="feed flow").metadata().columns
    assert not any(c.startswith(("extsid", "extrname")) for c in cols)
