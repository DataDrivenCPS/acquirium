"""Stream identity and statistics-free stream lookup, against a live server.

Describes registered streams through the public query API only: no SPARQL
over ``acq:refName`` / ``ref:hasExternalReference``, and with
``data(stats=False)`` no ``timeseries_info_batch`` scan.
"""
from datetime import datetime, timedelta, timezone

import pytest
from conftest import ACQUIRIUM_TEST_SERVER_HOST, ACQUIRIUM_TEST_SERVER_PORT

from acquirium import Acquirium

SOURCE_ID = "hist"
T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def acq():
    acq = Acquirium(
        server_url=ACQUIRIUM_TEST_SERVER_HOST,
        server_port=ACQUIRIUM_TEST_SERVER_PORT,
        use_ssl=False,
    )
    acq.register_datasource(SOURCE_ID)
    acq.register_streams([
        {"point_uri": "urn:test:feed", "source_id": SOURCE_ID, "ref_name": "FT_feed",
         "label": "feed flow", "unit": "M3-PER-HR", "value_kind": "numeric"},
        {"source_id": SOURCE_ID, "ref_name": "bare", "unit": "KiloW", "value_kind": "numeric"},
    ])
    acq.insert_timeseries_batch(
        SOURCE_ID, {"FT_feed": [(T0 + timedelta(minutes=i), float(i)) for i in range(5)]},
    )
    return acq


@pytest.fixture
def stats_calls(acq, monkeypatch):
    """Record every timeseries_info_batch request the client makes."""
    calls = []
    real = acq.client.timeseries_info_batch

    def spy(uris):
        calls.append(list(uris))
        return real(uris)

    monkeypatch.setattr(acq.client, "timeseries_info_batch", spy)
    return calls


def _refs(acq):
    return str(acq.reference_uri(SOURCE_ID, "FT_feed")), str(acq.reference_uri(SOURCE_ID, "bare"))


def test_binding_carries_stream_identity(acq):
    feed, _ = _refs(acq)
    [b] = acq.query().measurement(label="feed flow").data().bindings
    assert b.ref_uri == feed
    assert (b.source_id, b.ref_name) == (SOURCE_ID, "FT_feed")
    assert b.point_label == "feed flow"
    assert any(u and u.endswith("/unit/M3-PER-HR") for u in (b.property_unit, b.ref_unit))


def test_stream_without_point_carries_identity(acq):
    _, bare = _refs(acq)
    [b] = acq.query().measurement(ref=bare).data().bindings
    assert b.ref_name == "bare" and b.source_id == SOURCE_ID
    assert any(u and u.endswith("/unit/KiloW") for u in (b.property_unit, b.ref_unit))


def test_stats_off_makes_no_stats_call(acq, stats_calls):
    feed, bare = _refs(acq)
    data = acq.query().measurement(ref=[feed, bare]).data(stats=False)
    assert stats_calls == []
    by_ref = {b.ref_uri: b for b in data.bindings}
    assert set(by_ref) == {feed, bare}
    b = by_ref[feed]
    assert (b.point_label, b.source_id, b.ref_name) == ("feed flow", SOURCE_ID, "FT_feed")
    assert any(u and u.endswith("/unit/M3-PER-HR") for u in (b.property_unit, b.ref_unit))
    assert (b.row_count, b.earliest, b.latest) == (0, None, None)
    # statistics load on first use, once
    assert data.total_rows == 5
    assert len(stats_calls) == 1


@pytest.mark.parametrize("pick, expected", [
    (lambda feed, bare: feed, {"FT_feed"}),
    (lambda feed, bare: [feed, bare], {"FT_feed", "bare"}),
    (lambda feed, bare: "urn:acquirium#00000000-0000-0000-0000-000000000000", set()),
])
def test_select_by_ref(acq, stats_calls, pick, expected):
    data = acq.query().measurement(ref=pick(*_refs(acq))).data(stats=False)
    assert {b.ref_name for b in data.bindings} == expected
    assert stats_calls == []


def test_metadata_identity_columns(acq):
    feed, bare = _refs(acq)
    data = acq.query().measurement(ref=[feed, bare]).data(stats=False)
    meta = data.metadata(include_ref_uris=True)
    assert {"source_id", "ref_name"} <= set(meta.columns)
    assert dict(zip(meta["ref_uri"], meta["ref_name"])) == {feed: "FT_feed", bare: "bare"}
    assert data.metadata().columns == ["data_alias", "point_label", "point_uri"]


def test_default_data_still_fills_stats(acq):
    feed, _ = _refs(acq)
    data = acq.query().measurement(ref=feed).data()
    [b] = data.bindings
    assert b.row_count == 5
    assert b.earliest == T0 and b.latest == T0 + timedelta(minutes=4)
    assert data.time_range == (b.earliest, b.latest)
