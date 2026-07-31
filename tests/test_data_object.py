from datetime import timedelta

import pytest
from acquirium.Client.data_object import DataObject
from acquirium.internals.internals_namespaces import *


# ---- Alias-based access ----

def test_getitem_returns_correct_columns(acquirium_client_csv):
    """data['alias'] should return a DataFrame with [time, value] columns."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    assert not data.is_empty()
    assert len(data.aliases) >= 1

    alias = data.aliases[0]
    df = data[alias]
    assert "time" in df.columns
    assert "value" in df.columns
    assert not df.is_empty()


def test_getitem_nonexistent_alias_returns_empty(acquirium_client_csv):
    """Accessing a nonexistent alias should return an empty DataFrame."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data["nonexistent_alias_xyz"]
    assert df.is_empty()
    assert "time" in df.columns
    assert "value" in df.columns


# ---- Grouping via by() ----

def test_by_grouping(acquirium_client_csv):
    """by() should yield entity-scoped sub-DataObjects."""
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    entity_BE = entity_B.find_related(_class=ACQUIRIUM_NS.E, alias="e")
    query = entity_BE.find_all_data()

    data = query.data()

    if "b" in data.entity_aliases:
        count = 0
        for entity_uri, group in data.by("b"):
            assert isinstance(entity_uri, str)
            assert isinstance(group, DataObject)
            assert not group.is_empty()
            count += 1
        assert count > 0


def test_by_invalid_entity_raises(acquirium_client_csv):
    """by() with a nonexistent entity alias should raise KeyError."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    with pytest.raises(KeyError):
        list(data.by("nonexistent_entity"))


# ---- dataframe() ----

def test_dataframe_wide(acquirium_client_csv):
    """dataframe() should return a wide DataFrame with alias-named columns."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data.dataframe(shape="wide")
    assert "time" in df.columns
    assert not df.is_empty()

    # Column names should include data aliases (not raw URIs)
    non_time_cols = [c for c in df.columns if c != "time"]
    assert len(non_time_cols) > 0


def test_dataframe_narrow(acquirium_client_csv):
    """dataframe(shape='narrow') should return the enriched tall frame."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data.dataframe(shape="narrow")
    assert "data_alias" in df.columns
    assert "point_uri" in df.columns
    assert "ref_uri" in df.columns
    assert "time" in df.columns
    assert "value_numeric" in df.columns
    assert "value_text" in df.columns


# ---- iter() ----

def test_iter_yields_per_point(acquirium_client_csv):
    """iter() should yield (point_uri, DataFrame) pairs for each unique point."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    count = 0
    for point_uri, df in data.iter(alias):
        assert isinstance(point_uri, str)
        assert "time" in df.columns
        assert "value" in df.columns
        assert not df.is_empty()
        count += 1
    assert count >= 1


# ---- metadata() ----

def test_metadata(acquirium_client_csv):
    """metadata() should return unique rows with alias and URI info."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    meta = data.metadata()
    assert "data_alias" in meta.columns
    assert "point_uri" in meta.columns
    assert "ref_uri" not in meta.columns
    assert not meta.is_empty()

    meta_full = data.metadata(include_ref_uris=True)
    assert "ref_uri" in meta_full.columns


# ---- ref_info ----

def test_ref_info(acquirium_client_csv):
    """ref_info() should return indexed ref URIs."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    refs = data.ref_info(alias)
    assert len(refs) >= 1
    assert refs[0][0] == 0  # first index is 0
    assert isinstance(refs[0][1], str)


# ---- latest() ----

def test_latest(acquirium_client_csv):
    """latest() should return the most recent row for an alias."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    latest = data.latest(alias)
    assert not latest.is_empty()
    assert latest.shape[0] == 1
    assert "time" in latest.columns
    assert "value" in latest.columns


# ---- Edge cases ----

def test_empty_data_object(acquirium_client_csv):
    """A query with no data nodes should produce an empty DataObject."""
    acq = acquirium_client_csv
    query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    # No data nodes — just an entity query
    data = query.data()

    assert data.is_empty()
    assert data.aliases == []
    assert data.entity_aliases == []
    df = data.dataframe()
    assert df.is_empty()


def test_single_data_node_no_entity(acquirium_client_csv):
    """find_all_data with no prior entities should still work."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(limit=5, order="desc")

    assert not data.is_empty()
    # With find_all_data (no entity), entity_aliases should be empty
    # (there are no non-data nodes to group by)


def test_data_with_entity_context(acquirium_client_csv):
    """Query with entity + data should produce entity columns."""
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    data_query = entity_B.find_data(alias="b_data")
    data = data_query.data(limit=5)

    assert not data.is_empty()
    assert "b_data" in data.aliases

    # Should have entity context
    if data.entity_aliases:
        assert "b" in data.entity_aliases


def test_query_data_method_exists(acquirium_client_csv):
    """Query.data() method should exist and return a DataObject."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()
    assert isinstance(data, DataObject)


# ---- reconcile() ----

def test_reconcile_basic_shape(acquirium_client_csv):
    """reconcile() returns a wide frame: a shared uniform 'time' column plus
    one '<point>_reconciled' column per reconciled series."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile()
    assert "time" in df.columns
    assert not df.is_empty()

    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    assert len(reconciled_cols) > 0

    # the time column must be a sorted, uniform grid
    times = df["time"].to_list()
    assert times == sorted(times)
    if len(times) > 2:
        step = times[1] - times[0]
        assert all(b - a == step for a, b in zip(times, times[1:]))


def test_reconcile_points_subset(acquirium_client_csv):
    """Passing `points` restricts reconciliation to those aliases only."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    alias = data.aliases[0]
    df = data.reconcile([alias])
    non_time_cols = [c for c in df.columns if c != "time"]
    assert non_time_cols
    assert all(c.startswith(alias) for c in non_time_cols)


def test_reconcile_unknown_alias_raises(acquirium_client_csv):
    """An alias not present in the DataObject should raise ValueError."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(["definitely_not_a_real_alias"])


def test_reconcile_invalid_upsample_raises(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(upsample="not-a-real-method")


def test_reconcile_invalid_downsample_raises(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(downsample="not-a-real-method")


def test_reconcile_default_upsample_is_interpolate(acquirium_client_csv):
    """With no explicit upsample, empty buckets should be linearly
    interpolated — same result as passing upsample='interpolate' explicitly."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df_default = data.reconcile(resolution=timedelta(minutes=1))
    df_explicit = data.reconcile(resolution=timedelta(minutes=1), upsample="interpolate")
    assert df_default.equals(df_explicit)


def test_reconcile_upsample_null_leaves_gaps(acquirium_client_csv):
    """upsample='null' should leave empty buckets as null (no fill applied)."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    # Force a resolution finer than at least one series' native sampling so
    # some buckets are guaranteed to be empty.
    df = data.reconcile(resolution=timedelta(minutes=1), upsample="null")
    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    assert reconciled_cols
    assert any(df[c].null_count() > 0 for c in reconciled_cols)


def test_reconcile_default_value_requires_fill_value(acquirium_client_csv):
    """upsample='default_value' without fill_value should raise."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(upsample="default_value")


def test_reconcile_default_value_fill(acquirium_client_csv):
    """upsample='default_value' should fill empty buckets with fill_value
    ('zero' is just its 0 special case)."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(minutes=1), upsample="default_value", fill_value=-999.0)
    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    for c in reconciled_cols:
        assert df[c].null_count() == 0
    assert any(-999.0 in df[c].to_list() for c in reconciled_cols)


def test_reconcile_explicit_resolution_and_methods(acquirium_client_csv):
    """An explicit resolution should set the grid spacing; zero-fill upsample
    should leave no nulls."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(hours=2), downsample="mean", upsample="zero")
    assert "time" in df.columns

    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    for c in reconciled_cols:
        assert df[c].null_count() == 0

    times = df["time"].to_list()
    if len(times) > 1:
        assert (times[1] - times[0]) == timedelta(hours=2)


def test_reconcile_resolution_string(acquirium_client_csv):
    """String durations (e.g. '2h') should parse the same as an equivalent timedelta."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df_str = data.reconcile(resolution="2h", upsample="zero")
    df_td = data.reconcile(resolution=timedelta(hours=2), upsample="zero")
    assert df_str.shape == df_td.shape


def test_reconcile_empty_data_object(acquirium_client_csv):
    """reconcile() on an empty DataObject should return just a time column."""
    acq = acquirium_client_csv
    query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    data = query.data()

    df = data.reconcile()
    assert df.columns == ["time"]
    assert df.is_empty()
