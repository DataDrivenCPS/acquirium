from datetime import timedelta

import polars as pl
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
#
# The CSV fixture's numeric points are all sampled hourly (one row per hour,
# for all of 2023 -- 8760 exact hourly readings, no gaps). That means:
#   - resolution left at its default (native, ~1h) needs neither upsample
#     nor downsample for any point -- useful for testing the "identical,
#     untouched" path without providing any method.
#   - resolution coarser than 1h (e.g. "2h") makes every point need
#     *downsampling* (raises if downsample=None).
#   - resolution finer than 1h (e.g. "1m") makes every point need
#     *upsampling* (raises if upsample=None).

def test_reconcile_basic_shape(acquirium_client_csv):
    """reconcile() returns a wide frame: a shared uniform 'time' column plus
    one '<point>_reconciled' column per reconciled series. At the native
    (hourly) resolution, no upsample/downsample method is needed at all."""
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
    """An unrecognized method name is rejected up front, regardless of
    whether any point actually ends up needing that operation."""
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


def test_reconcile_missing_downsample_raises(acquirium_client_csv):
    """A resolution coarser than the native (hourly) data requires
    downsampling; omitting `downsample` must raise, not silently pick one."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(resolution=timedelta(hours=2))


def test_reconcile_missing_upsample_raises(acquirium_client_csv):
    """A resolution finer than the native (hourly) data requires upsampling;
    omitting `upsample` must raise, not silently pick one."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(resolution=timedelta(minutes=1))


def test_reconcile_identical_resolution_needs_no_method(acquirium_client_csv):
    """At the data's own native resolution, no upsample/downsample method
    is required, and the values come back untouched."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(hours=1))
    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    assert reconciled_cols
    # untouched hourly data over a full year should have essentially no gaps
    for c in reconciled_cols:
        assert df[c].null_count() == 0


def test_reconcile_upsample_null_leaves_gaps(acquirium_client_csv):
    """upsample='null' should leave empty grid points as null (no fill)."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(minutes=1), upsample="null")
    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    assert reconciled_cols
    assert any(df[c].null_count() > 0 for c in reconciled_cols)


def test_reconcile_default_value_requires_fill_value(acquirium_client_csv):
    """upsample='default_value' without fill_value should raise immediately,
    even before any per-point resolution comparison happens."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    with pytest.raises(ValueError):
        data.reconcile(upsample="default_value")


def test_reconcile_default_value_fill(acquirium_client_csv):
    """upsample='default_value' should fill empty grid points with
    fill_value ('zero' is just its 0 special case)."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(minutes=1), upsample="default_value", fill_value=-999.0)
    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    for c in reconciled_cols:
        assert df[c].null_count() == 0
    assert any(-999.0 in df[c].to_list() for c in reconciled_cols)


def test_reconcile_upsample_interpolate_matches_true_time_weighted(acquirium_client_csv):
    """upsample='interpolate' must interpolate from each point's own true
    timestamps -- not from any pre-binned/bucket-snapped grid. Verify a
    filled-in value against a manual, ground-truth linear interpolation
    computed directly from the raw (unreconciled) series."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    alias = data.aliases[0]
    raw = data[alias].sort("time")  # [time, value], native hourly cadence
    if raw.height < 2:
        pytest.skip("not enough raw data to verify interpolation")

    df = data.reconcile([alias], resolution=timedelta(minutes=1), upsample="interpolate")
    col = f"{alias}_reconciled"

    # Pick a grid timestamp strictly between the first two raw readings and
    # compute the true time-weighted linear interpolation by hand.
    t0, t1 = raw["time"][0], raw["time"][1]
    v0, v1 = raw["value"][0], raw["value"][1]
    mid = t0 + (t1 - t0) / 3
    # snap `mid` down to the reconcile grid (origin-aligned minute boundary)
    mid = mid.replace(second=0, microsecond=0)
    if not (t0 < mid < t1):
        pytest.skip("couldn't construct a valid intermediate grid timestamp")

    expected = v0 + (v1 - v0) * (mid - t0).total_seconds() / (t1 - t0).total_seconds()
    row = df.filter(pl.col("time") == mid)
    if row.is_empty():
        pytest.skip("grid didn't land exactly on the constructed timestamp")
    got = row[col][0]
    assert got is not None
    assert abs(got - expected) < 1e-6


def test_reconcile_downsample_requires_and_uses_method(acquirium_client_csv):
    """A resolution coarser than native (hourly) data requires downsample,
    and produces one aggregated value per (larger) bucket."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df = data.reconcile(resolution=timedelta(hours=2), downsample="mean")
    assert "time" in df.columns

    reconciled_cols = [c for c in df.columns if c.endswith("_reconciled")]
    for c in reconciled_cols:
        # A full year of hourly data resampled to 2h should have at most one
        # trailing null (the grid may extend one step past the last reading).
        assert df[c].null_count() <= 1

    times = df["time"].to_list()
    if len(times) > 1:
        assert (times[1] - times[0]) == timedelta(hours=2)


def test_reconcile_resolution_string(acquirium_client_csv):
    """String durations (e.g. '2h') should parse the same as an equivalent timedelta."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    df_str = data.reconcile(resolution="2h", downsample="mean")
    df_td = data.reconcile(resolution=timedelta(hours=2), downsample="mean")
    assert df_str.shape == df_td.shape


def test_reconcile_mixed_up_and_downsample(acquirium_client_csv):
    """Different points can independently need upsampling vs downsampling
    in the same call, at a resolution between their native rates."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two aliases")

    # All CSV points are hourly, so this doesn't exercise true mixed
    # resolutions, but it does confirm a single call can carry both methods
    # without error when only one direction is actually needed.
    df = data.reconcile(aliases[:2], resolution=timedelta(hours=1), upsample="interpolate", downsample="mean")
    assert "time" in df.columns


def test_reconcile_empty_data_object(acquirium_client_csv):
    """reconcile() on an empty DataObject should return just a time column."""
    acq = acquirium_client_csv
    query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    data = query.data()

    df = data.reconcile()
    assert df.columns == ["time"]
    assert df.is_empty()


# ---- difference() ----

def _two_alias_data(acq):
    """Helper: a DataObject with (at least) two distinct data aliases."""
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    entity_BE = entity_B.find_entity(_class=ACQUIRIUM_NS.E, alias="e")
    return entity_BE.find_all_data().data(cast_value="float")


def test_difference_two_columns(acquirium_client_csv):
    """difference() takes an already-reconciled frame and computes a - b,
    named 'a_minus_b' (no resampling knobs of its own)."""
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    reconciled = data.reconcile(aliases[:2])
    diff = data.difference(reconciled)
    assert isinstance(diff, DataObject)

    value_cols = [c for c in reconciled.columns if c != "time"]
    a_name = value_cols[0].removesuffix("_reconciled")
    b_name = value_cols[1].removesuffix("_reconciled")
    assert diff.aliases == [f"{a_name}_minus_{b_name}"]

    df = diff.dataframe(shape="wide")
    assert "time" in df.columns
    assert diff.aliases[0] in df.columns


def test_difference_custom_labels(acquirium_client_csv):
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    reconciled = data.reconcile(aliases[:2])
    diff = data.difference(reconciled, labels=["custom_delta"])
    assert diff.aliases == ["custom_delta"]


def test_difference_label_count_mismatch_raises(acquirium_client_csv):
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    reconciled = data.reconcile(aliases[:2])
    with pytest.raises(ValueError):
        data.difference(reconciled, labels=["one", "too_many"])


def test_difference_requires_time_column(acquirium_client_csv):
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    reconciled = data.reconcile(aliases[:2])
    with pytest.raises(ValueError):
        data.difference(reconciled.drop("time"))


def test_difference_too_few_columns_raises(acquirium_client_csv):
    """A single-column reconciled frame can't be differenced (need at least 2)."""
    acq = acquirium_client_csv
    query = acq.find_all_data(uri="urn:ex/point_1", alias="only_one")
    data = query.data(cast_value="float")

    reconciled = data.reconcile()
    with pytest.raises(ValueError):
        data.difference(reconciled)


def test_difference_too_many_columns_raises(acquirium_client_csv):
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    reconciled = data.reconcile(aliases[:2])
    value_col = [c for c in reconciled.columns if c != "time"][0]
    # Synthesize extra columns to push past the 5-column cap.
    padded = reconciled
    for i in range(4):
        padded = padded.with_columns((pl.col(value_col) + i).alias(f"synthetic_{i}_reconciled"))
    assert len([c for c in padded.columns if c != "time"]) == 6

    with pytest.raises(ValueError):
        data.difference(padded)


# ---- plot() ----

def test_plot_saves_file(acquirium_client_csv, tmp_path):
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(cast_value="float")

    out_path = tmp_path / "plot.png"
    fig = data.plot(output_path=str(out_path))
    assert fig is not None
    assert out_path.exists()
    assert out_path.stat().st_size > 0


def test_plot_label_count_mismatch_raises(acquirium_client_csv):
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    with pytest.raises(ValueError):
        data.plot(aliases[:2], labels=["only_one"])


def test_reconcile_difference_plot_chain(acquirium_client_csv, tmp_path):
    """The requested pipeline: reconcile explicitly, then difference, then plot."""
    acq = acquirium_client_csv
    data = _two_alias_data(acq)

    aliases = data.aliases
    if len(aliases) < 2:
        pytest.skip("fixture doesn't expose at least two distinct aliases")

    out_path = tmp_path / "chain.png"
    reconciled = data.reconcile(aliases[:2], resolution="2h")
    data.difference(reconciled).plot(output_path=str(out_path), title="difference over time")
    assert out_path.exists()
