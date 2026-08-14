"""Unit tests for the shared tabular reshaping functions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from acquirium.Drivers.BuiltInDrivers.tabular import parse_timestamps, to_observations


# ------------------------------------------------------------------ timestamps


def test_date_column_becomes_utc_midnight():
    col = pl.Series("time", ["2024-01-01", "2024-01-02"]).str.to_datetime("%Y-%m-%d").cast(pl.Date)
    assert parse_timestamps(col).to_list() == [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    ]


def test_naive_datetime_is_read_as_utc():
    col = pl.Series("time", [datetime(2024, 1, 1, 12, 0)])
    got = parse_timestamps(col).to_list()[0]
    assert got == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_non_utc_datetime_is_converted():
    col = pl.Series("time", [datetime(2024, 1, 1, 5, 0, tzinfo=timezone(timedelta(hours=-7)))])
    assert parse_timestamps(col).to_list()[0] == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_iso_strings_parse_without_a_format():
    col = pl.Series("time", ["2024-01-15", "2024-02-20"])
    assert parse_timestamps(col).to_list()[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_explicit_format_wins():
    col = pl.Series("time", ["01/15/2024", "02/20/2024"])
    got = parse_timestamps(col, "%m/%d/%Y").to_list()
    assert got == [
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        datetime(2024, 2, 20, tzinfo=timezone.utc),
    ]


def test_fallback_formats_used_when_none_given():
    """US-style dates parse even with no date_format configured."""
    col = pl.Series("time", ["01/15/2024", "01/16/2024"])
    assert parse_timestamps(col).to_list()[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_all_null_string_column_yields_nulls():
    col = pl.Series("time", [None, None], dtype=pl.Utf8)
    assert parse_timestamps(col).to_list() == [None, None]


def test_unparseable_values_become_null():
    col = pl.Series("time", ["2024-01-01", "not a date"])
    assert parse_timestamps(col).to_list()[1] is None


# ------------------------------------------------------------------ reshaping


def _wide() -> pl.DataFrame:
    return pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
        "temp": [22.5, 23.0],
        "rh": [55.0, 60.0],
    })


def test_wide_melts_to_observations():
    out = to_observations(_wide())
    assert out.columns == ["ts", "ref_name", "value"]
    assert set(out["ref_name"].unique()) == {"temp", "rh"}
    assert out.height == 4


def test_narrow_uses_id_and_value_columns():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
        "id": ["sensor/temp", "sensor/rh"],
        "value": [22.5, 55.0],
    })
    out = to_observations(df)
    assert set(out["ref_name"].to_list()) == {"sensor/temp", "sensor/rh"}


def test_layout_auto_prefers_narrow_when_both_columns_present():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "id": ["a"], "value": [1.0]})
    assert to_observations(df)["ref_name"].to_list() == ["a"]
    # forcing wide treats id/value as ordinary stream columns
    forced = to_observations(df, layout="wide")
    assert set(forced["ref_name"].to_list()) == {"id", "value"}


def test_stream_names_are_normalised():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "UV Intensity (mW/cm^2)": [1.5]})
    assert to_observations(df)["ref_name"].to_list() == ["UV_Intensity_mW/cm^2"]


def test_narrow_stream_names_are_normalised():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z"], "id": ["UV Intensity (mW/cm^2)"], "value": [1.5],
    })
    assert to_observations(df)["ref_name"].to_list() == ["UV_Intensity_mW/cm^2"]


def test_null_and_nan_values_are_dropped():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
        "temp": [float("nan"), 23.0],
    })
    assert to_observations(df)["value"].to_list() == ["23.0"]


def test_rows_with_unparseable_timestamps_are_dropped():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z", "nope"], "temp": [1.0, 2.0]})
    assert to_observations(df).height == 1


def test_missing_time_column_raises():
    with pytest.raises(ValueError, match="time column"):
        to_observations(pl.DataFrame({"ts": ["2024-01-01"], "temp": [1.0]}))


def test_missing_narrow_column_raises():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "value": [1.0]})
    with pytest.raises(ValueError, match="column 'id'"):
        to_observations(df, layout="narrow")


def test_custom_column_names():
    df = pl.DataFrame({
        "timestamp": ["2024-01-01T00:00:00Z"], "tag": ["a"], "reading": [1.5],
    })
    out = to_observations(df, time_col="timestamp", id_col="tag", value_col="reading")
    assert out["ref_name"].to_list() == ["a"]
    assert out["value"].to_list() == ["1.5"]
