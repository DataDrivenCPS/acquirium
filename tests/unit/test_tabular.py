"""Unit tests for the shared tabular reshaping functions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from acquirium import to_observations, to_timestamp


# ------------------------------------------------------------------ timestamps


def test_date_column_becomes_utc_midnight():
    col = pl.Series("time", ["2024-01-01", "2024-01-02"]).str.to_datetime("%Y-%m-%d").cast(pl.Date)
    assert to_timestamp(col).to_list() == [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    ]


def test_naive_datetime_is_read_as_utc():
    col = pl.Series("time", [datetime(2024, 1, 1, 12, 0)])
    got = to_timestamp(col).to_list()[0]
    assert got == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_non_utc_datetime_is_converted():
    col = pl.Series("time", [datetime(2024, 1, 1, 5, 0, tzinfo=timezone(timedelta(hours=-7)))])
    assert to_timestamp(col).to_list()[0] == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_iso_strings_parse_without_a_format():
    col = pl.Series("time", ["2024-01-15", "2024-02-20"])
    assert to_timestamp(col).to_list()[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_explicit_format_wins():
    col = pl.Series("time", ["01/15/2024", "02/20/2024"])
    got = to_timestamp(col, date_format="%m/%d/%Y").to_list()
    assert got == [
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        datetime(2024, 2, 20, tzinfo=timezone.utc),
    ]


def test_common_us_dates_parse_without_a_format():
    col = pl.Series("time", ["01/15/2024", "01/16/2024"])
    assert to_timestamp(col).to_list()[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_separate_date_and_time_columns():
    date = pl.Series("Date", ["12/1/2024"])
    clock = pl.Series("Time", ["5:32:52 PM"])
    assert to_timestamp(date, clock).to_list() == [
        datetime(2024, 12, 1, 17, 32, 52, tzinfo=timezone.utc)
    ]


def test_day_first_resolves_ambiguous_numeric_dates():
    col = pl.Series("time", ["03/04/2024"])
    assert to_timestamp(col, day_first=True).to_list() == [
        datetime(2024, 4, 3, tzinfo=timezone.utc)
    ]


def test_naive_timestamp_uses_configured_timezone():
    col = pl.Series("time", ["2024-01-01 05:00:00"])
    assert to_timestamp(col, timezone="America/Denver").to_list() == [
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    ]


def test_integer_epoch_unit_is_inferred():
    col = pl.Series("time", [1_704_067_200_000])
    assert to_timestamp(col).to_list() == [datetime(2024, 1, 1, tzinfo=timezone.utc)]


def test_all_null_string_column_yields_nulls():
    col = pl.Series("time", [None, None], dtype=pl.Utf8)
    assert to_timestamp(col).to_list() == [None, None]


def test_unparseable_values_become_null():
    col = pl.Series("time", ["2024-01-01", "2024-99-99"])
    assert to_timestamp(col).to_list()[1] is None


# ------------------------------------------------------------------ reshaping


def _wide() -> pl.DataFrame:
    return pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
        "temp": [22.5, 23.0],
        "rh": [55.0, 60.0],
    })


def test_wide_melts_to_observations():
    out = to_observations(_wide(), layout="wide")
    assert out.columns == ["ts", "ref_name", "value"]
    assert set(out["ref_name"].unique()) == {"temp", "rh"}
    assert out.height == 4


def test_narrow_uses_id_and_value_columns():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
        "id": ["sensor/temp", "sensor/rh"],
        "value": [22.5, 55.0],
    })
    out = to_observations(df, layout="narrow")
    assert set(out["ref_name"].to_list()) == {"sensor/temp", "sensor/rh"}


def test_layout_must_be_explicit_when_both_columns_present():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "id": ["a"], "value": [1.0]})
    with pytest.raises(ValueError, match="explicitly set"):
        to_observations(df, layout="auto")


def test_stream_names_are_preserved():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "UV Intensity (mW/cm^2)": [1.5]})
    assert to_observations(df, layout="wide")["ref_name"].to_list() == ["UV Intensity (mW/cm^2)"]


def test_distinct_names_that_used_to_normalize_together_remain_distinct():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z"],
        "a b": [1.0],
        "a_b": [2.0],
    })
    out = to_observations(df, layout="wide")
    assert set(out["ref_name"].to_list()) == {"a b", "a_b"}


def test_narrow_stream_names_are_preserved():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z"], "id": ["UV Intensity (mW/cm^2)"], "value": [1.5],
    })
    assert to_observations(df, layout="narrow")["ref_name"].to_list() == ["UV Intensity (mW/cm^2)"]


def test_null_and_nan_values_are_dropped():
    df = pl.DataFrame({
        "time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
        "temp": [float("nan"), 23.0],
    })
    assert to_observations(df, layout="wide")["value"].to_list() == ["23.0"]


def test_rows_with_unparseable_timestamps_are_dropped():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z", "2024-99-99"], "temp": [1.0, 2.0]})
    assert to_observations(df, layout="wide").height == 1


def test_missing_time_column_raises():
    with pytest.raises(ValueError, match="identify timestamp"):
        to_observations(pl.DataFrame({"when": ["2024-01-01"], "temp": [1.0]}), layout="wide")


def test_common_ts_name_is_discovered():
    out = to_observations(
        pl.DataFrame({"ts": ["2024-01-01"], "temp": [1.0]}), layout="wide"
    )
    assert out["ref_name"].to_list() == ["temp"]


def test_split_date_time_names_are_discovered_and_not_streams():
    out = to_observations(pl.DataFrame({
        "Date": ["12/1/2024"],
        "Time": ["5:32:52 PM"],
        "temp": [1.0],
    }), layout="wide")
    assert out["ts"].to_list() == [datetime(2024, 12, 1, 17, 32, 52, tzinfo=timezone.utc)]
    assert out["ref_name"].to_list() == ["temp"]


def test_prefixed_date_time_pair_is_discovered():
    out = to_observations(pl.DataFrame({
        "Sample Date": ["2024-01-01"],
        "Sample Time": ["12:30:00"],
        "temp": [1.0],
    }), layout="wide")
    assert out["ts"].to_list() == [datetime(2024, 1, 1, 12, 30, tzinfo=timezone.utc)]


def test_missing_narrow_column_raises():
    df = pl.DataFrame({"time": ["2024-01-01T00:00:00Z"], "value": [1.0]})
    with pytest.raises(ValueError, match="column 'id'"):
        to_observations(df, layout="narrow")


def test_custom_column_names():
    df = pl.DataFrame({
        "timestamp": ["2024-01-01T00:00:00Z"], "tag": ["a"], "reading": [1.5],
    })
    out = to_observations(
        df, time_col="timestamp", id_col="tag", value_col="reading", layout="narrow"
    )
    assert out["ref_name"].to_list() == ["a"]
    assert out["value"].to_list() == ["1.5"]
