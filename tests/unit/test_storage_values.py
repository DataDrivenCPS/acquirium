from __future__ import annotations

import polars as pl

from acquirium.Storage.values import (
    assign_stream_value_kind,
    normalize_value_kind,
    normalize_value_mode,
    prepare_value_columns,
    split_value,
)


def test_normalize_value_kind_defaults_to_text():
    assert normalize_value_kind(None) == "text"
    assert normalize_value_kind("") == "text"
    assert normalize_value_kind("unknown") == "text"


def test_normalize_value_mode_accepts_documented_modes():
    assert normalize_value_mode(None) == "default"
    assert normalize_value_mode("default") == "default"
    assert normalize_value_mode("coalesce") == "coalesce"
    assert normalize_value_mode("numeric") == "numeric"
    assert normalize_value_mode("text") == "text"


def test_normalize_value_mode_rejects_legacy_aliases():
    for value_mode in ("registered", "stream", "value", "float", ""):
        try:
            normalize_value_mode(value_mode)
        except ValueError:
            continue
        raise AssertionError(f"expected {value_mode!r} to be rejected")


def test_assign_stream_value_kind_uses_numeric_when_any_numeric_value_is_observed():
    assert assign_stream_value_kind([1, "Manual Control"]) == "numeric"


def test_assign_stream_value_kind_defaults_blank_streams_to_text():
    assert assign_stream_value_kind([None, " "]) == "text"


def test_assign_stream_value_kind_can_treat_numeric_strings_as_text():
    assert (
        assign_stream_value_kind(["1.0", "2.0"], parse_numeric_strings=False)
        == "text"
    )


def test_split_numeric_falls_back_to_text_when_float_conversion_fails():
    assert split_value("Manual Control", "numeric") == (None, "Manual Control")


def test_split_value_numeric_stream_still_stores_parseable_values_as_numeric():
    assert split_value("1.25", "numeric") == (1.25, None)


def test_split_value_numeric_nan_is_null():
    assert split_value(float("nan"), "numeric") == (None, None)


def test_prepare_value_columns_allows_text_rows_in_numeric_stream():
    df = pl.DataFrame(
        {
            "ref_uri": ["urn:test:mixed", "urn:test:mixed"],
            "ts": [1, 2],
            "value": ["1.5", "Manual Control"],
            "value_kind": ["numeric", "numeric"],
        }
    )

    prepared = prepare_value_columns(df)

    assert prepared.get_column("numeric_value").to_list() == [1.5, None]
    assert prepared.get_column("text_value").to_list() == [None, "Manual Control"]
