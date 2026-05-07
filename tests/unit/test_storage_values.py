from __future__ import annotations

from decimal import Decimal

import polars as pl

from acquirium.Storage.values import (
    assign_stream_value_kind,
    infer_value_kind,
    normalize_value_kind,
    prepare_value_columns,
    split_value,
)


def test_normalize_value_kind_defaults_to_text():
    assert normalize_value_kind(None) == "text"
    assert normalize_value_kind("") == "text"
    assert normalize_value_kind("unknown") == "text"


def test_infer_value_kind_numeric_native_values():
    assert infer_value_kind([1, 2.5, Decimal("3.0")]) == "numeric"


def test_infer_value_kind_text_wins_over_numeric():
    assert infer_value_kind([1, "ON"]) == "text"


def test_infer_value_kind_numeric_strings_by_default():
    assert infer_value_kind(["1", "2.5", " "]) == "numeric"


def test_infer_value_kind_can_treat_strings_as_text():
    assert infer_value_kind(["1", "2.5"], parse_numeric_strings=False) == "text"


def test_infer_value_kind_unknown_default_for_blank_stream():
    assert infer_value_kind([None, " "]) == "unknown"
    assert infer_value_kind([None, " "], unknown_default="numeric") == "numeric"


def test_assign_stream_value_kind_uses_numeric_when_any_numeric_value_is_observed():
    assert assign_stream_value_kind([1, "Manual Control"]) == "numeric"


def test_assign_stream_value_kind_uses_unknown_default_for_blank_streams():
    assert assign_stream_value_kind([None, " "]) == "numeric"
    assert assign_stream_value_kind([None, " "], unknown_default="text") == "text"


def test_assign_stream_value_kind_can_treat_numeric_strings_as_text():
    assert (
        assign_stream_value_kind(["1.0", "2.0"], parse_numeric_strings=False)
        == "text"
    )


def test_split_numeric_falls_back_to_text_when_float_conversion_fails():
    assert split_value("Manual Control", "numeric") == (None, "Manual Control")


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
