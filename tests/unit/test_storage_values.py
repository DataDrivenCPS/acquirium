from __future__ import annotations

from decimal import Decimal

from acquirium.Storage.values import infer_value_kind, normalize_value_kind, split_value


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


def test_split_value_keeps_unparseable_numeric_samples_as_text():
    assert split_value("manual_override", "numeric") == (None, "manual_override")


def test_split_value_numeric_stream_still_stores_parseable_values_as_numeric():
    assert split_value("1.25", "numeric") == (1.25, None)


def test_split_value_numeric_nan_is_null():
    assert split_value(float("nan"), "numeric") == (None, None)
