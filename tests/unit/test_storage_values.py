from __future__ import annotations

from decimal import Decimal

from acquirium.Storage.values import infer_value_kind


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
