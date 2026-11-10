import escapement

import acquirium.functions as functions


def test_calculation_helpers_are_escapement_helpers():
    for name in functions.__all__:
        assert getattr(functions, name) is getattr(escapement, name)
