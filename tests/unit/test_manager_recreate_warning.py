from __future__ import annotations

from acquirium.Server.manager import RECREATE_WARNING


def test_recreate_warning_is_explicit_about_repeat_deletion():
    assert "recreate=True" in RECREATE_WARNING
    assert "erased" in RECREATE_WARNING
    assert "erase data again" in RECREATE_WARNING
