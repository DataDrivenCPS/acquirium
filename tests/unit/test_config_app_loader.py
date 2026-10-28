"""File specs in ``[[apps]]`` import by file stem, so a stem must name one file."""
import sys
from pathlib import Path

import pytest

from acquirium.Server.app import _load_config_app_target

_APP_SOURCE = "class {name}:\n    pass\n"


def test_config_app_files_with_one_stem_in_two_directories_are_rejected(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    for folder, name in (("a", "First"), ("b", "Second")):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "shared_stem_transform.py").write_text(_APP_SOURCE.format(name=name))
    try:
        first = _load_config_app_target("a/shared_stem_transform.py:First", base_dir=tmp_path)
        # Reloading the same file is fine; it names the same module.
        assert _load_config_app_target("a/shared_stem_transform.py:First", base_dir=tmp_path) is first
        with pytest.raises(ValueError, match="would import as 'shared_stem_transform'"):
            _load_config_app_target("b/shared_stem_transform.py:Second", base_dir=tmp_path)
    finally:
        sys.modules.pop("shared_stem_transform", None)
    # The app directory is searched after installed modules, never before.
    assert sys.path.index(str(tmp_path / "a")) == len(sys.path) - 1


def test_config_app_file_named_like_an_installed_module_is_rejected(tmp_path: Path, monkeypatch):
    import colorsys

    monkeypatch.setattr(sys, "path", list(sys.path))
    (tmp_path / "colorsys.py").write_text(_APP_SOURCE.format(name="Shadow"))
    with pytest.raises(ValueError, match="would import as 'colorsys'"):
        _load_config_app_target("./colorsys.py:Shadow", base_dir=tmp_path)
    assert sys.modules["colorsys"] is colorsys
    assert str(tmp_path) not in sys.path
