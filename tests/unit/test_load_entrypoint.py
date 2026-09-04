import sys
from pathlib import Path

import pytest

from acquirium.Materialization.worker import load_entrypoint

SOURCE = """
import acquirium as aq


class Probe(aq.App):
    name = "probe"
    marker = {marker!r}
    outputs = {{"out": aq.output.per_input(value_kind="numeric")}}

    def build_query(self, plant):
        return plant.query().measurement(alias="input")

    def transform(self, inputs, output, context):
        pass
"""


@pytest.fixture
def app_dir(tmp_path: Path, monkeypatch):
    (tmp_path / "probe_app.py").write_text(SOURCE.format(marker="first"))
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    sys.modules.pop("probe_app", None)


def test_an_edited_app_is_reloaded_rather_than_served_from_cache(app_dir: Path):
    first = load_entrypoint("probe_app:Probe")
    assert first.marker == "first"

    (app_dir / "probe_app.py").write_text(SOURCE.format(marker="second"))

    # The module is already imported, so without a freshness check the cached
    # class would come back while the digest — read from disk — moved on.
    second = load_entrypoint("probe_app:Probe")
    assert second.marker == "second"


def test_search_path_loads_a_module_the_server_cannot_otherwise_import(tmp_path: Path):
    hidden = tmp_path / "hidden"
    hidden.mkdir()
    (hidden / "hidden_app.py").write_text(SOURCE.format(marker="hidden"))
    try:
        with pytest.raises(ModuleNotFoundError, match="could not import"):
            load_entrypoint("hidden_app:Probe")

        target = load_entrypoint("hidden_app:Probe", None, str(hidden))
        assert target.marker == "hidden"
    finally:
        sys.modules.pop("hidden_app", None)


def test_search_path_wins_over_a_same_named_module_already_imported(app_dir: Path, tmp_path: Path):
    assert load_entrypoint("probe_app:Probe").marker == "first"
    other = tmp_path / "other"
    other.mkdir()
    (other / "probe_app.py").write_text(SOURCE.format(marker="other"))

    # Same module name, different file: the named directory decides.
    assert load_entrypoint("probe_app:Probe", None, str(other)).marker == "other"


def test_missing_module_names_the_server_as_the_importer():
    with pytest.raises(ModuleNotFoundError) as error:
        load_entrypoint("no_such_app_module:Probe")

    message = str(error.value)
    assert "the server could not import" in message
    assert "importable by the server" in message
