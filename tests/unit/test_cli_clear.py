"""`acquirium clear`: erase a data folder without a server."""

import pyoxigraph
import pytest
from typer.testing import CliRunner

from acquirium import cli
from acquirium.cli import app


def _clear(*args, **kwargs):
    return CliRunner().invoke(app, ["clear", *map(str, args)], **kwargs)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    # Run from an empty folder so no ./acquirium.toml is discovered.
    monkeypatch.chdir(tmp_path)
    folder = tmp_path / ".acquirium"
    (folder / ".oxigraph").mkdir(parents=True)
    (folder / ".runtime").mkdir()
    (folder / "escapement-epoch.duckdb").write_bytes(b"data")
    (folder / "embedding_cache" / "qudt").mkdir(parents=True)
    (folder / "embedding_cache" / "qudt" / "x_vectors.npz").write_bytes(b"vectors")
    return folder


def test_clears_the_data_and_keeps_the_embeddings(data_dir):
    result = _clear("--yes")

    assert result.exit_code == 0, result.output
    assert "kept the embeddings" in result.output
    assert [entry.name for entry in data_dir.iterdir()] == ["embedding_cache"]
    assert (data_dir / "embedding_cache" / "qudt" / "x_vectors.npz").exists()


def test_all_removes_the_folder(data_dir):
    result = _clear(data_dir, "--all", "--yes")

    assert result.exit_code == 0, result.output
    assert not data_dir.exists()


def test_asks_before_erasing(data_dir):
    declined = _clear(input="n\n")

    assert declined.exit_code == 1
    assert (data_dir / "escapement-epoch.duckdb").exists()

    accepted = _clear(input="y\n")

    assert accepted.exit_code == 0, accepted.output
    assert not (data_dir / "escapement-epoch.duckdb").exists()


def test_a_missing_folder_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = _clear("--yes")

    assert result.exit_code == 0
    assert "Nothing to clear" in result.output


def test_refuses_a_folder_that_is_not_a_data_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    other = tmp_path / "documents"
    other.mkdir()
    (other / "report.txt").write_text("keep me")

    result = _clear(other, "--all", "--yes")

    assert result.exit_code == 1
    assert "does not look like an Acquirium data folder" in result.output
    assert (other / "report.txt").read_text() == "keep me"


def test_refuses_while_a_server_uses_the_folder(data_dir, monkeypatch):
    monkeypatch.setattr(cli, "_server_uses", lambda folder: True)

    result = _clear("--yes")

    assert result.exit_code == 1
    assert "Stop it first" in result.output
    assert (data_dir / "escapement-epoch.duckdb").exists()


def test_an_open_graph_store_counts_as_a_running_server(tmp_path):
    folder = tmp_path / ".acquirium"
    folder.mkdir()
    store = pyoxigraph.Store(str(folder / ".oxigraph"))
    try:
        assert cli._server_uses(folder)
    finally:
        del store
    assert not cli._server_uses(folder)


def test_uses_the_data_dir_of_the_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "acquirium.toml").write_text(
        '[server]\ndata_dir = "store"\nengine_path = "postgresql://localhost/aq"\n'
    )
    (tmp_path / "store" / ".oxigraph").mkdir(parents=True)

    result = _clear("--yes")

    assert result.exit_code == 0, result.output
    assert list((tmp_path / "store").iterdir()) == []
    assert "engine_path uses PostgreSQL. Its data was kept" in result.output
