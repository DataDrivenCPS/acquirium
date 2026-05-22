from pathlib import Path
from unittest.mock import MagicMock

from acquirium.Storage import graph_store as graph_store_module


class _FakeDataset:
    def __init__(self) -> None:
        self.store = MagicMock()

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


def _build_store(tmp_path: Path, monkeypatch, *, source_ready: bool):
    opened_paths: list[Path] = []
    datasets = [_FakeDataset(), _FakeDataset()]

    def fake_open_dataset(path: Path):
        opened_paths.append(path)
        return datasets[len(opened_paths) - 1], path

    monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_open_dataset", staticmethod(fake_open_dataset))
    monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_ensure_query_cache_current", lambda self: None)

    ontoenv_instances = []

    class FakeOntoEnv:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs
            self.update = MagicMock()
            ontoenv_instances.append(self)

    monkeypatch.setattr(graph_store_module, "OntoEnv", FakeOntoEnv)

    store_path = tmp_path / "store"
    env_root = tmp_path / "env"
    source_path = store_path / "source"
    source_path.mkdir(parents=True)
    env_root.mkdir(parents=True)

    if source_ready:
        (source_path / "000001.sst").write_bytes(b"sst")
        (source_path / "acquirium_source_state.json").write_text('{"version": 1}')
        (env_root / ".ontoenv").mkdir()

    store = graph_store_module.OxigraphGraphStore(
        store_path=store_path,
        env_root=env_root,
    )
    return store, ontoenv_instances[0]


def test_init_from_store_skips_ontoenv_update(tmp_path, monkeypatch):
    store, env = _build_store(tmp_path, monkeypatch, source_ready=True)
    assert env.kwargs["init_from_store"] is True
    env.update.assert_not_called()
    store.close()


def test_cold_start_runs_ontoenv_update(tmp_path, monkeypatch):
    store, env = _build_store(tmp_path, monkeypatch, source_ready=False)
    assert env.kwargs["init_from_store"] is False
    env.update.assert_called_once_with()
    store.close()
