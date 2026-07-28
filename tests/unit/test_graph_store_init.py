from pathlib import Path
from unittest.mock import MagicMock

from acquirium.Storage import graph_store as graph_store_module


class _FakeDataset:
    def __init__(self, *, has_graphs: bool) -> None:
        self.store = MagicMock()
        self._graphs = [object()] if has_graphs else []

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None

    def graphs(self):
        return iter(self._graphs)


def _build_store(tmp_path: Path, monkeypatch, *, source_ready: bool, adopt=None):
    opened_paths: list[Path] = []
    datasets = [
        _FakeDataset(has_graphs=source_ready),
        _FakeDataset(has_graphs=False),
    ]

    def fake_open_dataset(path: Path):
        opened_paths.append(path)
        return datasets[len(opened_paths) - 1], path

    monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_open_dataset", staticmethod(fake_open_dataset))

    ontoenv_instances = []

    class FakeOntoEnv:
        def __init__(self, lifecycle: str, *args, **kwargs):
            # Which of ontoenv's lifecycle entry points built this env.
            self.lifecycle = lifecycle
            self.args = args
            self.kwargs = kwargs
            self.update = MagicMock()
            # cold start path now calls env.add per bundled file
            self.add = MagicMock()
            self.close = MagicMock()
            ontoenv_instances.append(self)

        @classmethod
        def connect(cls, *args, **kwargs):
            return cls("connect", *args, **kwargs)

        @classmethod
        def adopt(cls, *args, **kwargs):
            if adopt is not None:
                return adopt(cls, *args, **kwargs)
            return cls("adopt", *args, **kwargs)

    monkeypatch.setattr(graph_store_module, "OntoEnv", FakeOntoEnv)

    store_path = tmp_path / "store"
    env_root = tmp_path / "env"
    source_path = store_path / "source"
    source_path.mkdir(parents=True)
    env_root.mkdir(parents=True)

    if source_ready:
        (source_path / "acquirium_source_state.json").write_text('{"version": 1}')

    store = graph_store_module.OxigraphGraphStore(
        store_path=store_path,
        env_root=env_root,
    )
    return store, ontoenv_instances[0], opened_paths


def test_warm_start_adopts_populated_store(tmp_path, monkeypatch):
    """A populated store must be adopted, not re-populated: ontoenv indexes
    what the store already holds instead of starting from an empty catalog."""
    store, env, opened_paths = _build_store(tmp_path, monkeypatch, source_ready=True)
    assert opened_paths == [tmp_path / "store" / "source", tmp_path / "store" / "query"]
    assert env.lifecycle == "adopt"
    env.update.assert_not_called()
    env.add.assert_not_called()
    store.close()


def test_warm_start_falls_back_to_connect_when_catalog_exists(tmp_path, monkeypatch):
    """adopt() refuses when a catalog survived the last run; reopening it
    with connect() is the right move, and the store still counts as warm."""
    def _adopt(cls, *args, **kwargs):
        raise FileExistsError("OntoEnv catalog already exists")

    store, env, _ = _build_store(
        tmp_path, monkeypatch, source_ready=True, adopt=_adopt
    )
    assert env.lifecycle == "connect"
    env.add.assert_not_called()
    store.close()


def test_cold_start_adds_bundled_ontologies(tmp_path, monkeypatch):
    """Cold start must connect to an empty env and register each bundled
    ontology via env.add() — no directory crawl (env.update is no longer
    the population path)."""
    from acquirium._ontologies import BUNDLED_FILES

    store, env, opened_paths = _build_store(tmp_path, monkeypatch, source_ready=False)
    assert opened_paths == [tmp_path / "store" / "source", tmp_path / "store" / "query"]
    assert env.lifecycle == "connect"
    env.update.assert_not_called()
    assert env.add.call_count == len(BUNDLED_FILES)
    store.close()


def test_close_releases_the_ontoenv_lock(tmp_path, monkeypatch):
    """ontoenv holds an exclusive lock for the environment's lifetime, so
    close() has to release it alongside the datasets."""
    store, env, _ = _build_store(tmp_path, monkeypatch, source_ready=False)
    store.close()
    env.close.assert_called_once()
