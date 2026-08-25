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


def _build_store(tmp_path: Path, monkeypatch, *, source_ready: bool):
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

        # Only connect() is stubbed on purpose: reaching for one of the
        # narrower lifecycle entry points instead would fail loudly here.
        @classmethod
        def connect(cls, *args, **kwargs):
            return cls("connect", *args, **kwargs)

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


def test_warm_start_reuses_the_populated_store(tmp_path, monkeypatch):
    """A populated store must be reopened, not re-populated: connect()
    reconciles the catalog against what the store already holds, so the
    bundled ontologies are not added a second time."""
    store, env, opened_paths = _build_store(tmp_path, monkeypatch, source_ready=True)
    assert opened_paths == [tmp_path / "store" / "source", tmp_path / "store" / "query"]
    assert env.lifecycle == "connect"
    env.update.assert_not_called()
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


def test_named_graph_returns_a_cached_memory_copy(tmp_path, monkeypatch):
    """named_graph must hand out a materialized in-memory copy, never
    ontoenv's live store-backed view: concurrent iteration of that view
    deadlocks the process (its Rust backend holds a mutex while re-entering
    Python, GIL vs mutex). One copy per IRI is cached until an ontology
    graph changes."""
    from rdflib import Graph, Literal, URIRef

    store, env, _ = _build_store(tmp_path, monkeypatch, source_ready=False)
    source = Graph()
    source.add((URIRef("urn:x#a"), URIRef("urn:x#p"), Literal("v")))
    env.get_graph = MagicMock(return_value=source)

    g1 = store.named_graph("urn:x")
    assert g1 is not source
    assert len(g1) == 1
    # cached: the second call must not touch ontoenv again
    assert store.named_graph("urn:x") is g1
    env.get_graph.assert_called_once()
    # detached: mutating the copy never reaches the ontoenv view
    g1.add((URIRef("urn:x#b"), URIRef("urn:x#p"), Literal("w")))
    assert len(source) == 1

    store._mark_ontology_graph_changed()
    g2 = store.named_graph("urn:x")
    assert g2 is not g1
    assert env.get_graph.call_count == 2
    store.close()


def test_close_releases_the_ontoenv_lock(tmp_path, monkeypatch):
    """ontoenv holds an exclusive lock for the environment's lifetime, so
    close() has to release it alongside the datasets."""
    store, env, _ = _build_store(tmp_path, monkeypatch, source_ready=False)
    store.close()
    env.close.assert_called_once()
