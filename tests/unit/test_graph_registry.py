from acquirium.Storage.graph_registry import (
    ACQUIRIUM_GRAPH_URI,
    PLANT_GRAPH_URI,
    GraphRegistry,
)


def test_registry_creates_core_data_graphs(tmp_path):
    registry = GraphRegistry(tmp_path / "graph_registry.json")

    records = registry.data_graphs()

    assert {(record.owner, record.uri) for record in records} == {
        ("plant", PLANT_GRAPH_URI),
        ("acquirium", ACQUIRIUM_GRAPH_URI),
    }


def test_source_graph_is_deterministic_and_persistent(tmp_path):
    path = tmp_path / "graph_registry.json"
    first = GraphRegistry(path).source_graph("driver/a")
    second = GraphRegistry(path).source_graph("driver/a")
    reopened = GraphRegistry(path).source_graph("driver/a")

    assert first == second == reopened
    assert first.uri == "urn:acquirium:graph:data:source:driver%2Fa"


def test_removing_source_does_not_remove_other_data_graphs(tmp_path):
    registry = GraphRegistry(tmp_path / "graph_registry.json")
    removed = registry.source_graph("one")
    retained = registry.source_graph("two")

    assert registry.remove_source("one") == removed
    assert {record.uri for record in registry.data_graphs()} == {
        PLANT_GRAPH_URI,
        ACQUIRIUM_GRAPH_URI,
        retained.uri,
    }


def test_empty_source_id_is_rejected(tmp_path):
    registry = GraphRegistry(tmp_path / "graph_registry.json")

    try:
        registry.source_graph("")
    except ValueError as exc:
        assert str(exc) == "source_id must not be empty"
    else:
        raise AssertionError("empty source_id must be rejected")
