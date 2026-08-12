from acquirium.Storage.graph_registry import (
    ACQUIRIUM_GRAPH_URI,
    PLANT_GRAPH_URI,
    PLANT_SOURCE_ID,
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


def test_plant_source_id_maps_to_the_existing_plant_graph(tmp_path):
    registry = GraphRegistry(tmp_path / "graph_registry.json")

    plant = registry.source_graph(PLANT_SOURCE_ID)

    assert plant.uri == PLANT_GRAPH_URI
    assert plant.owner == "plant"


def test_empty_source_id_is_rejected(tmp_path):
    registry = GraphRegistry(tmp_path / "graph_registry.json")

    try:
        registry.source_graph("")
    except ValueError as exc:
        assert str(exc) == "source_id must not be empty"
    else:
        raise AssertionError("empty source_id must be rejected")
