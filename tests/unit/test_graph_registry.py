import pytest

from acquirium.Storage.graph_registry import (
    PLANT_GRAPH_URI,
    PLANT_SOURCE_ID,
    SOURCE_GRAPH_PREFIX,
    source_graph_uri,
)


def test_source_graph_uri_is_deterministic():
    first = source_graph_uri("driver/a")
    second = source_graph_uri("driver/a")

    assert first == second
    assert str(first) == f"{SOURCE_GRAPH_PREFIX}driver%2Fa"


def test_plant_source_id_maps_to_the_plant_graph():
    assert str(source_graph_uri(PLANT_SOURCE_ID)) == PLANT_GRAPH_URI


def test_empty_source_id_is_rejected():
    with pytest.raises(ValueError, match="source_id must not be empty"):
        source_graph_uri("")
