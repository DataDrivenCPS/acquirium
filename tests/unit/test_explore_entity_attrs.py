"""Entity nodes take every attribute; measurement nodes keep their gate."""
from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.attributes import REGISTRY
from acquirium.Client.explore.core import Query


def make_client():
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.sparql_query.return_value = {"columns": ["p", "dt"], "rows": []}
    return client


class TestEntityAttrs:
    def test_include_all_on_entity_is_every_attribute(self):
        q = Query(client=make_client()).entity(uri="urn:p#point", alias="p").include("all")
        names = {n for _, n, _ in q.query_graph.selects}
        assert names == set(REGISTRY) - {"type", "cp_type", "id"}

    def test_explicit_measurement_attributes_on_any_entity(self):
        q = (Query(client=make_client()).entity("urn:x#Class", alias="e")
             .include("unit", "quantity_kind").where(unit="http://qudt.org/vocab/unit/PSI"))
        assert [n for _, n, _ in q.query_graph.selects] == ["unit", "quantity_kind"]

    def test_measurement_node_is_still_gated(self):
        m = Query(client=make_client()).entity("urn:x#Pump").measurement(alias="m")
        with pytest.raises(ValueError, match="does not apply to data node"):
            m.include("process")
        names = {n for _, n, _ in m.include("all").query_graph.selects}
        assert "unit" in names and "process" not in names and "label" not in names

    def test_no_lookup_for_pinned_nodes(self):
        client = make_client()
        Query(client=client).entity(uri="urn:p#point", alias="p").include("all")
        client.node_roles.assert_not_called()
