"""include("unit.raw"): the text a built-in value was resolved from."""
from unittest.mock import MagicMock

import polars as pl

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD


def make_client():
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p", "dt"], "rows": [[f"{ATTR_NS}unit.raw", XSD + "string"]]}
        if sparql == DISCOVERY_SPARQL else {"columns": [], "rows": []})
    return client


def test_unit_raw_is_a_leaf_and_unit_stays_the_unit():
    r = Registry(make_client())
    assert "unit.raw" in r and not r.is_group("unit")
    q = Query(client=make_client()).entity("urn:x#A", alias="A").measurement(alias="m").include("unit", "unit.raw")
    s = q.to_sparql()
    assert "?attr1_unit " in s.splitlines()[0] + " " and f"<{ATTR_NS}unit.raw>" in s
    assert q.schema() == pl.Schema({"A": pl.String, "m": pl.String, "m.label": pl.String,
                                    "m.unit": pl.String, "m.unit.raw": pl.String})
