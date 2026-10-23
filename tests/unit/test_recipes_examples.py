"""The recipe files under examples/recipes: they load, and build the queries they claim."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium import Acquirium
from acquirium.Client.explore.core import Query
from acquirium.recipes import Recipes

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "recipes"
FILES = sorted(EXAMPLES.glob("*.py"))
EXPECTED = {
    "plant": ["equipment_in", "measurements_of", "units_in"],
    "flow": ["nearest_measurement", "across"],
    "quality": ["missing_metadata", "gaps", "exceedances"],
}
UTC = timezone.utc


@pytest.fixture
def acq():
    client = Acquirium(health_timeout=None)
    client.client = MagicMock()
    return client


@pytest.fixture
def recipes(acq):
    r = Recipes(acq, builtin=False)
    for path in FILES:
        r.add(path)
    return r


def aliases(query):
    return set(query.query_graph.aliases)


class TestFiles:
    def test_every_file_is_listed_and_loads(self, recipes):
        assert {p.stem for p in FILES} == set(EXPECTED)
        assert sorted(dir(recipes)) == sorted(sum(EXPECTED.values(), []) + ["add", "source"])
        for recipe in recipes:
            assert recipe.description, recipe.name

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
    def test_file_pins_a_version_and_has_a_docstring(self, path):
        text = path.read_text()
        assert text.lstrip().startswith('"""') and 'ACQUIRIUM = ">=' in text

    def test_readme_lists_every_recipe(self):
        readme = (EXAMPLES / "README.md").read_text()
        for stem, names in EXPECTED.items():
            assert f"`{stem}.py`" in readme
            for name in names:
                assert f"`{name}`" in readme


class TestPlant:
    def test_equipment_in(self, recipes):
        q = recipes.equipment_in("urn:process/ozonation")
        sparql = q.to_sparql()
        assert "unit" in aliases(q) and "urn:process/ozonation" in sparql
        assert q.query_graph.nodes[q.query_graph.current_pointer].alias == "unit"

    def test_measurements_of(self, recipes):
        q = recipes.measurements_of("urn:cls/Pump", unit="urn:unit/psi")
        assert {"entity", "stream"} <= aliases(q) and q.query_graph.data_nodes
        assert "urn:unit/psi" in q.to_sparql()
        by_uri = recipes.measurements_of(uri="urn:ex/pump1")
        assert "VALUES ?v0 { <urn:ex/pump1> }" in by_uri.to_sparql()

    def test_units_in(self, recipes, monkeypatch):
        seen = []
        monkeypatch.setattr(Query, "metadata", lambda self, **_: (seen.append(self),
            pl.DataFrame({"stream.unit": ["unit:PSI", None, "unit:GAL", "unit:PSI"]}))[1])
        assert recipes.units_in("urn:ex/system1") == ["unit:GAL", "unit:PSI"]
        assert {"system", "equipment", "stream"} <= aliases(seen[0])
        assert "urn:ex/system1" in seen[0].to_sparql()


class TestFlow:
    def test_nearest_measurement_is_a_directional_nearest_search(self, recipes):
        q = recipes.nearest_measurement("urn:cls/UF", quantity_kind="urn:qk/turbidity", max_depth=4)
        edge = next(e for e in q.query_graph.edges if e.direction)
        assert edge.direction == "downstream" and edge.nearest is True and edge.hops == 4
        assert "stream" in aliases(q) and "urn:qk/turbidity" in q.to_sparql()
        up = recipes.nearest_measurement(uri="urn:ex/uf1", direction="upstream")
        assert next(e for e in up.query_graph.edges if e.direction).direction == "upstream"

    def test_across_returns_upstream_then_downstream(self, recipes):
        up, down = recipes.across("urn:cls/UF", quantity_kind="urn:qk/turbidity")
        assert next(e for e in up.query_graph.edges if e.direction).direction == "upstream"
        assert next(e for e in down.query_graph.edges if e.direction).direction == "downstream"


class TestQuality:
    CATALOG = pl.DataFrame({
        "stream": ["s1", "s2", "s3"], "unit": ["psi", None, "gpm"],
        "quantity_kind": ["pressure", None, None], "host": ["u1", None, "u3"],
    })

    def test_missing_metadata(self, recipes):
        recipes._recipe("missing_metadata").func.__globals__["catalog"] = lambda acq: self.CATALOG
        out = recipes.missing_metadata()
        assert out["stream"].to_list() == ["s2", "s3"]
        assert out["missing"].to_list() == [["unit", "quantity_kind", "host"], ["quantity_kind"]]
        assert recipes.missing_metadata("host")["stream"].to_list() == ["s2"]

    @staticmethod
    def narrow(rows):
        return pl.DataFrame(rows, schema={"time": pl.Datetime("us", "UTC"), "point_id": pl.Utf8,
                                          "value_numeric": pl.Float64}, orient="row")

    def test_gaps(self, acq, recipes, monkeypatch):
        t = lambda h: datetime(2025, 1, 1, h, tzinfo=UTC)
        frame = self.narrow([(t(0), "a", 1.0), (t(1), "a", 1.0), (t(5), "a", 1.0), (t(6), "a", 1.0),
                             (t(0), "b", 1.0), (t(9), "b", 1.0)])
        calls = {}
        monkeypatch.setattr(Query, "dataframe", lambda self, shape="wide", **kw: calls.setdefault("args", (shape, kw)) and frame)
        out = recipes.gaps(acq.query(), longer_than=timedelta(hours=2), start=t(0))
        assert calls["args"] == ("narrow", {"start": t(0), "end": None})
        assert out.to_dicts() == [
            {"stream": "a", "from": t(1), "to": t(5), "duration": timedelta(hours=4)},
            {"stream": "b", "from": t(0), "to": t(9), "duration": timedelta(hours=9)},
        ]

    def test_gaps_without_data(self, acq, recipes, monkeypatch):
        monkeypatch.setattr(Query, "dataframe", lambda self, shape="wide", **kw: pl.DataFrame())
        out = recipes.gaps(acq.query(), longer_than=timedelta(hours=1))
        assert out.height == 0 and out.columns == ["stream", "from", "to", "duration"]

    def test_exceedances(self, acq, recipes, monkeypatch):
        t = lambda h: datetime(2025, 1, 1, h, tzinfo=UTC)
        frame = self.narrow([
            (t(0), "a", 0.1), (t(1), "a", 0.3), (t(2), "a", 0.4), (t(3), "a", 0.1),   # 2h run, peak .4
            (t(4), "a", 0.5), (t(5), "a", 0.1),                                      # 1h run, too short
            (t(0), "b", 0.9), (t(1), "b", 0.9), (t(2), "b", 0.9), (t(3), "b", 0.9),   # 3h run
        ])
        monkeypatch.setattr(Query, "dataframe", lambda self, shape="wide", **kw: frame)
        out = recipes.exceedances(acq.query(), 0.15, longer_than=timedelta(hours=1, minutes=30))
        assert out.to_dicts() == [
            {"stream": "a", "start": t(1), "end": t(2), "duration": timedelta(hours=1), "peak": 0.4},
            {"stream": "b", "start": t(0), "end": t(3), "duration": timedelta(hours=3), "peak": 0.9},
        ][1:]
        short = recipes.exceedances(acq.query(), 0.15, longer_than=timedelta(minutes=30))
        assert short["stream"].to_list() == ["a", "b"] and short["peak"].to_list() == [0.4, 0.9]
