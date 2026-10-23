"""The shipped recipes: the queries they build and the frames they shape.

The server is replaced by patching ``Query.metadata``, ``Query.data`` and
``Query.dataframe``; each test asserts on the query graphs the recipe built
and on the frame it returned.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

import acquirium.recipes
from acquirium import Acquirium
from acquirium.Client.data_object import BindingInfo
from acquirium.Client.explore.core import Query

BUILTIN_DIR = Path(acquirium.recipes.__file__).parent
BUILTIN_FILES = sorted(BUILTIN_DIR.glob("[!_]*.py"))


@pytest.fixture
def acq():
    client = Acquirium(health_timeout=None)
    client.client = MagicMock()
    client.client.resolve.side_effect = lambda text, kind=None, **_: f"urn:resolved/{text}"
    client.client.compact_uri.side_effect = lambda u: u
    return client


def aliases(query):
    return set(query.query_graph.aliases)


def relation_names(query):
    return [e.relation_name for e in query.query_graph.edges if getattr(e, "relation_name", None)]


class TestShipped:
    def test_one_file_five_recipes(self, acq):
        assert [p.stem for p in BUILTIN_FILES] == ["builtins"]
        assert sorted(dir(acq.recipes)) == ["add", "catalog", "processes", "recent", "source", "stream_stats", "streams"]
        for recipe in acq.recipes:
            assert recipe.origin == "acquirium.recipes.builtins"
            assert recipe.description

    def test_file_pins_a_version(self):
        text = BUILTIN_FILES[0].read_text()
        assert 'ACQUIRIUM = ">=' in text and text.lstrip().startswith('"""')


class TestCatalog:
    def test_three_questions_joined_on_stream(self, acq, monkeypatch):
        seen = []

        def metadata(self, **_):
            seen.append(self)
            names = aliases(self)
            if "host" in names:
                return pl.DataFrame({
                    "stream": ["s1", "s2"], "stream.label": ["L1", "L2"],
                    "host": ["unitA", "pipeP"], "host.type": ["Tank", "Pipe"],
                    "host.process": ["ozonation", None],
                })
            if "feeder" in names:
                return pl.DataFrame({
                    "stream": ["s2"], "stream.label": ["L2"],
                    "feeder": ["unitB"], "feeder.process": ["reverse osmosis"],
                })
            return pl.DataFrame({
                "stream": ["s1", "s2", "s3"], "stream.label": ["L1", "L2", "L3"],
                "stream.quantity_kind": ["pressure", "flow", "temperature"],
                "stream.unit": ["psi", "gpm", None], "stream.substance": [None, None, None],
            })

        monkeypatch.setattr(Query, "metadata", metadata)
        out = acq.recipes.catalog()

        assert out.columns == ["stream", "label", "quantity_kind", "unit", "substance",
                               "host", "host_type", "feeder", "process"]
        rows = {r["stream"]: r for r in out.to_dicts()}
        assert rows["s1"]["process"] == "ozonation" and rows["s1"]["host_type"] == "Tank"
        assert rows["s2"]["process"] == "reverse osmosis" and rows["s2"]["feeder"] == "unitB"
        assert rows["s3"]["host"] is None and rows["s3"]["process"] is None

        measures, hosts, feeders = seen
        assert "stream" in aliases(measures) and measures.query_graph.data_nodes
        assert relation_names(hosts) == ["entity"] and "host" in aliases(hosts)
        assert relation_names(feeders) == ["upstream"] and "feeder" in aliases(feeders)

    def test_narrowing_query_is_renamed_to_stream(self, acq, monkeypatch):
        seen = []
        monkeypatch.setattr(Query, "metadata", lambda self, **_: (seen.append(self), pl.DataFrame({"stream": []}, schema={"stream": pl.Utf8}))[1])
        narrowed = acq.query().entity("pump", alias="pump").measurement(alias="pdata")
        out = acq.recipes.catalog(narrowed)
        assert out.height == 0 and out.columns[0] == "stream"
        assert all({"pump", "stream"} <= aliases(q) for q in seen)

    def test_scada_only_server_keeps_every_stream(self, acq, monkeypatch):
        def metadata(self, **_):
            if "host" not in aliases(self) and "feeder" not in aliases(self):
                return pl.DataFrame({"stream": ["s1"], "stream.quantity_kind": [None],
                                     "stream.unit": ["mg/l"], "stream.substance": ["nh3"]})
            return pl.DataFrame()   # no model: context queries come back empty

        monkeypatch.setattr(Query, "metadata", metadata)
        out = acq.recipes.catalog()
        assert out.to_dicts() == [{"stream": "s1", "label": None, "quantity_kind": None, "unit": "mg/l",
                                   "substance": "nh3", "host": None, "host_type": None,
                                   "feeder": None, "process": None}]


class TestRecent:
    @pytest.fixture
    def capture(self, monkeypatch):
        calls = {}

        def data(self, **kw):
            obj = MagicMock()
            obj.bindings = calls["bindings"]
            return obj

        def dataframe(self, shape="wide", **kw):
            calls["dataframe"] = (shape, kw)
            return pl.DataFrame({"time": []})

        monkeypatch.setattr(Query, "data", data)
        monkeypatch.setattr(Query, "dataframe", dataframe)
        return calls

    @staticmethod
    def binding(latest):
        return BindingInfo(nid=0, point_uri="urn:p", ref_uri="urn:r", alias="data", entity_contexts=[], row_count=1, latest=latest)

    def test_window_ends_at_the_newest_sample(self, acq, capture):
        t1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2025, 3, 1, tzinfo=timezone.utc)
        capture["bindings"] = [self.binding(t1), self.binding(None), self.binding(t2)]
        acq.recipes.recent(acq.query().measurement(), hours=12)
        shape, kw = capture["dataframe"]
        assert shape == "wide"
        assert kw == {"start": t2 - timedelta(hours=12), "end": t2, "cast_value": "float"}

    def test_days_shape_and_cast(self, acq, capture):
        t = datetime(2025, 3, 1, tzinfo=timezone.utc)
        capture["bindings"] = [self.binding(t)]
        acq.recipes.recent(acq.query().measurement(), days=30, shape="narrow", cast_value=None)
        assert capture["dataframe"] == ("narrow", {"start": t - timedelta(days=30), "end": t, "cast_value": None})

    def test_no_data_fetches_unbounded_and_is_empty(self, acq, capture):
        capture["bindings"] = [self.binding(None)]
        out = acq.recipes.recent(acq.query().measurement(), days=1)
        assert out.height == 0
        assert capture["dataframe"] == ("wide", {"cast_value": "float"})

    def test_exactly_one_span(self, acq, capture):
        capture["bindings"] = []
        with pytest.raises(ValueError, match="exactly one of days or hours"):
            acq.recipes.recent(acq.query().measurement())
        with pytest.raises(ValueError, match="exactly one of days or hours"):
            acq.recipes.recent(acq.query().measurement(), days=1, hours=1)


class TestProcesses:
    UNITS = pl.DataFrame({
        "unit": ["u1", "u2", "u3", "u4"],
        "unit.process": ["ozonation", "bac", "bac", "ro"],
    })
    FLOW = pl.DataFrame({
        "from_unit": ["u1", "u2", "u3", "u2"], "from_unit.process": ["ozonation", "bac", "bac", "bac"],
        "to_unit": ["u2", "u3", "u4", "u4"], "to_unit.process": ["bac", "bac", "ro", "ro"],
    })

    @pytest.fixture
    def seen(self, monkeypatch):
        seen = []

        def metadata(self, **_):
            seen.append(self)
            return TestProcesses.FLOW if "to_unit" in aliases(self) else TestProcesses.UNITS

        monkeypatch.setattr(Query, "metadata", metadata)
        return seen

    def test_counts(self, acq, seen):
        out = acq.recipes.processes()
        assert out.to_dicts() == [
            {"process": "bac", "units": 2},
            {"process": "ozonation", "units": 1},
            {"process": "ro", "units": 1},
        ]
        assert len(seen) == 1 and aliases(seen[0]) == {"unit"}

    def test_neighbors(self, acq, seen):
        out = acq.recipes.processes(neighbors=True, max_depth=5)
        rows = {r["process"]: r for r in out.to_dicts()}
        assert rows["ozonation"] == {"process": "ozonation", "units": 1, "upstream": None, "downstream": ["bac"]}
        assert rows["bac"] == {"process": "bac", "units": 2, "upstream": ["ozonation"], "downstream": ["ro"]}
        assert rows["ro"] == {"process": "ro", "units": 1, "upstream": ["bac"], "downstream": None}
        flow = seen[1]
        assert {"from_unit", "to_unit"} <= aliases(flow)
        edge = next(e for e in flow.query_graph.edges if e.direction == "downstream")
        assert edge.nearest is True and edge.hops == 5


class TestStreams:
    def test_is_the_root_measurement(self, acq):
        q = acq.recipes.streams()
        assert isinstance(q, Query)
        assert q.to_sparql() == acq.query().measurement().to_sparql()

    def test_filters_and_uri_pass_through(self, acq):
        qk, unit = "urn:qk/pressure", "urn:unit/psi"
        q = acq.recipes.streams(quantity_kind=qk, unit=unit)
        assert q.to_sparql() == acq.query().measurement(quantity_kind=qk, unit=unit).to_sparql()
        assert qk in q.to_sparql() and unit in q.to_sparql()
        assert "VALUES ?v0 { <urn:ex/p1> }" in acq.recipes.streams(uri="urn:ex/p1").to_sparql()


class TestStreamStats:
    @staticmethod
    def binding(uri, label, rows, earliest, latest, alias="data"):
        return BindingInfo(nid=0, point_uri=uri, ref_uri="urn:r", alias=alias, entity_contexts=[],
                           row_count=rows, earliest=earliest, latest=latest, point_label=label)

    def test_frame_from_bindings(self, acq, monkeypatch):
        t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        t1 = datetime(2025, 2, 1, tzinfo=timezone.utc)
        seen = []

        def data(self, **kw):
            seen.append(self)
            obj = MagicMock()
            obj.bindings = [
                TestStreamStats.binding("urn:ex/a", "A", 5, t0, t1),
                TestStreamStats.binding("urn:ex/b", None, 0, None, None),
                TestStreamStats.binding("urn:ex/c", "C", 9, t0, t0),
            ]
            return obj

        monkeypatch.setattr(Query, "data", data)
        acq.client.compact_uri.side_effect = lambda u: "ex:" + u.rsplit("/", 1)[-1]
        out = acq.recipes.stream_stats()
        assert out.columns == ["alias", "stream", "label", "rows", "earliest", "latest"]
        assert out["stream"].to_list() == ["ex:c", "ex:a", "ex:b"]
        assert out["rows"].to_list() == [9, 5, 0]
        assert out["latest"].to_list() == [t0, t1, None]
        assert out["label"].to_list() == ["C", "A", None]
        assert seen[0].query_graph.data_nodes and "pump" not in aliases(seen[0])

    def test_empty_and_narrowed(self, acq, monkeypatch):
        seen = []

        def data(self, **kw):
            seen.append(self)
            obj = MagicMock()
            obj.bindings = []
            return obj

        monkeypatch.setattr(Query, "data", data)
        narrowed = acq.query().entity("pump", alias="pump").measurement()
        out = acq.recipes.stream_stats(narrowed)
        assert out.height == 0 and out.schema["latest"] == pl.Datetime("us", "UTC")
        assert "pump" in aliases(seen[0])
