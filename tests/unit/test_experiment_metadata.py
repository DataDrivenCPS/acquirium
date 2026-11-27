"""Experiment metadata through a local server and the ledger's graph boundary.

Tests of what users see go through a local server started with aq.init(). The
ones that need a count of graph work or a graph edited underneath the ledger
open the storage components directly.
"""
from datetime import datetime, timezone
from unittest.mock import Mock

import polars as pl
import pytest
from rdflib import Literal, URIRef
from rdflib.namespace import XSD
from requests import HTTPError

from acquirium.Client.explore.attributes import ATTR_NS
from acquirium.ExperimentMetadata import EXPERIMENT, annotation_uri, run_uri, variable_uri
from acquirium.Experiments import ExperimentStore
from acquirium.Storage.ledger import DuckDBLedger
from acquirium.internals.internals_namespaces import HAS_UNIT

PSI = "http://qudt.org/vocab/unit/PSI"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


# Through the server


def test_metadata_filters_are_scoped_and_observations_follow_selection(ac):
    study = ac.study.define("cases")
    cost = study.output("cost").scalar()
    first = study.start({"scenario": "baseline", "settings": {"flow": 10}, "tags": ["a", "b"]})
    cost.record(12.5)
    first.finish()
    # Attribute discovery must work before any collection query has run.
    assert ac.attr.settings.flow.name == "settings.flow"
    second = study.start({"scenario": "trial", "settings": {"flow": 20}, "tags": ["b"]})
    cost.record(11.0)
    second.finish()
    other = ac.study.define("other")
    other.start({"scenario": "baseline", "settings": {"flow": 100}})

    assert [run.run_id for run in study.experiments.where(scenario="baseline").all()] == [first.run_id]
    high_flow = study.experiments.where(ac.attr.settings.flow >= 15)
    assert [run.run_id for run in high_flow.all()] == [second.run_id]
    assert high_flow.observations(cost).frame()["value"].to_list() == [11.0]
    assert high_flow.scalars(metadata=["settings.flow"]).select("settings.flow", "cost").rows() == [(20, 11.0)]
    assert len(study.experiments.where(tags="b").all()) == 2
    absent = study.experiments.where(scenario="absent")
    assert absent.all() == []
    assert absent.observations().all() == []

    query = study.experiments.query().include("settings", "tags")
    frame = query.metadata(include_dependencies=False)
    assert frame.height == 2
    assert frame.schema["experiment.settings"] == pl.Struct({"flow": pl.Int64})
    assert frame.schema["experiment.tags"] == pl.List(pl.String)


def test_scalar_variable_unit_strings_and_uris_share_semantic_filter(ac):
    study = ac.study.define("pressure")
    pressure = study.output("pressure").scalar(unit="PSI", settings={"sensor": "A"}, category="simulation")
    other = ac.study.define("other")
    other.output("pressure").scalar(unit="PSI")

    assert study.variables.where(unit="psi").all() == [pressure]
    assert study.variables.where(unit=URIRef(PSI)).all() == [pressure]
    assert pressure.unit == "PSI"  # Saved objects retain their original shape and values.
    result = ac.client.sparql_query(
        f"SELECT ?unit WHERE {{ <{pressure.uri}> <{HAS_UNIT}> ?unit }}",
        wait_for_fresh=True,
    )
    assert result["rows"] == [[PSI]]
    frame = study.variables.query().include("unit").metadata(include_dependencies=False)
    assert frame["variable.unit"].to_list() == ["unit:PSI"]
    objects = study.variables.query().include("unit", "unit.raw").metadata_objects(include_dependencies=False)
    assert objects[0]["variable"]["unit"] == PSI
    assert objects[0]["variable"]["unit.raw"] == "PSI"
    assert study.variables.where(**{"unit.raw": "PSI"}).all() == [pressure]
    assert study.variables.where(role="input").where(unit="PSI").all() == []
    assert study.variables.where(role="input").query().metadata().height == 0
    assert pressure.metadata["category"] == "simulation"
    assert study.variables.where(**{"metadata.category": "simulation"}).all() == [pressure]
    with pytest.raises(ValueError, match="category"):
        study.variables.where(category="simulation").all()


def test_server_reads_return_saved_metadata(ac):
    study = ac.study.define("recorded objects")
    pressure = study.output("pressure").scalar(unit="PSI", settings={"mode": "baseline"})
    run = study.start({"settings": {"mode": "baseline"}, "tags": ["A", "B"]})
    pressure.record(1)
    pressure.record(2)
    run.annotate_stream("urn:site:stream", start=NOW, metadata={"treatment": "A"})
    run.finish()
    trial = study.start({"settings": {"mode": "trial", "duration": 60}})
    trial.finish()
    study.output("other").scalar(unit=PSI, settings={"duration": 60})

    reopened = ac.study.get("recorded objects")
    saved = {"settings": {"mode": "baseline"}, "tags": ["A", "B"]}
    assert reopened.variables["pressure"].metadata == {"settings": {"mode": "baseline"}}
    assert reopened.experiments.get(run.run_id).metadata == saved
    assert [item.metadata for item in reopened.experiments.all()] == [saved, {"settings": {"mode": "trial", "duration": 60}}]
    assert ac.client.list_experiments(study.study_id, run_ids=[run.run_id])[0]["metadata"] == saved
    observations = ac.client.list_experiment_observations(study.study_id)
    assert ac.client.list_experiment_observations(study.study_id, run_ids=[run.run_id]) == observations
    assert [item["experiment_metadata"] for item in observations] == [saved] * 2
    assert [item["variable_metadata"] for item in observations] == [reopened.variables["pressure"].declaration.document()] * 2
    assert reopened.stream_annotations()[0].metadata == {"treatment": "A"}

    # Objects retain each run's own keys. The shared table schema fills an absent field with null.
    query = reopened.experiments.query().include("settings")
    objects = [item["experiment"] for item in query.metadata_objects()]
    assert {item["id"]: item["settings"] for item in objects} == {
        run.uri: {"mode": "baseline"}, trial.uri: {"mode": "trial", "duration": 60},
    }
    assert {"duration": None, "mode": "baseline"} in query.metadata()["experiment.settings"].to_list()
    assert query.where(run_id=run.run_id).metadata_objects() == [
        {"experiment": {"id": run.uri, "settings": {"mode": "baseline"}}},
    ]


@pytest.mark.parametrize("identity", ["pump-7", "urn:example:pump-7"], ids=["label", "uri"])
def test_saved_run_documents_preserve_mixed_metadata(ac, identity):
    study = ac.study.define("round trip")
    values = {
        "id": identity,
        "settings": {"enabled": True, "threshold": 1.5},
        "tags": ["A", None, "B", "A"],
        "items": [{"name": "A"}, {"name": "B", "count": 2}],
        "matrix": [[1, 2], [3, 4]],
        "datasheet": "https://example.org/input",
        "metadata": {"any.key": [None, {}, []]},
    }
    run = study.start(values)
    reopened = ac.study.get("round trip")
    assert reopened.experiments.get(run.run_id).metadata == values
    obj = reopened.experiments.query().include("tags", "metadata").metadata_objects()[0]["experiment"]
    assert obj["tags"] == values["tags"]
    assert obj["metadata"] == values["metadata"]


def test_null_empty_metadata_is_removed_but_recorded_json_is_preserved(ac):
    study = ac.study.define("values")
    output = study.output("result").json(unit=None, tags=[], info={}, note="")
    run = study.start({"tags": [], "notes": None, "settings": {"flow": 10, "unused": {}}})
    output.record({"tags": [], "notes": None, "settings": {}})
    assert output.metadata == {"note": "", "tags": [], "info": {}}
    assert run.metadata == {"settings": {"flow": 10, "unused": {}}}
    assert run.observations().latest().value == {"tags": [], "notes": None, "settings": {}}


def test_lifecycle_writes_keep_metadata_edits(ac):
    study = ac.study.define("concurrent lifecycle")
    run = study.start({"scenario": "baseline"})
    ac.insert_metadata(run.uri, {"scenario": "edited"})
    run.finish()
    assert ac.client.get_experiment(run.run_id)["metadata"] == {"scenario": "edited"}
    assert len(study.experiments.where(scenario="edited", status="succeeded").all()) == 1


def test_variable_metadata_edits_keep_the_user_namespace(ac):
    from acquirium.Client.metadata import plan_write, update_text

    study = ac.study.define("edited declaration")
    pressure = study.output("pressure").scalar(unit="PSI", metadata={"saved": [None, {}, []]})
    ac.insert_metadata(pressure.uri, {"review": "edited"})
    reopened = ac.study.get("edited declaration").output["pressure"]
    assert reopened.metadata == {"saved": [None, {}, []], "review": "edited"}
    assert reopened.unit == "PSI"
    # Write an explicit override to the metadata source. The ledger's saved
    # reads use that document in preference to the original declaration.
    pascal = "http://qudt.org/vocab/unit/PA"
    override = plan_write(
        pressure.uri, {"metadata": {"category": "replaced"}, "unit": pascal},
        role="data", resolve=lambda *_: None,
    )
    ac.client.sparql_update(update_text([override]), source_id="metadata")
    reopened = ac.study.get("edited declaration").output["pressure"]
    assert reopened.metadata == {"category": "replaced", "review": "edited"}
    assert reopened.unit == pascal


def test_invalid_metadata_fails_before_anything_is_saved(ac):
    study = ac.study.define("validation")
    with pytest.raises(HTTPError, match="[Cc]ould not resolve"):
        study.output("pressure").scalar(unit="not-a-unit")
    with pytest.raises(HTTPError, match="reserved"):
        study.start({"status": "pretend"})
    with pytest.raises(HTTPError, match="invalid metadata key"):
        study.start({"solver.options": 1})
    assert ac.client.list_experiment_variables(study.study_id) == []
    assert ac.client.list_experiments(study.study_id) == []


def test_empty_study_native_queries_and_lifecycle_filter_override(ac):
    study = ac.study.define("empty")
    assert study.variables.where(role="input").query().metadata().height == 0
    assert study.experiments.where(status="succeeded").query().metadata().height == 0
    study.start({"scenario": "setup"}).finish()
    selected = study.experiments.where(status="running", scenario="baseline")
    assert selected.all() == []
    run = study.start({"scenario": "baseline"})
    assert [item.run_id for item in selected.all()] == [run.run_id]
    run.finish()
    assert selected.all() == []
    assert [item.run_id for item in selected.where(status="succeeded").all()] == [run.run_id]


def test_each_new_run_key_is_filterable_right_after_the_run(ac):
    study = ac.study.define("keys")
    first = study.start({"operator": "ana"})
    fresh = ac.client.sparql_query(
        f"SELECT ?v WHERE {{ <{first.uri}> <{ATTR_NS}operator> ?v }}", wait_for_fresh=True,
    )
    assert fresh["rows"] == [["ana"]]
    first.finish()
    for index in range(4):
        run = study.start({f"setting{index}": index})
        run.finish()
        # Discovery is cached by graph version; each new key must still be found.
        selected = study.experiments.where(getattr(ac.attr, f"setting{index}") == index)
        assert [found.run_id for found in selected.all()] == [run.run_id]


def test_deleted_records_leave_the_metadata_graph(ac):
    def nodes_mentioning(identifier):
        """Find graph nodes whose URI or triple values contain a run or Study ID."""
        # The server rebuilds its query view in the background. Wait for the
        # write or delete just performed so we can check its effect on the graph.
        result = ac.client.sparql_query(f"""
            SELECT DISTINCT ?s WHERE {{
                ?s ?p ?o .
                FILTER(CONTAINS(STR(?s), '{identifier}') || CONTAINS(STR(?o), '{identifier}'))
            }}
        """, wait_for_fresh=True)
        return {uri for (uri,) in result["rows"]}

    # Declare a Study and two output variables that its runs will share.
    study = ac.study.define("cases")
    cost = study.output("cost").scalar(unit="PSI")
    level = study.output("level").timeseries()

    # Run a baseline and a trial. Record a scalar and a timestamped reading
    # in each run so both have records to check when we delete them.
    runs = []
    for scenario in ("baseline", "trial"):
        run = study.start({"scenario": scenario})
        cost.record(1.0)
        level.record([(NOW, 1.0)])
        run.finish()
        runs.append(run)

    # Link the finished baseline run to a plant stream by URI. The run,
    # its annotation, and its recorded streams all mention the run ID.
    annotation = runs[0].annotate_stream("urn:plant:flow", start=NOW)
    assert {run_uri(runs[0].run_id), annotation_uri(annotation.annotation_id)} < nodes_mentioning(runs[0].run_id)

    # Delete the baseline. Its graph nodes should disappear and the trial should remain.
    runs[0].delete()
    assert not nodes_mentioning(runs[0].run_id)
    assert run_uri(runs[1].run_id) in nodes_mentioning(runs[1].run_id)
    assert study.experiments.where(scenario="baseline").all() == []

    # Deleting the Study should also remove the trial and its records.
    study.delete()
    assert not nodes_mentioning(runs[1].run_id)
    assert not nodes_mentioning(study.study_id)

    # Create a populated Study to check that clearing all Studies also clears
    # their metadata from the graph.
    other = ac.study.define("other")
    other_level = other.output("level").timeseries()
    run = other.start({"scenario": "baseline"})
    other_level.record([(NOW, 1.0)])
    run.finish()
    ac.study.clear(confirm=True)
    assert not nodes_mentioning(run.run_id)
    assert not nodes_mentioning(other.study_id)
    assert ac.study.all() == []


def test_compatibility_metadata_filters_match_complete_saved_fields(ac):
    study = ac.study.define("exact metadata")
    first = study.start({"tags": ["A", "B"], "settings": {"flow": 10}})
    first.finish()
    study.start({"tags": ["B", "A"], "settings": {"flow": 10, "mode": "trial"}}).finish()
    study.start({"tags": ["A", None, "B"]}).finish()
    runs = lambda metadata: [item["run_id"] for item in ac.client.list_experiments(study.study_id, metadata=metadata)]
    assert runs({"tags": ["A", "B"]}) == [first.run_id]
    assert runs({"settings": {"flow": 10}}) == [first.run_id]
    assert len(runs({"absent": None})) == 3


# The ledger's graph boundary


@pytest.fixture
def duckdb_experiments(tmp_path, samples, source_graph):
    """Open a ledger for graph behavior that is shared by the two SQL backends."""
    store = DuckDBLedger(tmp_path / "experiments.duckdb", samples=samples)
    try:
        yield ExperimentStore(store, graph_store=source_graph)
    finally:
        store.close()


def test_ledger_reads_hydrate_each_unique_subject_once(duckdb_experiments, source_graph, monkeypatch):
    ledger = duckdb_experiments
    study = ledger.define("batched metadata")["template_id"]
    pressure = ledger.declare(study, "pressure", "output", "scalar", {"unit": PSI})
    cost = ledger.declare(study, "cost", "output", "scalar", {"metadata": {"region": "east"}})
    metadata = {"metadata": {"exact.key": [None, {}, []]}}
    first = ledger.start(study, metadata)["run_id"]
    second = ledger.start(study, {"scenario": "trial"})["run_id"]
    pressure_id, cost_id = pressure["variable_id"], cost["variable_id"]
    for identifier, value in ((pressure_id, 1), (cost_id, 7), (pressure_id, 2)):
        ledger.observe(first, identifier, value=value)
    ledger.sync_metadata_graph()

    # Read at the ledger entry points the server uses. A list read hydrates
    # each subject exactly once even when two observations name it.
    read = Mock(wraps=source_graph.metadata_documents)
    monkeypatch.setattr(source_graph, "metadata_documents", read)
    for fetch, subjects in (
        (lambda: ledger.variables(study), [pressure_id, cost_id]),
        (lambda: ledger.runs(study), [run_uri(first), run_uri(second)]),
        (lambda: ledger.run(first), [run_uri(first)]),
    ):
        read.reset_mock()
        fetch()
        read.assert_called_once()
        assert sorted(read.call_args.args[0]) == sorted(subjects)

    read.reset_mock()
    observations = ledger.observations(study)
    assert [item["value"] for item in observations] == [1, 7, 2]
    assert [item["variable_metadata"] for item in observations] == [
        pressure["metadata"], cost["metadata"], pressure["metadata"],
    ]
    assert [item["experiment_metadata"] for item in observations] == [metadata] * 3
    read.assert_called_once()
    assert sorted(read.call_args.args[0]) == sorted([run_uri(first), pressure_id, cost_id])

    assert ledger.metadata.read(EXPERIMENT.Variable, [cost_id, cost_id]) == {cost_id: cost["metadata"]}
    read.reset_mock()
    assert ledger.metadata.read(EXPERIMENT.Variable, []) == {}
    assert ledger.metadata.read_observation_metadata([]) == []
    assert ledger.metadata.read_metadata([], EXPERIMENT.Run, "run_id") == []
    read.assert_not_called()


def test_uri_values_under_a_bound_namespace_are_written_as_uris(duckdb_experiments, source_graph):
    ledger = duckdb_experiments
    source_graph.insert_graph("@prefix plant: <urn:plant#> . plant:tank plant:status 'active' .")
    study = ledger.define("namespaces")["template_id"]
    values = {"feeds": "urn:plant#tank", "datasheet": "https://example.org/ds.pdf"}
    variable = ledger.declare(study, "level", "output", "scalar", values)
    run = ledger.start(study, values)["run_id"]
    annotation = ledger.annotate_stream(run, "urn:site:stream", NOW, metadata=values)
    ledger.sync_metadata_graph()

    # All three node kinds preserve URI terms. The variable's user metadata
    # is under its metadata key on the graph.
    asserted = source_graph.data_graph("experiments")
    for subject, prefix in ((variable_uri(variable["variable_id"]), "metadata."), (run_uri(run), ""),
                            (annotation_uri(annotation["annotation_id"]), "")):
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "feeds"), URIRef("urn:plant#tank")) in asserted
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "datasheet"),
                Literal("https://example.org/ds.pdf", datatype=XSD.string)) in asserted
    assert ledger.run(run)["metadata"] == values


@pytest.mark.parametrize("missing", ["run", "variable"])
def test_missing_document_prevents_observation_hydration(duckdb_experiments, source_graph, missing):
    from acquirium.Metadata import document_predicate

    ledger = duckdb_experiments
    study = ledger.define("corrupted graph")["template_id"]
    variable = ledger.declare(study, "result", "output", "scalar", {})["variable_id"]
    run = ledger.start(study, {"scenario": "baseline"})["run_id"]
    ledger.observe(run, variable, value=1.0)
    ledger.sync_metadata_graph()

    # Simulate a missing authoritative document while the SQL rows remain.
    subject, field = {"run": (run_uri(run), "run_id"), "variable": (variable, "variable_id")}[missing]
    graph = source_graph.source_dataset.graph(source_graph.source_graph_uri("experiments"))
    graph.remove((URIRef(subject), document_predicate(field), None))
    with pytest.raises(RuntimeError, match="no document"):
        ledger.observations(study)
    if missing == "run":
        with pytest.raises(RuntimeError, match="no document"):
            ledger.run(run)
    with ledger.store._own_conn() as conn:
        assert conn.execute("SELECT status FROM experiment_runs WHERE run_id=?", [run]).fetchone() == ("running",)


def test_writes_between_reads_publish_in_one_graph_update(duckdb_experiments, source_graph, monkeypatch):
    ledger = duckdb_experiments
    study = ledger.define("sweep")["template_id"]
    pressure = ledger.declare(study, "pressure", "output", "scalar", {"unit": PSI})["variable_id"]
    ledger.sync_metadata_graph()
    update = Mock(wraps=source_graph.metadata_update)
    monkeypatch.setattr(source_graph, "metadata_update", update)
    for flow in range(20):
        run = ledger.start(study, {"settings": {"flow": flow}})["run_id"]
        ledger.observe(run, pressure, value=flow)
        ledger.finish(run, "succeeded")
    update.assert_not_called()

    # Before a graph query, the server publishes the pending runs in their final states.
    ledger.publish_queued_metadata()
    update.assert_called_once()
    subjects = source_graph.data_graph("experiments").subject_objects(URIRef(ATTR_NS + "status"))
    statuses = {str(subject): str(status) for subject, status in subjects}
    assert len(statuses) == 20 and set(statuses.values()) == {"succeeded"}
