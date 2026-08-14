"""Version-counter semantics: source_version vs data_version.

source_version counts every write (it keys the derived query cache);
data_version excludes provenance-graph writes so pollers of the data
generation don't wake on — or loop over — derived bookkeeping.

The store object is built bare (no Oxigraph/ontoenv): the counters touch
only the state file and their own attributes, which is exactly the surface
under test.
"""

import json

import pytest

from acquirium.Storage import graph_store as graph_store_module
from acquirium.Storage.graph_registry import (
    PLANT_GRAPH_URI,
    is_provenance_graph_uri,
    provenance_source_id,
    source_graph_uri,
)


def make_store(tmp_path):
    store = object.__new__(graph_store_module.OxigraphGraphStore)
    store.source_store_path = tmp_path
    store._source_version = store._load_source_version()
    store._data_version = store._load_data_version()
    # _mark_ontology_graph_changed also touches closure state.
    store._closure_version = 0
    store._dependency_graph_closure_version = -1
    return store


class TestProvenanceGraphNaming:
    def test_provenance_source_maps_under_source_prefix(self):
        sid = provenance_source_id("app:seawater_tds")
        assert sid == "app:seawater_tds:prov"
        assert is_provenance_graph_uri(source_graph_uri(sid))

    def test_non_provenance_uris(self):
        assert not is_provenance_graph_uri(source_graph_uri("app:seawater_tds"))
        assert not is_provenance_graph_uri(source_graph_uri("mybox-metrics"))
        assert not is_provenance_graph_uri(PLANT_GRAPH_URI)
        assert not is_provenance_graph_uri("urn:something:else")

    def test_empty_source_id_rejected(self):
        with pytest.raises(ValueError):
            provenance_source_id("")


class TestVersionCounters:
    def test_data_write_bumps_both(self, tmp_path):
        store = make_store(tmp_path)
        store._mark_source_changed(source_graph_uri("app:x"))
        assert (store._source_version, store._data_version) == (1, 1)

    def test_provenance_write_bumps_only_source(self, tmp_path):
        store = make_store(tmp_path)
        store._mark_source_changed(source_graph_uri(provenance_source_id("app:x")))
        assert (store._source_version, store._data_version) == (1, 0)

    def test_default_target_counts_as_data(self, tmp_path):
        store = make_store(tmp_path)
        store._mark_source_changed(None)
        assert (store._source_version, store._data_version) == (1, 1)

    def test_ontology_change_bumps_data_version(self, tmp_path):
        # The ontoenv callback bypasses _finalize_source_write; ontology
        # changes reshape the inferred graph every query reads, so pollers
        # must wake.
        store = make_store(tmp_path)
        store._mark_ontology_graph_changed()
        assert (store._source_version, store._data_version) == (1, 1)
        assert store._closure_version == 1

    def test_counters_persist_and_reload(self, tmp_path):
        store = make_store(tmp_path)
        store._mark_source_changed(source_graph_uri("app:x"))
        store._mark_source_changed(source_graph_uri(provenance_source_id("app:x")))
        assert (store._source_version, store._data_version) == (2, 1)

        reloaded = make_store(tmp_path)
        assert (reloaded._source_version, reloaded._data_version) == (2, 1)

    def test_legacy_state_file_seeds_data_version_from_version(self, tmp_path):
        # State written before data_version existed: every recorded write was
        # a data write, so the counters start out equal.
        (tmp_path / "acquirium_source_state.json").write_text(json.dumps({"version": 5}))
        store = make_store(tmp_path)
        assert (store._source_version, store._data_version) == (5, 5)
