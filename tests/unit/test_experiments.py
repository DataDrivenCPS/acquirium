from datetime import datetime, timezone

import pytest

from acquirium.Experiments import ExperimentStore
from acquirium.Storage.duckdb_store import DuckDBStore


def test_reusable_template_variables_are_isolated_and_timestamped(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        experiments = ExperimentStore(store, tmp_path / "artifacts")
        template = experiments.define("load-shift")
        variable = experiments.declare(template["template_id"], "configuration", "input", "json", {})
        first = experiments.start(template["template_id"], {"case": 1})
        first_observation = experiments.observe(first["run_id"], variable["variable_id"], value={"flow": 10})
        assert first_observation["sequence"] == 1
        assert first_observation["recorded_at"].tzinfo is not None
        experiments.finish(first["run_id"], "succeeded")
        with pytest.raises(ValueError, match="terminal"):
            experiments.observe(first["run_id"], variable["variable_id"], value={"flow": 20})
        second = experiments.start(template["template_id"], {"case": 2})
        second_observation = experiments.observe(second["run_id"], variable["variable_id"], value={"flow": 20})
        assert second_observation["sequence"] == 1
    finally:
        store.close()


def test_file_attachment_is_content_addressed(tmp_path):
    store = DuckDBStore(tmp_path / "data.duckdb", recreate=True)
    try:
        experiments = ExperimentStore(store, tmp_path / "artifacts")
        template = experiments.define("files")
        variable = experiments.declare(template["template_id"], "config", "input", "file", {})
        run = experiments.start(template["template_id"], {})
        first = experiments.attach_file(run["run_id"], variable["variable_id"], "a.json", "application/json", b"{}")
        second = experiments.attach_file(run["run_id"], variable["variable_id"], "b.json", "application/json", b"{}")
        assert first["digest"] == second["digest"]
        assert (tmp_path / "artifacts" / first["digest"]).read_bytes() == b"{}"
    finally:
        store.close()
