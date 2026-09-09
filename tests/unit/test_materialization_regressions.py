from dataclasses import replace

import pytest

from acquirium.Materialization.runtime import Materializer
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import LineageCopy, LineageGraph


def test_invalid_deployment_does_not_replace_active_definition(tmp_path):
    store = DuckDBStore(tmp_path / 'state.duckdb')
    try:
        runtime = Materializer(store, LineageGraph())
        original = Deployment.from_class(LineageCopy)
        runtime.deploy(original)
        with pytest.raises((ImportError, ValueError)):
            runtime.deploy(replace(original, entrypoint='missing_review_app:Broken'))
        assert runtime._deployments() == (original,)
    finally:
        store.close()
