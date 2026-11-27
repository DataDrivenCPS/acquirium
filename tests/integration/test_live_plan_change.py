"""Admit writes while a metadata edit is activating a new app plan."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep

import polars as pl
import pytest

from acquirium import App, Native, Output, PythonFunction, Selection
from acquirium.apps import output_reference
from acquirium.internals.internals_namespaces import OUTPUT_GROUP, PRODUCED_BY
from tests.epoch_helpers import epoch_server


@pytest.mark.integration
def test_live_ingest_through_plan_changes_stays_coherent_across_restart(tmp_path):
    directory = tmp_path / "server"
    started = tmp_path / "started"
    release = tmp_path / "release"
    module = tmp_path / "calculation.py"
    module.write_text(
        "import time\n"
        "from pathlib import Path\n"
        "from acquirium.functions import emit\n"
        "def calculate(frame, context):\n"
        "    if context['parameters']['group'] == ('urn:test:new',):\n"
        f"        Path({str(started)!r}).touch()\n"
        "        deadline = time.monotonic() + 30\n"
        f"        while not Path({str(release)!r}).exists():\n"
        "            if time.monotonic() >= deadline:\n"
        "                raise RuntimeError('plan activation was not released')\n"
        "            time.sleep(0.01)\n"
        "    return {'mean': emit(context, frame['value'].mean())}\n",
        encoding="utf-8",
    )
    mean = App(
        "group-mean",
        {"input": Selection("SELECT ?stream ?group WHERE { ?stream <urn:test:group> ?group }")},
        {"mean": Output()},
        PythonFunction.from_file(module, "calculate"),
    )
    total = App(
        "total",
        {"input": Selection.outputs_of(mean, "mean", combine=True)},
        {"total": Output()},
        Native.sum("total"),
    )
    old = output_reference(mean.name, "urn:test:old", "mean")
    new = output_reference(mean.name, "urn:test:new", "mean")
    combined = output_reference(total.name, total.name, "total")
    first_time, next_day = 0, 86_400_000_000_000

    def read(ref, epoch):
        """Read one stream through HTTP at the requested epoch."""
        frame = client.client.timeseries_df(ref, epoch=epoch)
        return list(zip(frame["ts"].cast(pl.Int64), frame["value"]))

    def assign(a_group, b_group):
        """Change the source bindings and wait for their app plan to publish."""
        manager.insert_graph(
            f"<{a}> <urn:test:group> <urn:test:{a_group}> .\n"
            f"<{b}> <urn:test:group> <urn:test:{b_group}> .",
            source_id="membership", replace=True,
        )

    def wait_until_started(edit):
        """Wait for the worker marker and surface an early graph-edit failure."""
        deadline = monotonic() + 15
        while not started.exists():
            if edit.done():
                edit.result()
                pytest.fail("the graph edit completed before its worker blocked")
            assert monotonic() < deadline, "the new plan did not start computing"
            sleep(0.01)

    with epoch_server(directory, read_batch_size=1) as (manager, _, client):
        # Both probes initially contribute to the old group and its downstream total.
        client.register_streams([
            {"source_id": "live", "stream_name": name, "value_kind": "numeric"}
            for name in ("a", "b")
        ])
        a, b = (str(client.stream("live", name)) for name in ("a", "b"))
        assign("old", "old")
        client.deploy_app(mean)
        client.deploy_app(total)
        receipt, = client.insert_timeseries_batch("live", {
            "a": [(first_time, 2), (next_day, 8)],
            "b": [(first_time, 6), (next_day, 12)],
        })["receipts"]
        original = client.wait(receipt)["epoch"]
        original_rows = [(first_time, 4), (next_day, 10)]
        assert read(old, original) == read(combined, original) == original_rows
        assert read(new, original) == []

        # Block the new group inside its worker after the engine accepts the plan.
        with ThreadPoolExecutor(max_workers=1) as executor:
            moving = executor.submit(assign, "new", "new")
            try:
                wait_until_started(moving)
                corrected, = client.insert_timeseries(
                    "live", "b", [(first_time, 10)], request_id="correct", wait=0,
                )["receipts"]
                deleted = client.delete_timeseries(
                    "live", "b", [next_day], request_id="delete", wait=0,
                )
                assert all(r["accepted"] and not r["published"] for r in (corrected, deleted))
                assert read(old, original) == read(combined, original) == original_rows
                assert read(new, original) == []
            finally:
                release.touch()
            moving.result(timeout=15)

        # After activation and queued writes, the graph, plan, and outputs agree.
        client.wait(deleted)
        published = client.engine_snapshot()
        assert published["plan_current"]
        assert {
            op["key"] for op in published["plan"]["instances"] if op["template"] == mean.name
        } == {"urn:test:new"}
        matches = manager.graph_store.sparql_query(
            f'SELECT ?group WHERE {{ ?point <{PRODUCED_BY}> "{mean.name}" ; <{OUTPUT_GROUP}> ?group }}',
            wait_for_fresh=True,
        )
        assert [str(group) for group, in matches["rows"]] == ["urn:test:new"]
        latest = published["epoch"]
        latest_rows = [(first_time, 6), (next_day, 8)]
        assert read(new, latest) == read(combined, latest) == latest_rows
        assert read(old, latest) == []
        assert read(b, latest) == [(first_time, 10)]

    with epoch_server(directory, read_batch_size=1) as (manager, _, client):
        # Recovery keeps both epochs. A further membership edit still repairs the total.
        recovered = client.engine_snapshot()
        assert (recovered["epoch"], recovered["plan"]) == (latest, published["plan"])
        assert read(old, original) == read(combined, original) == original_rows
        assert read(new, latest) == read(combined, latest) == latest_rows
        assign("new", "old")
        current = client.engine_snapshot()["epoch"]
        assert read(new, current) == [(first_time, 2), (next_day, 8)]
        assert read(old, current) == [(first_time, 10)]
        assert read(combined, current) == [(first_time, 12), (next_day, 8)]
