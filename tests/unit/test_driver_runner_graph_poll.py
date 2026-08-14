"""Unit tests for DriverRunner's graph-change polling cadence.

The runner is a Ray actor; these exercise the plain class behind the decorator
so the polling logic can be tested without standing up Ray.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from acquirium.Drivers.Driver import Driver
from acquirium.Drivers.runner import DEFAULT_GRAPH_POLL_INTERVAL, DriverRunner

RunnerClass = DriverRunner.__ray_actor_class__


class RecordingDriver(Driver):
    def __init__(self, aq, config):
        super().__init__(aq, config)
        self.graph_changes = 0

    def setup(self) -> None:
        self.source_id = "demo"

    def tick(self) -> None:
        return None

    def on_graph_change(self) -> None:
        self.graph_changes += 1


def make_runner(interval=1.0, config=None, source_version=1):
    aq = MagicMock()
    aq.graph_status.return_value = {"source_version": source_version}
    return RunnerClass(RecordingDriver, config or {}, aq, interval)


# ------------------------------------------------------------------ cadence


def test_graph_poll_interval_defaults_to_a_floor_for_fast_ticks():
    """A 1s tick must not become a 1s poll of the server."""
    runner = make_runner(interval=1.0)
    assert runner.graph_poll_interval == DEFAULT_GRAPH_POLL_INTERVAL


def test_graph_poll_interval_follows_slower_tick_intervals():
    runner = make_runner(interval=30.0)
    assert runner.graph_poll_interval == 30.0


def test_graph_poll_interval_can_be_configured():
    runner = make_runner(interval=1.0, config={"driver": {"graph_poll_interval": 60.0}})
    assert runner.graph_poll_interval == 60.0


def test_configured_graph_poll_interval_may_be_shorter_than_the_floor():
    runner = make_runner(interval=1.0, config={"driver": {"graph_poll_interval": 2.0}})
    assert runner.graph_poll_interval == 2.0


# ------------------------------------------------------------------ polling


def test_poll_is_skipped_until_the_interval_elapses():
    runner = make_runner(interval=1.0)
    runner.setup()
    runner.acquirium_cli.graph_status.reset_mock()

    runner._poll_graph_version()
    runner.acquirium_cli.graph_status.assert_not_called()


def test_poll_runs_once_the_interval_elapses():
    runner = make_runner(interval=1.0, source_version=2)
    runner.setup()
    runner.acquirium_cli.graph_status.reset_mock()
    runner._last_graph_poll -= runner.graph_poll_interval

    runner._poll_graph_version()
    runner.acquirium_cli.graph_status.assert_called_once()


def test_version_change_fires_on_graph_change():
    runner = make_runner(interval=1.0, source_version=1)
    runner.setup()
    assert runner.driver.graph_changes == 0

    runner.acquirium_cli.graph_status.return_value = {"source_version": 2}
    runner._last_graph_poll -= runner.graph_poll_interval
    runner._poll_graph_version()

    assert runner.driver.graph_changes == 1
    assert runner.source_version == 2


def test_unchanged_version_does_not_fire_on_graph_change():
    runner = make_runner(interval=1.0, source_version=1)
    runner.setup()
    runner._last_graph_poll -= runner.graph_poll_interval
    runner._poll_graph_version()
    assert runner.driver.graph_changes == 0


def test_graph_status_failure_is_swallowed():
    runner = make_runner(interval=1.0)
    runner.setup()
    runner.acquirium_cli.graph_status.side_effect = RuntimeError("server down")
    runner._last_graph_poll -= runner.graph_poll_interval

    runner._poll_graph_version()  # must not raise; the tick still needs to run
    assert runner.driver.graph_changes == 0


def test_on_graph_change_failure_does_not_propagate():
    runner = make_runner(interval=1.0, source_version=1)
    runner.setup()
    runner.driver.on_graph_change = MagicMock(side_effect=RuntimeError("boom"))
    runner.acquirium_cli.graph_status.return_value = {"source_version": 5}
    runner._last_graph_poll -= runner.graph_poll_interval

    runner._poll_graph_version()
    assert runner.source_version == 5
