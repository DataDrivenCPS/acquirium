"""Driver shutdown preserves exit failures and a shared deadline."""

import pickle
import subprocess
import sys
import time
from unittest.mock import Mock, call

import pytest

from acquirium.Drivers.supervisor import DriverSupervisor


def test_start_reaps_a_child_that_misses_the_setup_deadline(monkeypatch):
    spawn = subprocess.Popen
    children = []

    def stalled_child(_command, **options):
        proc = spawn(
            [sys.executable, "-c", "import pickle, sys, time; pickle.load(sys.stdin.buffer); time.sleep(10)"],
            **options,
        )
        children.append(proc)
        return proc

    monkeypatch.setattr("acquirium.Drivers.supervisor.subprocess.Popen", stalled_child)
    supervisor = DriverSupervisor("localhost", 8000)
    with pytest.raises(TimeoutError, match="setup timed out"):
        supervisor.start_driver(spec="example:Sensor", config={"driver": {"setup_timeout": 0.2}})
    child, = children
    assert child.poll() is not None
    assert supervisor.list_drivers() == []


def test_child_exits_when_parent_stdin_closes_during_setup(tmp_path):
    started = tmp_path / "started"
    (tmp_path / "stalled.py").write_text(
        f"from pathlib import Path\nfrom time import sleep\nPath({str(started)!r}).touch()\nsleep(30)\n"
    )
    payload = {"spec": "stalled.py:Sensor", "config": {"__config_dir": str(tmp_path)}}
    with subprocess.Popen(
        [sys.executable, "-m", "acquirium.Drivers.runner"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    ) as child:
        try:
            pickle.dump(payload, child.stdin)
            child.stdin.flush()
            deadline = time.monotonic() + 10
            while not started.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            assert started.exists()
            child.stdin.close()
            assert child.wait(timeout=5) == 1
        finally:
            if child.poll() is None:
                child.kill()


@pytest.mark.parametrize("exit_code", [0, 3, None])
def test_stop_reports_exit_failure_or_timeout(exit_code):
    supervisor = DriverSupervisor("localhost", 8000)
    proc = Mock()
    proc.wait.side_effect = (
        [subprocess.TimeoutExpired("driver", 10), -9] if exit_code is None else [exit_code]
    )
    supervisor._drivers["sensor"] = {"name": "sensor", "proc": proc}

    if exit_code == 3:
        with pytest.raises(RuntimeError, match="exit code 3"):
            supervisor.stop_driver("sensor")
    else:
        result = supervisor.stop_driver("sensor")
        assert result["stopped"] is (exit_code == 0)
        if exit_code is None:
            assert result["error"] == "shutdown timed out"
            proc.kill.assert_called_once()
    proc.stdin.close.assert_called_once()
    assert supervisor.list_drivers() == []


def test_stop_all_signals_every_driver_first_and_shares_one_deadline(monkeypatch, caplog):
    supervisor = DriverSupervisor("localhost", 8000)
    failed, stuck = Mock(), Mock()
    supervisor._drivers = {
        "failed": {"name": "failed", "proc": failed},
        "stuck": {"name": "stuck", "proc": stuck},
    }
    monkeypatch.setattr("acquirium.Drivers.supervisor.time.monotonic", Mock(side_effect=[100, 104, 110]))

    def failed_exit(*, timeout):
        # Waiting on the first process must not delay signaling the others.
        stuck.stdin.close.assert_called_once()
        return 3

    failed.wait.side_effect = failed_exit
    stuck.wait.side_effect = [subprocess.TimeoutExpired("driver", 0), -9]
    supervisor.stop_all(timeout=10)

    failed.wait.assert_called_once_with(timeout=6)
    assert stuck.wait.call_args_list == [call(timeout=0), call()]
    stuck.kill.assert_called_once()
    assert "exit code 3" in caplog.text
    assert supervisor.list_drivers() == []
