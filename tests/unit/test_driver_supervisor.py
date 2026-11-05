"""Driver shutdown preserves exit failures and a shared deadline."""

import subprocess
from unittest.mock import Mock, call

import pytest

from acquirium.Drivers.supervisor import DriverSupervisor


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
