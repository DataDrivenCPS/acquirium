"""The history harness loads, reopens and corrects every case it reports."""

import json
import subprocess
import sys

import pytest


@pytest.mark.integration
def test_history_cases_reopen_and_repair_their_means(tmp_path):
    output = tmp_path / "limits"
    subprocess.run(
        [sys.executable, "-m", "examples.operating_limits.run", "--output", str(output),
         "--sizes", "20000", "60000", "--streams", "10", "--derived"],
        check=True, capture_output=True, text=True,
    )
    results = json.loads((output / "results.json").read_text())["results"]
    assert [item["load"]["readings"] for item in results] == [20_000, 60_000]
    for item in results:
        seconds = item["load"]["readings"] // 10
        # One 15-minute mean per started bucket of each stream.
        assert item["load"]["derived_rows"] == 10 * -(-seconds // 900)
        assert item["reopen"]["verified"]
        assert item["reopen"]["stream_rows"] == seconds
        assert item["reopen"]["hour_rows"] == min(seconds, 3601)
    assert not list(output.glob("*.duckdb*"))
    assert "| 60,000 |" in (output / "summary.md").read_text()
