"""Real local server tests; no Docker services required."""
import json
import subprocess
import sys

import pytest

import acquirium as aq
from acquirium import runtime


SCRIPT = """
import json, sys
import acquirium as aq
from acquirium import runtime
options = {'config': sys.argv[1]} if sys.argv[1].endswith('.toml') else {'data_dir': sys.argv[1]}
ac = aq.init(**options, exact_only=True, timeout=180)
print(json.dumps({'address': ac.client.base_url, 'owner': runtime._session[3] is not None}), flush=True)
sys.stdin.readline()
# Normal interpreter exit must invoke shutdown through atexit.
"""


@pytest.mark.integration
@pytest.mark.parametrize("configured", [False, True])
def test_concurrent_start_attach_exit_and_restart(tmp_path, monkeypatch, configured):
    root = tmp_path
    options = {"data_dir": root}
    argument = str(root)
    if configured:
        config = tmp_path / "custom.toml"
        config.write_text('[server]\ndata_dir = "data"\nport = 0\nexact_only = true\nduckdb_path = "data/custom.duckdb"\n')
        root = tmp_path / "data"
        options = {"config": config}
        argument = str(config)
        # Inherited settings must not recreate data or override config paths.
        monkeypatch.setenv("ACQUIRIUM_RECREATE", "true")
        monkeypatch.setenv("ACQUIRIUM_DATA_DIR", str(tmp_path / "wrong"))
    # Two scripts race to start; only one may own/open the embedded stores.
    processes = [subprocess.Popen([sys.executable, "-c", SCRIPT, argument],
                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                 stderr=subprocess.PIPE, text=True) for _ in range(2)]
    try:
        # Readiness has a deadline in each script; an early crash closes stdout.
        info = []
        for process in processes:
            line = process.stdout.readline()
            assert line, process.stderr.read()
            info.append(json.loads(line))
        assert sum(item["owner"] for item in info) == 1
        assert info[0]["address"] == info[1]["address"]
        owner = processes[next(i for i, item in enumerate(info) if item["owner"])]
        attached = processes[next(i for i, item in enumerate(info) if not item["owner"])]
        attached.communicate("exit\n", timeout=45)
        assert attached.returncode == 0

        ac = aq.init(**options, exact_only=True)
        assert runtime._session[3] is None
        assert aq.init(**options) is ac
        if configured:
            (tmp_path / "acquirium.toml").write_text(config.read_text())
            monkeypatch.chdir(tmp_path)
            assert aq.init() is ac
        ac.insert_graph('<urn:runtime:test> <urn:runtime:value> "persisted" .', source_id="runtime-test")
        assert ac.client.health()["ok"]
        aq.shutdown()  # Attached shutdown leaves the owner's server alive.
        ac = aq.init(address=info[0]["address"])
        assert ac.client.health()["ok"]
        aq.shutdown()

        owner.communicate("exit\n", timeout=45)
        assert owner.returncode == 0
        assert not runtime._running(root / ".runtime")
        assert not (root / ".runtime" / "server.json").exists()

        ac = aq.init(**options, exact_only=True, timeout=180)
        assert runtime._session[3] is not None
        result = ac.client.sparql_query('SELECT ?v WHERE { <urn:runtime:test> <urn:runtime:value> ?v }', wait_for_fresh=True)
        assert "persisted" in str(result)
        with pytest.raises(ValueError, match="different options"):
            aq.init(data_dir=tmp_path / "other")
        aq.shutdown()
        assert not runtime._running(root / ".runtime")
        assert not (root / ".runtime" / "server.json").exists()
        if configured:
            assert (root / "custom.duckdb").exists()
            assert not (tmp_path / "wrong").exists()
    finally:
        aq.shutdown()
        for process in processes:
            if process.poll() is None:
                try:
                    process.communicate("exit\n", timeout=45)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


@pytest.mark.integration
def test_stale_registry_does_not_block_default_config_start(tmp_path, monkeypatch):
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\ndata_dir = "data"\nport = 0\nexact_only = true\n')
    runtime_dir = tmp_path / "data" / ".runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "server.json").write_text('{"port": 1, "token": "stale", "config": "stale", "exact_only": true}')
    monkeypatch.chdir(tmp_path)
    ac = aq.init(timeout=180)
    try:
        assert ac.client.health()["ok"]
        assert json.loads((runtime_dir / "server.json").read_text())["port"] != 1
    finally:
        aq.shutdown()
