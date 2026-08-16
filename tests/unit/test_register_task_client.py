"""Client-side tests for Acquirium.register_task / @aq.task (HTTP mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from acquirium.Client.explore.core import Query
from acquirium.internals.models import TaskSpec

CLS_A = "urn:test#TypeA"


def check(ctx):
    return []


@pytest.fixture
def aq():
    with patch("acquirium.Client.client.requests") as mock_requests:
        ok = MagicMock()
        ok.json.return_value = {"ok": True, "name": "check", "outputs": [], "load_error": None,
                                "replaced": False}
        ok.raise_for_status = MagicMock()
        ok.status_code = 200
        mock_requests.get.return_value = ok
        mock_requests.post.return_value = ok
        from acquirium import Acquirium
        instance = Acquirium(server_url="localhost", server_port=8000)
        instance._mock_requests = mock_requests
        yield instance


def posted_spec(aq) -> TaskSpec:
    call = aq._mock_requests.post.call_args
    assert call.args[0].endswith("/apps/register_task")
    return TaskSpec(**call.kwargs["json"])


class TestRegisterTask:
    def test_ships_query_and_function(self, aq):
        q = Query(client=None).entity(CLS_A, alias="a").measurement(alias="m")
        aq.register_task(check, query=q, outputs=[{"kind": "event", "point_uri": "urn:o"}],
                         interval=15.0, run_mode="interval", params={"k": 1})
        spec = posted_spec(aq)
        assert spec.name == "check"
        assert spec.fn_name == "check" and "def check(ctx):" in spec.fn_source
        assert spec.fn_blob is not None and spec.python_version
        # The query round-trips exactly.
        assert Query.from_dict(spec.query).query_graph == q.query_graph
        assert spec.outputs[0].kind == "event"
        assert (spec.interval, spec.run_mode, spec.params) == (15.0, "interval", {"k": 1})
        assert aq._mock_requests.post.call_args.kwargs["params"] == {"replace": False}

    def test_name_override_and_replace(self, aq):
        aq.register_task(check, name="tds_monitor", replace=True)
        assert posted_spec(aq).name == "tds_monitor"
        assert aq._mock_requests.post.call_args.kwargs["params"] == {"replace": True}

    def test_lambda_rejected_before_any_request(self, aq):
        with pytest.raises(ValueError, match="named function"):
            aq.register_task(lambda ctx: [])
        aq._mock_requests.post.assert_not_called()

    def test_decorator_registers_and_returns_fn(self, aq):
        @aq.task(name="deco", interval=5.0)
        def body(ctx):
            return []

        assert body(None) == []                      # still callable locally
        spec = posted_spec(aq)
        assert (spec.name, spec.fn_name, spec.interval) == ("deco", "body", 5.0)

    def test_returns_apps_response(self, aq):
        from acquirium.Client.app_display import AppsResponse
        assert isinstance(aq.register_task(check), AppsResponse)
