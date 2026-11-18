"""Where the `acquirium driver` and `acquirium app` commands look for the server."""

import pytest

from acquirium.cli import _server_base_url


@pytest.mark.parametrize(
    "cfg, expected",
    [
        ({}, "http://127.0.0.1:8000"),
        # a config that only says where its server listens reaches that server
        ({"server": {"host": "127.0.0.1", "port": 8765}}, "http://127.0.0.1:8765"),
        ({"server": {"host": "0.0.0.0", "port": 9000}}, "http://127.0.0.1:9000"),
        # [driver] names the server explicitly and wins
        (
            {"server": {"port": 8765}, "driver": {"server_url": "plant.example", "server_port": 9100}},
            "http://plant.example:9100",
        ),
        ({"server": {"port": 8765}, "driver": {"use_ssl": True}}, "https://127.0.0.1:8765"),
        # port 0 means "any free port", which the file cannot tell
        ({"server": {"port": 0}}, "http://127.0.0.1:8000"),
    ],
)
def test_address_from_the_config(cfg, expected):
    assert _server_base_url(cfg) == expected


def test_command_line_options_win():
    cfg = {"server": {"port": 8765}, "driver": {"server_port": 9100}}

    assert _server_base_url(cfg, "other.example", 7000) == "http://other.example:7000"
