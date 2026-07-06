"""Unit tests for ``[prefixes]`` config loading in acquirium.Server.config."""

from __future__ import annotations

import textwrap

from acquirium.Server.config import load_prefix_config


def _write_cfg(tmp_path, body: str):
    p = tmp_path / "acquirium.toml"
    p.write_text(textwrap.dedent(body))
    return p


def test_no_env_var_returns_empty(monkeypatch):
    monkeypatch.delenv("ACQUIRIUM_CONFIG", raising=False)
    assert load_prefix_config() == {}


def test_missing_file_returns_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(tmp_path / "nope.toml"))
    assert load_prefix_config() == {}


def test_reads_prefix_table(monkeypatch, tmp_path):
    p = _write_cfg(
        tmp_path,
        """
        [prefixes]
        s223 = "http://data.ashrae.org/standard223#"
        nawi = "urn:nawi-water-ontology#"
        """,
    )
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(p))
    assert load_prefix_config() == {
        "s223": "http://data.ashrae.org/standard223#",
        "nawi": "urn:nawi-water-ontology#",
    }


def test_no_prefix_section_returns_empty(monkeypatch, tmp_path):
    p = _write_cfg(tmp_path, "[driver]\nserver_url = 'localhost'\n")
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(p))
    assert load_prefix_config() == {}


def test_non_string_entries_are_skipped(monkeypatch, tmp_path):
    # a sub-table (non-string value) should be ignored, not crash
    p = _write_cfg(
        tmp_path,
        """
        [prefixes]
        s223 = "http://data.ashrae.org/standard223#"
        nested = { x = 1 }
        """,
    )
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(p))
    out = load_prefix_config()
    assert out == {"s223": "http://data.ashrae.org/standard223#"}


def test_malformed_toml_returns_empty(monkeypatch, tmp_path):
    p = tmp_path / "bad.toml"
    p.write_text("this is : not : toml {{{")
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(p))
    assert load_prefix_config() == {}


def test_non_table_prefixes_returns_empty(monkeypatch, tmp_path):
    # prefixes assigned a scalar string instead of a table
    p = _write_cfg(tmp_path, 'prefixes = "not a table"\n')
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(p))
    assert load_prefix_config() == {}
