"""`acquirium example init`: the example plant's files, without a server."""

import polars as pl
from rdflib import Graph
from typer.testing import CliRunner

from acquirium.cli import app
from acquirium.examples import seawater_ro


def _init(path, *extra):
    return CliRunner().invoke(app, ["example", "init", str(path), *extra])


def test_writes_the_model_and_a_year_of_data(tmp_path):
    target = tmp_path / "example"

    result = _init(target)

    assert result.exit_code == 0, result.output
    assert f"Wrote {target / 'data.csv'} (8760 rows, 32 streams)" in result.output
    assert (target / "model.ttl").read_bytes() == seawater_ro.model_path.read_bytes()
    data = pl.read_csv(target / "data.csv", try_parse_dates=True)
    assert data.columns[0] == "time" and data.shape == (8760, 33)
    assert data.schema["time"] == pl.Datetime("us", "UTC")
    assert str(data["time"][0]) == "2025-01-01 00:00:00+00:00"


def test_the_columns_are_the_property_names_of_the_model(tmp_path):
    target = tmp_path / "example"
    _init(target, "--step", "1440")

    model = Graph().parse(target / "model.ttl")
    subjects = {str(subject) for subject in model.subjects()}
    columns = pl.read_csv(target / "data.csv").columns[1:]
    assert all(f"urn:swro/{column}" in subjects for column in columns)


def test_step_sets_the_minutes_between_rows(tmp_path):
    target = tmp_path / "example"

    result = _init(target, "--step", "1440")

    assert result.exit_code == 0, result.output
    assert pl.read_csv(target / "data.csv").height == 365


def test_existing_files_need_force(tmp_path):
    target = tmp_path / "example"
    _init(target, "--step", "1440")
    (target / "data.csv").write_text("mine")

    refused = _init(target, "--step", "1440")
    assert refused.exit_code == 1 and "already exist" in refused.output
    assert (target / "data.csv").read_text() == "mine"

    forced = _init(target, "--step", "1440", "--force")
    assert forced.exit_code == 0 and pl.read_csv(target / "data.csv").height == 365


def test_step_must_be_positive(tmp_path):
    result = _init(tmp_path / "example", "--step", "0")
    assert result.exit_code == 1 and "at least 1 minute" in result.output
