"""Unit tests for CSVIngestDriver — no server required."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from rdflib import Graph, Literal, URIRef

from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver
from acquirium.Client.client import AcquiriumClient
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    HAS_EXTERNAL_REFERENCE,
)


# ------------------------------------------------------------------ fixtures


def make_driver(cfg_overrides: dict | None = None, tmp_path: Path | None = None) -> CSVIngestDriver:
    aq = MagicMock()
    aq.client = MagicMock()
    aq.register_datasource.return_value = "csv_files"
    aq.register_streams.side_effect = lambda streams: AcquiriumClient.register_streams(aq.client, streams)
    aq.insert_timeseries_arrow.return_value = {"ok": True, "rows_inserted": 0}
    watch = str(tmp_path) if tmp_path else "/tmp/csv_test_watch"
    driver = CSVIngestDriver(aq, {"driver": {
        "watch_dir": watch, "glob": ["*.csv", "*.tsv"],
        "source_id": "csv_files", "format": "wide",
        **(cfg_overrides or {}),
    }})
    driver.setup()
    return driver


def parse(driver: CSVIngestDriver, path: Path, cursor=None):
    """Run the driver's own read(), regrouped as {ref_name: [(ts, value)]}."""
    result = driver.read(path, cursor)
    observations, next_cursor = result.observations, result.next_cursor
    batch: dict[str, list[tuple[datetime, str]]] = {}
    if observations is not None:
        for row in observations.iter_rows(named=True):
            batch.setdefault(row["ref_name"], []).append((row["ts"], row["value"]))
    return batch, next_cursor


def _wide_csv(tmp_path: Path) -> Path:
    p = tmp_path / "wide.csv"
    p.write_text("time,temp,rh\n2024-01-01T00:00:00Z,22.5,55.0\n2024-01-02T00:00:00Z,23.0,60.0\n")
    return p


def _narrow_csv(tmp_path: Path) -> Path:
    p = tmp_path / "narrow.csv"
    p.write_text(
        "time,id,value\n"
        "2024-01-01T00:00:00Z,sensor/temp,22.5\n"
        "2024-01-01T00:00:00Z,sensor/rh,55.0\n"
        "2024-01-02T00:00:00Z,sensor/temp,23.0\n"
    )
    return p


# ------------------------------------------------------------------ wide parsing


def test_parse_wide_basic(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_csv(tmp_path))
    assert set(batch) == {"temp", "rh"}
    assert cursor == 2
    assert batch["temp"][0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), "22.5")
    assert batch["rh"][0][1] == "55.0"


def test_null_values_config_drops_sentinel_values(tmp_path):
    p = tmp_path / "nulls.csv"
    p.write_text(
        "time,temp\n"
        "2024-01-01T00:00:00Z,Null\n"
        "2024-01-02T00:00:00Z,21.0\n"
    )
    driver = make_driver({"null_values": ["Null"]}, tmp_path=tmp_path)
    batch, _ = parse(driver, p)
    assert batch["temp"] == [(datetime(2024, 1, 2, tzinfo=timezone.utc), "21.0")]


def test_prepare_frame_hook_sees_text_frame(tmp_path):
    """Every column reads as text, and prepare_frame runs on the raw frame
    before timestamps are parsed."""
    import polars as pl

    class Fixer(CSVIngestDriver):
        def prepare_frame(self, df, path):
            assert df.schema["temp"] == pl.Utf8
            return df.with_columns(pl.col("temp").str.replace_all(",", ""))

    p = tmp_path / "commas.csv"
    p.write_text(
        "time,temp\n"
        "2024-01-01T00:00:00Z,731.00\n"
        '2024-01-02T00:00:00Z,"3,293.00"\n'
    )
    aq = MagicMock()
    driver = Fixer(aq, {"driver": {
        "watch_dir": str(tmp_path), "glob": "*.csv",
        "source_id": "csv_files", "format": "wide",
    }})
    driver.setup()
    batch, _ = parse(driver, p)
    assert [v for _, v in batch["temp"]] == ["731.00", "3293.00"]


def test_column_shape_change_after_sample_window_does_not_fail(tmp_path):
    """A status word or a first decimal reading far into a numeric column
    must not fail the file (polars would type the column from a row sample)."""
    rows = ["time,valve,do"]
    for i in range(150):
        rows.append(f"2024-01-01T00:{i // 60:02d}:{i % 60:02d}Z,0,7.1")
    rows.append("2024-01-01T00:02:30Z,86.306,LOW")
    p = tmp_path / "scada.csv"
    p.write_text("\n".join(rows) + "\n")
    driver = make_driver({}, tmp_path=tmp_path)
    batch, _ = parse(driver, p)
    assert batch["valve"][-1][1] == "86.306"
    assert batch["do"][-1][1] == "LOW"


def test_declare_stream_hook_attaches_metadata(tmp_path):
    class Meta(CSVIngestDriver):
        def declare_stream(self, ref_name):
            self.declare(ref_name, label=f"L:{ref_name}")

    aq = MagicMock()
    driver = Meta(aq, {"driver": {
        "watch_dir": str(tmp_path), "glob": "*.csv",
        "source_id": "csv_files", "format": "wide",
    }})
    driver.setup()
    parse(driver, _wide_csv(tmp_path))
    assert driver._declarations[("csv_files", "temp")] == {"label": "L:temp"}
    assert driver._declarations[("csv_files", "rh")] == {"label": "L:rh"}


def test_parse_wide_skip_rows(tmp_path):
    p = tmp_path / "wide_skip_rows.csv"
    p.write_text(
        "Report generated by system X\n"
        "Do not edit manually\n"
        "time,temp,rh\n"
        "2024-01-01T00:00:00Z,22.5,55.0\n"
        "2024-01-02T00:00:00Z,23.0,60.0\n"
    )
    driver = make_driver({"skip_rows": [1, 2]}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2
    assert batch["temp"][0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), "22.5")
    assert batch["rh"][1] == (datetime(2024, 1, 2, tzinfo=timezone.utc), "60.0")


def test_parse_wide_skip_rows_by_file(tmp_path):
    sub = tmp_path / "subdir"
    sub.mkdir()
    p = sub / "wide_skip_rows.csv"
    p.write_text(
        "Report generated by system X\n"
        "time,temp,rh\n"
        "2024-01-01T00:00:00Z,22.5,55.0\n"
        "2024-01-02T00:00:00Z,23.0,60.0\n"
    )
    driver = make_driver({"skip_rows": {"subdir/wide_skip_rows.csv": [1, 3]}}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 1
    assert batch["temp"] == [(datetime(2024, 1, 2, tzinfo=timezone.utc), "23.0")]
    assert batch["rh"] == [(datetime(2024, 1, 2, tzinfo=timezone.utc), "60.0")]


def test_parse_wide_skip_cols_from_config(tmp_path):
    p = tmp_path / "wide_skip_cols.csv"
    p.write_text(
        "time,temp,rh,notes\n"
        "2024-01-01T00:00:00Z,22.5,55.0,ok\n"
        "2024-01-02T00:00:00Z,23.0,60.0,still ok\n"
    )
    driver = make_driver({"skip_cols": ["notes"]}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2
    assert set(batch) == {"temp", "rh"}


def test_parse_wide_preserves_column_names(tmp_path):
    p = tmp_path / "unsafe_headers.csv"
    p.write_text("time,UV Intensity (mW/cm^2)\n2024-01-01T00:00:00Z,1.5\n")
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 1
    assert set(batch) == {"UV Intensity (mW/cm^2)"}


def test_parse_wide_date_only_col(tmp_path):
    p = tmp_path / "dates.csv"
    p.write_text("Date,temp\n2024-01-01,22.5\n2024-01-02,23.0\n")
    driver = make_driver({"time_col": "Date"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2
    assert batch["temp"][0][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_parse_wide_non_iso_date_with_format(tmp_path):
    p = tmp_path / "us_dates.csv"
    p.write_text("Date,temp\n01/15/2024,22.5\n01/16/2024,23.0\n")
    driver = make_driver({"time_col": "Date", "date_format": "%m/%d/%Y"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 2
    assert batch["temp"][0][0] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_parse_wide_skips_null_values(tmp_path):
    p = tmp_path / "nulls.csv"
    p.write_text("time,temp\n2024-01-01T00:00:00Z,\n2024-01-02T00:00:00Z,23.0\n")
    driver = make_driver(tmp_path=tmp_path)
    batch, _ = parse(driver, p)
    assert batch["temp"] == [(datetime(2024, 1, 2, tzinfo=timezone.utc), "23.0")]


def test_parse_wide_discovers_ts_column(tmp_path):
    p = tmp_path / "no_time.csv"
    p.write_text("ts,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 1
    assert batch["temp"] == [(datetime(2024, 1, 1, tzinfo=timezone.utc), "22.5")]


def test_parse_wide_discovers_split_date_and_time(tmp_path):
    p = tmp_path / "split_timestamp.csv"
    p.write_text("Date,Time,temp\n12/1/2024,5:32:52 PM,22.5\n")
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 1
    assert batch["temp"] == [
        (datetime(2024, 12, 1, 17, 32, 52, tzinfo=timezone.utc), "22.5")
    ]


# ------------------------------------------------------------------ narrow parsing


def test_parse_narrow_basic(tmp_path):
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, _narrow_csv(tmp_path))
    assert set(batch) == {"sensor/temp", "sensor/rh"}
    assert cursor == 3
    assert len(batch["sensor/temp"]) == 2


def test_parse_narrow_missing_id_col_raises(tmp_path):
    p = tmp_path / "no_id.csv"
    p.write_text("time,value\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver({"format": "narrow"}, tmp_path=tmp_path)
    with pytest.raises(ValueError, match="column 'id'"):
        parse(driver, p)


# ------------------------------------------------------------------ ragged rows


def _ragged_csv(tmp_path: Path) -> Path:
    p = tmp_path / "ragged.csv"
    p.write_text(
        "time,temp,rh\n"
        "2024-01-01T00:00:00Z,22.5,55.0,99.9\n"  # extra cell
        "2024-01-02T00:00:00Z,23.0\n"            # missing cell
        "2024-01-03T00:00:00Z,24.0,60.0\n"       # well-formed
    )
    return p


def test_ragged_rows_ignored_by_default(tmp_path):
    """Extra cells are dropped, missing cells become null; rows are kept."""
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _ragged_csv(tmp_path))
    assert cursor == 3
    assert [v for _, v in batch["temp"]] == ["22.5", "23.0", "24.0"]
    assert [v for _, v in batch["rh"]] == ["55.0", "60.0"]  # null from short row dropped


def test_ragged_lines_skip_drops_whole_rows(tmp_path):
    driver = make_driver({"ragged_lines": "skip"}, tmp_path=tmp_path)
    batch, cursor = parse(driver, _ragged_csv(tmp_path))
    assert cursor == 1
    assert batch["temp"] == [(datetime(2024, 1, 3, tzinfo=timezone.utc), "24.0")]


def test_ragged_lines_error_raises_on_extra_cells(tmp_path):
    driver = make_driver({"ragged_lines": "error"}, tmp_path=tmp_path)
    with pytest.raises(pl.exceptions.ComputeError, match="more fields"):
        parse(driver, _ragged_csv(tmp_path))


def test_ragged_lines_invalid_value_raises(tmp_path):
    driver = make_driver({"ragged_lines": "explode"}, tmp_path=tmp_path)
    with pytest.raises(ValueError, match="ragged_lines"):
        parse(driver, _ragged_csv(tmp_path))


# ------------------------------------------------------------------ header detection


def test_header_contains_skips_banner_rows(tmp_path):
    p = tmp_path / "banner.csv"
    p.write_text(
        "EXPORT from PLC, station 4\n"
        "generated 2024-01-05\n"
        "time,temp\n"
        "2024-01-01T00:00:00Z,22.5\n"
    )
    driver = make_driver({"header_contains": ["time", "temp"]}, tmp_path=tmp_path)
    batch, cursor = parse(driver, p)
    assert cursor == 1
    assert batch["temp"] == [(datetime(2024, 1, 1, tzinfo=timezone.utc), "22.5")]


def test_header_contains_handles_file_without_banner(tmp_path):
    driver = make_driver({"header_contains": ["time", "temp"]}, tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_csv(tmp_path))
    assert cursor == 2
    assert batch["temp"][0][1] == "22.5"


def test_header_contains_missing_header_raises(tmp_path):
    p = tmp_path / "no_header.csv"
    p.write_text("just,some,cells\n1,2,3\n")
    driver = make_driver({"header_contains": ["time", "temp"]}, tmp_path=tmp_path)
    with pytest.raises(ValueError, match="no header row"):
        parse(driver, p)


def test_tsv_parsed_correctly(tmp_path):
    p = tmp_path / "data.tsv"
    p.write_text("time\ttemp\n2024-01-01T00:00:00Z\t22.5\n")
    driver = make_driver(tmp_path=tmp_path)
    batch, _ = parse(driver, p)
    assert batch["temp"][0][1] == "22.5"


# ------------------------------------------------------------------ per-file source_id


def test_loop_uses_explicit_source_id(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_csv(tmp_path)
    driver.tick()
    source_id, table = driver.aq.insert_timeseries_arrow.call_args[0]
    assert source_id == "csv_files"
    assert set(pl.from_arrow(table)["ref_name"].to_list()) == {"temp", "rh"}


def test_loop_source_id_is_stable_across_subdirectories(tmp_path):
    sub = tmp_path / "sensors"
    sub.mkdir()
    path = sub / "data.csv"
    path.write_text("time,flow\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.tick()
    source_id, table = driver.aq.insert_timeseries_arrow.call_args[0]
    assert source_id == "csv_files"
    assert "flow" in pl.from_arrow(table)["ref_name"].to_list()


def test_loop_registers_streams_before_insert(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    _wide_csv(tmp_path)

    def assert_registered_first(source_id, table):
        assert driver.aq.client.insert_graph.called
        return {"ok": True, "rows_inserted": table.num_rows}

    driver.aq.insert_timeseries_arrow.side_effect = assert_registered_first
    driver.tick()


def test_loop_marks_text_only_csv_streams_as_text(tmp_path):
    p = tmp_path / "mixed.csv"
    p.write_text("time,temp,state\n2024-01-01T00:00:00Z,22.5,ON\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.tick()

    g = Graph().parse(data=driver.aq.client.insert_graph.call_args[0][0], format="turtle")
    source_id = "csv_files"
    assert (compute_ref_uri(source_id, "temp"), ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    assert (compute_ref_uri(source_id, "state"), ACQUIRIUM_VALUE_KIND, Literal("text")) in g


def test_loop_registers_mixed_csv_streams_as_numeric(tmp_path):
    p = tmp_path / "mixed_numeric.csv"
    p.write_text(
        "time,mode\n"
        "2024-01-01T00:00:00Z,1.0\n"
        "2024-01-02T00:00:00Z,Manual Control\n"
    )
    driver = make_driver(tmp_path=tmp_path)
    driver.tick()

    g = Graph().parse(data=driver.aq.client.insert_graph.call_args[0][0], format="turtle")
    ref = compute_ref_uri("csv_files", "mode")
    assert (ref, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g


def test_loop_mints_valid_dummy_point_uri_for_messy_ref_name(tmp_path):
    """A ref_name full of URI-hostile characters still yields a valid dummy point URI."""
    p = tmp_path / "bad_uri.csv"
    p.write_text("time,UV-Ultraviolet Intensity (mW/cm^2)\n2024-01-01T00:00:00Z,1.0\n")
    driver = make_driver(tmp_path=tmp_path)
    driver.tick()

    g = Graph().parse(data=driver.aq.client.insert_graph.call_args[0][0], format="turtle")
    source_id = "csv_files"
    ref_name = "UV-Ultraviolet Intensity (mW/cm^2)"
    ref_uri = compute_ref_uri(source_id, ref_name)
    assert (ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)) in g
    assert (ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)) in g
    assert (ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    points = list(g.subjects(HAS_EXTERNAL_REFERENCE, ref_uri))
    assert points == [URIRef(f"{ref_uri}__point")]


# ------------------------------------------------------------------ cursor / paging


def test_cursor_skips_already_seen_rows(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_csv(tmp_path)  # 2 data rows
    batch, cursor = parse(driver, path, cursor=1)
    assert cursor == 2
    assert batch["temp"][0][1] == "23.0"


def test_cursor_past_end_is_unchanged(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    batch, cursor = parse(driver, _wide_csv(tmp_path), cursor=10)
    assert cursor == 10
    assert batch == {}


def test_loop_advances_cursor_on_each_tick(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = tmp_path / "growing.csv"
    path.write_text("time,temp\n2024-01-01T00:00:00Z,1.0\n")
    driver.tick()
    assert driver.aq.insert_timeseries_arrow.call_count == 1
    assert driver._cursors[str(path)] == 1

    with path.open("a") as f:
        f.write("2024-01-02T00:00:00Z,2.0\n")
    driver.tick()
    assert driver.aq.insert_timeseries_arrow.call_count == 2
    assert driver._cursors[str(path)] == 2

    driver.tick()
    assert driver.aq.insert_timeseries_arrow.call_count == 2


def test_file_stays_in_place_after_insert(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    path = _wide_csv(tmp_path)
    driver.tick()
    assert path.exists()


# ------------------------------------------------------------------ error recovery


def test_tick_propagates_insert_failure(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.aq.insert_timeseries_arrow.side_effect = RuntimeError("server down")
    path = _wide_csv(tmp_path)
    with pytest.raises(RuntimeError, match="server down"):
        driver.tick()
    assert str(path) not in driver._cursors


def test_tick_does_not_advance_cursor_on_false_insert_result(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.aq.insert_timeseries_arrow.return_value = {"ok": False, "rows_inserted": 0}
    path = _wide_csv(tmp_path)
    with pytest.raises(RuntimeError, match="reported failure"):
        driver.tick()
    assert str(path) not in driver._cursors


def test_setup_requires_explicit_source_id(tmp_path):
    driver = CSVIngestDriver(MagicMock(), {"driver": {
        "watch_dir": str(tmp_path), "glob": "*.csv", "format": "wide",
    }})
    with pytest.raises(ValueError, match="require driver.source_id"):
        driver.setup()


def test_setup_requires_explicit_glob(tmp_path):
    driver = CSVIngestDriver(MagicMock(), {"driver": {
        "watch_dir": str(tmp_path), "source_id": "csv_files", "format": "wide",
    }})
    with pytest.raises(ValueError, match="require driver.glob"):
        driver.setup()


def test_setup_requires_explicit_watch_dir(tmp_path):
    driver = CSVIngestDriver(MagicMock(), {"driver": {
        "source_id": "csv_files", "glob": "*.csv", "format": "wide",
    }})
    with pytest.raises(ValueError, match="require driver.watch_dir"):
        driver.setup()


def test_configured_glob_controls_file_discovery(tmp_path):
    (tmp_path / "ignored.csv").write_text(
        "time,temp\n2024-01-01T00:00:00Z,1.0\n"
    )
    (tmp_path / "selected.export").write_text(
        "time,temp\n2024-01-01T00:00:00Z,2.0\n"
    )
    driver = make_driver({"glob": "*.export"}, tmp_path=tmp_path)
    driver.tick()

    driver.aq.insert_timeseries_arrow.assert_called_once()
    _, table = driver.aq.insert_timeseries_arrow.call_args.args
    assert pl.from_arrow(table)["value"].to_list() == ["2.0"]


def test_tick_propagates_registration_failure(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    driver.aq.register_streams.side_effect = RuntimeError("graph down")
    path = _wide_csv(tmp_path)
    with pytest.raises(RuntimeError, match="graph down"):
        driver.tick()
    assert str(path) not in driver._cursors
    driver.aq.insert_timeseries_arrow.assert_not_called()


def test_loop_skips_bad_file_and_continues(tmp_path):
    driver = make_driver(tmp_path=tmp_path)
    (tmp_path / "bad.csv").write_text("not,valid\ncsvgarbagehere\n")
    (tmp_path / "good.csv").write_text("time,temp\n2024-01-01T00:00:00Z,22.5\n")
    driver.tick()
    driver.aq.insert_timeseries_arrow.assert_called_once()
