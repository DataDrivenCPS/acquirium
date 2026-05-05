from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

from acquirium.Client.acquirium import Acquirium


def _load_dpr_driver():
    root = Path(__file__).resolve().parents[2]
    driver_path = root / "dpr-trailer-data" / "dpr_csv_driver.py"
    spec = importlib.util.spec_from_file_location("dpr_csv_driver", driver_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.DPRTrailerCSVDriver


def _write_dpr_csv(path: Path, value: float) -> None:
    path.write_text(
        "metadata banner\n"
        "Date,Time,Runtime hr\n"
        f"12/01/2024,05:32:00 PM,{value}\n"
    )


def test_dpr_trailer_driver_uses_one_source_id_for_all_csv_files(tmp_path):
    driver_cls = _load_dpr_driver()
    aq = MagicMock()
    aq.client = MagicMock()
    aq.register_datasource.return_value = "dpr-trailer"
    aq.register_streams.side_effect = lambda streams: Acquirium.register_streams(aq, streams)
    aq.insert_timeseries_polars.return_value = {"ok": True, "rows_inserted": 1}
    config = {
        "driver": {
            "source_id": "dpr-trailer",
            "watch_dir": str(tmp_path),
            "format": "wide",
            "date_col": "Date",
            "clock_col": "Time",
            "skip_rows": [1],
        }
    }
    driver = driver_cls(aq, config)
    driver.setup()
    _write_dpr_csv(tmp_path / "a.csv", 1.0)
    _write_dpr_csv(tmp_path / "b.csv", 2.0)

    driver.tick()

    source_ids = [
        call.args[0]
        for call in aq.insert_timeseries_polars.call_args_list
    ]
    assert source_ids == ["dpr-trailer", "dpr-trailer"]
    aq.register_datasource.assert_called_once_with("dpr-trailer")
