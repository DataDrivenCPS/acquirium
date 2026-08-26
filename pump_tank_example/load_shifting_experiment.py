"""Run the pump/tank load-shift optimizer with the experiment API."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import acquirium as aq

from helpers.build import build_model, solve_model
from helpers.config import load_config
from helpers.results import extract_results


BLDG = "urn:flex-pse-example:pump-tank-battery#"
ac = aq.Acquirium(server_url="localhost", server_port=8000)


def result_series(results: Any) -> dict[str, Iterable[tuple[Any, Any]]]:
    """Adapt the existing result object to the named experiment outputs."""
    return results.timeseries


# Declare the study and its reusable variables once. Each `study.start()`
# below creates a fresh run whose values are isolated from prior scenarios.
# Keeping these declarations at module scope also makes the experiment's
# vocabulary visible before a reader gets to the solver implementation.
study = ac.study.define("pump-tank-load-shift")
configuration = study.input("configuration").json()
configuration_file = study.input("configuration file").file(
    media_type="application/json"
)
operator_note = study.input("operator note").text()

total_operating_cost = study.output("total operating cost").scalar(unit="USD")
peak_pumping_volume = study.output("peak pumping volume").scalar(unit="M3")
peak_net_energy = study.output("peak net energy").scalar(unit="KiloW-HR")
solver_result = study.output("solver result").json()

facility_net_load = study.output("facility net load").timeseries(
    observed=ac.point(f"{BLDG}facility-net-load"), unit="KiloW-HR"
)
pump_inlet_flow = study.output("pump inlet flow").timeseries(
    observed=ac.point(f"{BLDG}pump-in-flow-vol"), unit="M3-PER-HR"
)
tank_volume = study.output("tank volume").timeseries(
    observed=ac.point(f"{BLDG}tank-volume"), unit="M3"
)
battery_soc = study.output("battery state of charge").timeseries(
    observed=ac.point(f"{BLDG}battery-soc"), unit="PERCENT"
)
battery_net_power = study.output("battery net power").timeseries(
    observed=ac.point(f"{BLDG}battery-grid-power-net"), unit="KiloW"
)
solver_log = study.log("solver events")


def run_load_shift(config_path: Path, *, note: str = "", tags: list[str] | None = None):
    return run_scenario(config_path, note=note, tags=tags)


def run_scenario(config_path: Path, *, note: str = "", tags: list[str] | None = None):
    """The model build/solve flow is unchanged; only provenance surrounds it."""
    e = study.start(metadata={
        "example": "pump_tank", "config_path": str(config_path), "tags": tags or [],
    })
    try:
        config = load_config(config_path)
        configuration.set(config)
        configuration_file.attach(config_path)
        operator_note.set(note)

        # This is the original optimization workflow. The experiment API
        # records its inputs and results; it does not orchestrate the solve.
        model = build_model(config)
        solve_model(model)
        results = extract_results(model, config)

        total_operating_cost.set(results.total_cost)
        peak_pumping_volume.set(results.peak_pumping)
        peak_net_energy.set(results.peak_net_energy)
        solver_result.set({"termination": results.termination, "objective": results.total_cost})

        series = result_series(results)
        # Each named result is written explicitly so this remains the visible
        # contract between the optimizer result object and study variables.
        facility_net_load.add(series["facility-net-load"])
        pump_inlet_flow.add(series["pump-in-flow-vol"])
        tank_volume.add(series["tank-volume"])
        if "battery-soc" in series:
            battery_soc.add(series["battery-soc"])
        if "battery-power-net" in series:
            battery_net_power.add(series["battery-power-net"])

        solver_log.append({"event": "solve-complete", "termination": results.termination})
        return e.finish()
    except Exception as error:
        e.fail(error)
        raise


if __name__ == "__main__":
    run_load_shift(Path(__file__).parent / "config.json", note="initial run")
