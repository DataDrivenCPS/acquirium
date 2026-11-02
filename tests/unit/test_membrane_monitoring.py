"""Sequential and as-of Python apps against an independent whole-history recomputation."""

import math

from fastapi.testclient import TestClient
import pytest

from acquirium import Acquirium
from acquirium.apps import output_reference
from acquirium.Server.app import app as api
from acquirium.Server.manager import Manager
from examples.membrane_monitoring.declarations import (
    DRIFT,
    DRIFT_LIMITS,
    FIT,
    HOUR,
    MINUTE,
    PERMEABILITY,
    RESIDUAL,
    SKIDS,
    SMOOTHED,
    SMOOTHING,
    SOURCE,
    deploy,
    register,
)
from examples.membrane_monitoring.run import readings


@pytest.fixture
def client(tmp_path):
    manager = Manager(data_dir=tmp_path, exact_only=True)
    manager.recover_apps()
    api.state.manager = manager
    http = TestClient(api)
    client = Acquirium(health_timeout=None)
    client.client._http = http
    try:
        yield client
    finally:
        http.close()
        manager.close()


def name(skid, measurement):
    return f"{skid.rsplit(':', 1)[1]}-{measurement}"


def recompute(observations):
    """Every output, by reference, from the full history in time order."""
    expected = {}
    for skid in SKIDS:
        conductivity = sorted(observations[name(skid, "conductivity")].items())
        tau = SMOOTHING[skid]["tau_ns"]
        smoothed, last = {}, None
        for time, value in conductivity:
            if last is not None:
                weight = math.exp(-(time - last) / tau)
                value = weight * smoothed[last] + (1 - weight) * value
            smoothed[time], last = value, time
        limits = DRIFT_LIMITS[skid]
        cusum, alarm, statistic = {}, {}, 0.0
        for time, value in conductivity:
            statistic = max(0.0, statistic + value - limits["target"] - limits["slack"])
            cusum[time] = statistic
            if statistic > limits["limit"]:
                alarm[time] = statistic
        hours = {}
        for measurement in ("flux", "tmp"):
            for time, value in observations[name(skid, measurement)].items():
                hours.setdefault(time // HOUR * HOUR, {}).setdefault(measurement, []).append(value)
        fit = FIT[skid]
        estimate, variance = fit["initial"], fit["initial_variance"]
        permeability, variances = {}, {}
        for hour, values in sorted(hours.items()):
            x = sum(values["tmp"]) / len(values["tmp"])
            y = sum(values["flux"]) / len(values["flux"])
            gain = variance * x / (fit["forgetting"] + x * variance * x)
            estimate += gain * (y - x * estimate)
            variance = (1 - gain * x) * variance / fit["forgetting"]
            permeability[hour], variances[hour] = estimate, variance
        # Each time with both flux and TMP reads the latest estimate at or
        # before it, within two hours.
        flux, tmp = observations[name(skid, "flux")], observations[name(skid, "tmp")]
        expected_flux, residual = {}, {}
        for time in sorted(flux.keys() & tmp.keys()):
            fitted = [hour for hour in permeability if time - 2 * HOUR <= hour <= time]
            if fitted:
                expected_flux[time] = permeability[max(fitted)] * tmp[time]
                residual[time] = flux[time] - expected_flux[time]
        for app, port, values in (
            (SMOOTHED, "smoothed", smoothed),
            (DRIFT, "cusum", cusum),
            (DRIFT, "alarm", alarm),
            (PERMEABILITY, "permeability", permeability),
            (PERMEABILITY, "variance", variances),
            (RESIDUAL, "expected", expected_flux),
            (RESIDUAL, "residual", residual),
        ):
            expected[output_reference(app, skid, port)] = values
    return expected


def test_sequential_apps_match_a_full_recomputation_after_late_data(client):
    engine = api.state.manager.core.engine
    register(client)
    deploy(client)
    observations = readings(hours=12)

    def submit(label, streams):
        for stream, rows in streams.items():
            observations.setdefault(stream, {}).update(rows)
        response = client.insert_timeseries_batch(SOURCE, streams, request_id=label)
        client.wait(response["receipts"][0])

    def actual():
        snapshot = engine.snapshot()
        return {
            reference: {r.event_time_ns: r.value for r in snapshot.readings(reference)}
            for reference in recompute(observations)
        }

    def check():
        result, expected = actual(), recompute(observations)
        for reference, values in expected.items():
            assert result[reference].keys() == values.keys(), reference
            for time, value in values.items():
                assert result[reference][time] == pytest.approx(value), (reference, time)
        return result

    submit("initial", {stream: list(rows.items()) for stream, rows in observations.items()})
    initial = check()
    skid_1, skid_2 = SKIDS
    # The fit follows skid 1's fouling of 0.05 L/m2/h/bar per hour, about
    # four hours behind with a forgetting factor of 0.8.
    estimate = initial[output_reference(PERMEABILITY, skid_1, "permeability")]
    hourly = [estimate[h * HOUR] for h in range(12)]
    assert hourly == sorted(hourly, reverse=True)
    true = 3.0 - 0.05 * 11.5
    assert true < hourly[-1] < true + 0.05 * 4.5
    assert initial[output_reference(PERMEABILITY, skid_2, "permeability")][11 * HOUR] == (
        pytest.approx(2.5, abs=0.01)
    )
    # The estimate lags the fouling, so skid 1 delivers less flux than its
    # estimate predicts; skid 2 delivers what it predicts.
    residuals = [initial[output_reference(RESIDUAL, skid, "residual")] for skid in SKIDS]
    assert all(value < 0 for time, value in residuals[0].items() if time >= 2 * HOUR)
    assert all(abs(value) < 0.01 for value in residuals[1].values())
    # Skid 1's conductivity excursion from 08:00 raises an alarm; skid 2 has none.
    assert min(initial[output_reference(DRIFT, skid_1, "alarm")]) > 8 * HOUR
    assert not initial[output_reference(DRIFT, skid_2, "alarm")]

    # A late spike repairs every later smoothed value, and the drift statistic
    # until it returns to zero.
    spike = 2 * HOUR + 150_000_000_000
    submit("late-spike", {"skid-2-conductivity": [(spike, 120.0)]})
    repaired = check()
    cusum = output_reference(DRIFT, skid_2, "cusum")
    assert repaired[cusum][spike] > 0
    assert repaired[cusum][5 * HOUR] == initial[cusum][5 * HOUR] == 0
    # Deleting it restores every earlier result.
    client.wait(client.delete_timeseries(SOURCE, "skid-2-conductivity", [spike], request_id="drop"))
    del observations["skid-2-conductivity"][spike]
    assert check() == initial

    # A late flux reading changes its hour's estimate and every later one.
    submit("late-flux", {"skid-1-flux": [(5 * HOUR + MINUTE, 3.0)]})
    result = check()
    later = result[output_reference(PERMEABILITY, skid_1, "permeability")]
    assert later[4 * HOUR] == estimate[4 * HOUR]
    assert all(later[h * HOUR] != estimate[h * HOUR] for h in range(5, 12))
    # Every reading from 05:00 reads a changed estimate; the late reading has
    # no TMP at its time, so it has no residual of its own.
    after = result[output_reference(RESIDUAL, skid_1, "residual")]
    before = residuals[0]
    assert all(after[t] == before[t] for t in before if t < 5 * HOUR)
    assert all(after[t] != before[t] for t in before if t >= 5 * HOUR)
    assert 5 * HOUR + MINUTE not in after
