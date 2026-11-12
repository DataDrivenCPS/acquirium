"""The benchmark calculation also works as an ordinary Acquirium soft sensor."""

from datetime import datetime, timezone

import polars as pl
import pytest

from acquirium.functions import SCHEMA
from examples.model_workload.model import ROLES, predict
from examples.model_workload.run import scalar_prediction


@pytest.mark.parametrize("missing_role", [False, True])
def test_model_prediction_uses_public_datetime_interface(missing_role):
    at = datetime(2026, 1, 1, 10, 0, 40, 123456, tzinfo=timezone.utc)
    values = [10., 250., 25., 2.5]
    frame = pl.DataFrame({"role": ROLES, "time": [at] * 4, "value": values})
    if missing_role:
        frame = frame.head(3)

    result = predict(frame, {"output_time": at})["prediction"]

    assert result.schema == SCHEMA
    if missing_role:
        assert result.is_empty()
    else:
        assert result["time"].item() == at
        assert result["value"].item() == pytest.approx(scalar_prediction(values))
