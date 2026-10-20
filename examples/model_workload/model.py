"""Synthetic RBF soft sensor: immutable weights loaded once per worker.

Repeated basis functions intentionally make model memory and inference cost
adjustable. This is a load fixture, not a trained water-quality model.
"""

import numpy as np
import polars as pl

COPIES = 2048
index = np.arange(128)
CENTERS = np.tile(
    np.column_stack(
        (index % 8 / 7, index // 8 % 4 / 3, index // 32 % 4 / 3, index * 37 % 128 / 127)
    ),
    (COPIES, 1),
)
WEIGHTS = np.tile((index % 11 + 1).astype(float), COPIES)
CENTERS.setflags(write=False)
WEIGHTS.setflags(write=False)
WEIGHT_SUM = WEIGHTS.sum()
ROLES = ("temperature", "conductivity", "flow", "pressure")
SCALES = np.array([40.0, 1000.0, 100.0, 10.0])
SCALES.setflags(write=False)
SCHEMA = {
    "event_time_ns": pl.Int64,
    "sample_key": pl.Int64,
    "value": pl.Float64,
    "quality": pl.UInt32,
}


def predict(frame, context):
    values = dict(frame.select("role", "value").iter_rows())
    rows = []
    if all(role in values for role in ROLES):
        point = np.array([values[role] for role in ROLES]) / SCALES
        distance = np.square(CENTERS - point).sum(axis=1)
        prediction = 50.0 + 100.0 * (np.exp(-distance) * WEIGHTS).sum() / WEIGHT_SUM
        rows.append((context["window_ns"], 0, float(prediction), 0))
    return {"prediction": pl.DataFrame(rows, schema=SCHEMA, orient="row")}
