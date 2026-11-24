"""Independent scalar oracle for the synthetic membrane model."""

import math

SCALES = (40.0, 1000.0, 100.0, 10.0)


def scalar_prediction(values):
    """Independent scalar oracle: no worker imports, NumPy, or repeated arrays."""
    point = [value / scale for value, scale in zip(values, SCALES, strict=True)]
    terms = []
    weights = []
    for index in range(128):
        center = (
            index % 8 / 7,
            index // 8 % 4 / 3,
            index // 32 % 4 / 3,
            index * 37 % 128 / 127,
        )
        weight = index % 11 + 1
        terms.append(
            weight
            * math.exp(-sum((x - y) ** 2 for x, y in zip(point, center, strict=True)))
        )
        weights.append(weight)
    return 50.0 + 100.0 * math.fsum(terms) / sum(weights)
