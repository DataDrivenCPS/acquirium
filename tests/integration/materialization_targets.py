"""Importable application definitions used by live materialization tests."""

from __future__ import annotations

import polars as pl

from acquirium.Materialization.api import Service, Transformation, outputs
from acquirium.Materialization.impact import pointwise
from acquirium.internals.models import compute_ref_uri


SOURCE_ID = "materialization-integration"
INPUT_REF_NAME = "input"
INPUT_POINT_URI = "urn:acquirium:integration:materialization-input"
INPUT_REF_URI = str(compute_ref_uri(SOURCE_ID, INPUT_REF_NAME))
OUTPUT_REF_URI = "urn:acquirium:integration:materialization-output"


class AddOne(Transformation):
    """A deterministic transformation for the app-level happy path."""

    name = "materialization-integration-add-one"
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri=OUTPUT_REF_URI)}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="input", data_source=SOURCE_ID)

    def transform(self, inputs, context):
        values = inputs["input"].values
        context.outputs.declare("output").write(
            values.select("time", (pl.col("value") + 1).alias("value"))
        )


class FailingService(Service):
    """A service whose failure is observable through the service API."""

    name = "materialization-integration-failing-service"

    def on_change(self, change, context) -> None:
        raise RuntimeError("integration service failure")
