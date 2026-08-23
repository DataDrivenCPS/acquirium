# Artifact-backed stateful transformations

A stateful transformation is an ordinary durable materialization whose plans
pin an immutable artifact revision.  The class instance, decoded state, and
worker setup resource are caches only: replacing a process or worker reloads
the same artifact and produces the same result.

```python
import polars as pl
import acquirium as aq
from acquirium.Materialization import TransformContext


class CalibratedTemperature(aq.StatefulTransformation):
    invocation = "per_row"
    outputs = {
        "calibrated": aq.outputs.stream(
            value_kind="numeric",
            prefix="urn:acquirium:derived:calibrated-temperature",
        )
    }

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature")

    def setup_worker(self):
        return load_fast_decoder_library()

    def load_artifact(self, artifact: bytes, worker):
        return decode_calibration(artifact, worker)

    def transform(self, stream, calibration, context: TransformContext):
        values = apply_calibration(stream.values["value"], calibration)
        context.outputs.declare("calibrated", for_input=stream).write(
            pl.DataFrame({"time": stream.values["time"], "value": values})
        )
```

An independent producer creates the bytes.  It can be a calibration job, a
rules compiler, a lookup-table builder, or any other artifact-producing
process; the runtime does not assign model-training semantics to it.

```python
from acquirium.Client.client import AcquiriumClient

client = AcquiriumClient()
client.create_artifact_request({
    "request_id": "calibration:ahu-1:2026-08-21",
    "kind": "calibration",
    "deployment_name": "calibrated-temperature",
    "binding_id": "... durable binding id ...",
    "input_versions": {"urn:ahu-1:temperature": 42},
    "start": "2026-08-21T00:00:00Z",
    "end": "2026-08-22T00:00:00Z",
})

lease = client.lease_artifact_request("calibration-worker-1")
if lease:
    artifact_bytes, metrics = build_calibration(lease)
    candidate = client.complete_artifact_request(
        lease["request_id"], owner=lease["owner"], attempt=lease["attempt"],
        data=artifact_bytes, media_type="application/x-acquirium-calibration",
        metrics=metrics,
    )
    client.promote_state_revision(candidate["revision_id"], policy="prospective")
```

Use `recompute_all` when the promoted artifact changes all retained output, or
`recompute_from` with an ISO-8601 event-time boundary when it changes only the
tail.  The server turns either request into normal durable range plans; each
output receipt records the artifact revision that produced it.
