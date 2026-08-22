# Persistent services

A service is a durable lifecycle record plus a dedicated, bounded execution
slot. It is useful for dashboards, controllers, and API-side integrations that
need to react to the latest canonical state rather than consume every event.

```python
import os
import acquirium as aq


@aq.service(name="room_dashboard")
class RoomDashboard:
    def on_change(self, change: aq.ChangeHint, context) -> None:
        # This is an Arrow table read from authoritative canonical storage.
        snapshot = context.snapshot(["urn:plant:room-101:temperature"])

        # Push the latest rendered state to a UI gateway. The platform records
        # the intent durably and retries it with this idempotency key.
        context.emit_effect(
            effect_id=f"room-dashboard:{change.token}",
            kind="webhook",
            destination=os.environ["DASHBOARD_GATEWAY_URL"],
            payload={"rows": snapshot.inputs.to_pylist()},
            idempotency_key=f"room-dashboard:{snapshot.token}",
        )


client = aq.Acquirium()
client.register_service(RoomDashboard)
client.start_service("room_dashboard")
```

`ChangeHint` is deliberately coalesced and at-least-once: it says that data or
graph state may have advanced, not that an individual event must be replayed.
`context.snapshot()` returns the latest canonical row of each requested stream,
with the version vector and opaque snapshot token that it observed. Pass
`since=<datetime>` to read every live row at or after that event time instead —
a rolling window or the full retained history — when a service needs more than
the current value. A periodic safety scan compares
acknowledged vectors with stream heads, so a missed in-memory wake-up is
recovered without polling database internals.

Services have no materialized-output commit capability. Derived streams remain
owned by transformations; a service's durable external writes are effect
intents, delivered at least once with an `Idempotency-Key` header.
