"""Check average turbidity on a timer."""

import pandas as pd

import acquirium as aq


class TurbidityAlarm:
    def initial_state(self):
        # Every check uses the readings in its snapshot, so no state is needed.
        return None

    def on_timer(self, tick: aq.TimerFired) -> aq.Transition:
        # Anchor the 15-minute window to the scheduled tick.
        end = pd.Timestamp(tick.scheduled_at)
        readings = tick.snapshot.read("turbidity", start=end - pd.Timedelta("15m"), end=end)

        average_ntu = readings["value"].mean()  # None if there are no numeric readings.
        if average_ntu is not None and average_ntu > 0.5:
            # Publish a 1 to the alarm stream at the tick time.
            return aq.Transition(None, {"alarm": [(tick.scheduled_at, 1)]})
        return aq.Transition(None)


alarm = aq.Actor(
    name="turbidity-alarm",
    handler=TurbidityAlarm,
    inputs={"turbidity": aq.Stream.from_source("filter", "turbidity")},
    outputs={"alarm": "numeric"},
    timers={"check": pd.Timedelta("4s")},
)
