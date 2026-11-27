---
title: Apps
---

Declare an app with `aq.App(name=..., inputs=..., outputs=..., function=..., window=...)`.
Acquirium compiles declarations into one Escapement plan. As new readings arrive
or are corrected, Escapement publishes source readings and affected synchronous
results together. Set `freshness="asynchronous"` to calculate in the
[background](deploying.md#background-refresh).

Start with [Build your first soft sensor](../../tutorials/advanced/first-app.md)
or use [Write a soft sensor](../../how-to/write-soft-sensors.md) for common app patterns.
