---
title: Apps
---

Declare an app with `aq.App(name=..., inputs=..., outputs=..., function=..., window=...)`.
Acquirium compiles declarations into one Escapement plan. As new readings arrive
or are corrected, Escapement publishes source readings and affected synchronous
results together. Set `freshness="asynchronous"` to calculate in the
[background](deploying.md#background-refresh).

Start with the [pump power tutorial](../../tutorials/advanced/first-app.md).
