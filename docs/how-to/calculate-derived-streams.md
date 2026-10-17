---
title: Calculate derived streams
---

Declare a native pointwise or fixed-bucket function with semantic input selection.
The [temperature example](../../examples/transformation/README.md) is a complete
working example. For a complete semantic DAG and Python subprocess execution,
run the [tank-temperature chain](../../examples/soft_sensors/README.md) and follow
[write soft sensors](write-soft-sensors.md). The [app reference](../reference/apps.md) explains roles, groups,
output metadata, frozen unit factors, and the function's Arrow/Polars contract.

Escapement owns dependency scheduling and historical repair. Acquirium does not
load custom history windows or schedule transforms separately. Rolling averages,
full-history gap filling, and custom output timestamps need native core extensions.
