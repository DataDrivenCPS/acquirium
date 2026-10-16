---
title: Calculate derived streams
---

Declare a native pointwise or fixed-bucket function with semantic input selection.
The [temperature example](../../examples/transformation/README.md) is a complete
working example. The [app reference](../reference/apps.md) explains roles, groups,
output metadata, frozen unit factors, and the function's Arrow/Polars contract.

Escapement owns dependency scheduling and historical repair. Acquirium does not
load custom history windows or schedule transforms separately. Rolling averages,
full-history gap filling, and custom output timestamps need native core extensions.
