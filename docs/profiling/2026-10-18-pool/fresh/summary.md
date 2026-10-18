# Execution profile

Execution: fresh; paired fresh-owner controls; median milliseconds per measured epoch.

| Scenario | Workers | Peak | Requests/epoch | Unprofiled compute | Profiled compute | Delta | Total/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 2 | 2 | 8 | 759.75 | 1011.73 | +33.2% | 95.29 |
| dense500 | 2 | 2 | 1 | 660.99 | 724.38 | +9.6% | 662.70 |
| history128 | 2 | 2 | 1 | 702.83 | 647.90 | -7.8% | 705.73 |
| tiny | 2 | 2 | 1 | 658.45 | 721.13 | +9.5% | 660.48 |
| wide8 | 2 | 2 | 1 | 1341.82 | 1763.11 | +31.4% | 1343.82 |

## Actual worker stage means

Milliseconds per worker invocation, including all groups in that invocation. Parallel worker times are not additive epoch latency.

| Scenario | Workers | Imports | Function setup | Arrow/JSON input | Function | Validation/Arrow output | Wrapper setup | External/unattributed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 2 | 207.04 | 1.10 | 3.94 | 2.04 | 38.19 | 20.18 | 63.02 |
| dense500 | 2 | 152.69 | 0.84 | 1.35 | 1.85 | 22.69 | 15.60 | 51.97 |
| history128 | 2 | 142.27 | 0.76 | 0.97 | 0.56 | 21.97 | 12.74 | 47.06 |
| tiny | 2 | 153.47 | 0.78 | 0.87 | 0.59 | 22.64 | 15.52 | 48.02 |
| wide8 | 2 | 179.08 | 0.97 | 1.02 | 0.87 | 28.10 | 17.07 | 55.80 |

External/unattributed includes Rust grouping/serialization/diff, OS process startup/exit, polling, and uninstrumented/reporting overhead; it is not a pure interpreter-startup measurement.
