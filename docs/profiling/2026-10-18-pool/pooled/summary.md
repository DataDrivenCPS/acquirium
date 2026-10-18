# Execution profile

Execution: pooled; paired fresh-owner controls; median milliseconds per measured epoch.

| Scenario | Workers | Peak | Requests/epoch | Unprofiled compute | Profiled compute | Delta | Total/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 2 | 2 | 8 | 41.57 | 37.46 | -9.9% | 5.73 |
| dense500 | 2 | 2 | 1 | 18.04 | 12.97 | -28.1% | 20.78 |
| history128 | 2 | 2 | 1 | 6.92 | 6.27 | -9.4% | 9.33 |
| tiny | 2 | 2 | 1 | 10.52 | 6.46 | -38.6% | 14.33 |
| wide8 | 2 | 2 | 1 | 11.64 | 32.20 | +176.7% | 13.96 |

## Actual worker stage means

Milliseconds per worker invocation, including all groups in that invocation. Parallel worker times are not additive epoch latency.

| Scenario | Workers | Imports | Function setup | Arrow/JSON input | Function | Validation/Arrow output | Wrapper setup | External/unattributed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 2 | 0.00 | 0.02 | 2.99 | 1.56 | 5.93 | 0.00 | 5.08 |
| dense500 | 2 | 0.00 | 0.02 | 0.28 | 1.78 | 0.52 | 0.00 | 2.71 |
| history128 | 2 | 0.00 | 0.01 | 0.24 | 0.17 | 0.51 | 0.00 | 1.14 |
| tiny | 2 | 0.00 | 0.01 | 0.23 | 0.16 | 0.53 | 0.00 | 1.13 |
| wide8 | 2 | 0.00 | 0.03 | 0.49 | 0.39 | 1.70 | 0.00 | 2.17 |

External/unattributed includes Rust grouping/serialization/diff, OS process startup/exit, polling, and uninstrumented/reporting overhead; it is not a pure interpreter-startup measurement.
