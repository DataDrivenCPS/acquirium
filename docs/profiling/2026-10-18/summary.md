# Execution profile

Paired fresh-process controls; median milliseconds per measured epoch.

| Scenario | Workers | Peak | Requests/epoch | Unprofiled compute | Profiled compute | Delta | Total/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 1 | 1 | 8 | 748.51 | 802.72 | +7.2% | 93.81 |
| batch8 | 2 | 2 | 8 | 577.29 | 607.13 | +5.2% | 72.41 |
| batch8 | 4 | 2 | 8 | 583.75 | 750.10 | +28.5% | 73.23 |
| dense500 | 1 | 1 | 1 | 811.69 | 777.57 | -4.2% | 813.74 |
| dense500 | 2 | 2 | 1 | 615.50 | 578.66 | -6.0% | 618.75 |
| dense500 | 4 | 2 | 1 | 603.41 | 585.38 | -3.0% | 605.63 |
| history128 | 1 | 1 | 1 | 739.17 | 738.93 | -0.0% | 741.14 |
| history128 | 2 | 2 | 1 | 549.13 | 558.76 | +1.8% | 551.08 |
| history128 | 4 | 2 | 1 | 673.91 | 576.56 | -14.4% | 676.55 |
| tiny | 1 | 1 | 1 | 733.74 | 767.58 | +4.6% | 736.15 |
| tiny | 2 | 2 | 1 | 555.92 | 580.58 | +4.4% | 559.41 |
| tiny | 4 | 2 | 1 | 559.92 | 629.62 | +12.4% | 562.17 |
| wide8 | 1 | 1 | 1 | 1823.04 | 1980.30 | +8.6% | 1825.53 |
| wide8 | 2 | 2 | 1 | 1179.71 | 1177.83 | -0.2% | 1181.73 |
| wide8 | 4 | 4 | 1 | 824.98 | 849.26 | +2.9% | 827.71 |

## Actual worker stage means

Milliseconds per worker invocation, including all groups in that invocation. Parallel worker times are not additive epoch latency.

| Scenario | Workers | Imports | Function setup | Arrow/JSON input | Function | Validation/Arrow output | Wrapper setup | External/unattributed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch8 | 1 | 124.98 | 0.72 | 2.31 | 1.31 | 21.82 | 10.92 | 40.74 |
| batch8 | 2 | 125.41 | 0.72 | 2.25 | 1.29 | 21.74 | 10.62 | 40.89 |
| batch8 | 4 | 146.13 | 0.79 | 3.24 | 1.77 | 26.68 | 13.06 | 50.07 |
| dense500 | 1 | 120.34 | 0.69 | 0.74 | 1.67 | 18.80 | 10.96 | 37.43 |
| dense500 | 2 | 126.20 | 0.70 | 0.79 | 1.74 | 18.84 | 10.83 | 40.54 |
| dense500 | 4 | 120.73 | 0.68 | 0.77 | 1.62 | 18.66 | 11.00 | 41.76 |
| history128 | 1 | 119.89 | 0.66 | 0.75 | 0.45 | 19.12 | 10.76 | 36.59 |
| history128 | 2 | 123.33 | 0.67 | 0.71 | 0.49 | 18.70 | 10.85 | 39.73 |
| history128 | 4 | 123.02 | 0.69 | 0.87 | 0.48 | 19.71 | 10.73 | 39.27 |
| tiny | 1 | 123.69 | 0.67 | 0.71 | 0.45 | 18.89 | 11.13 | 37.70 |
| tiny | 2 | 128.35 | 0.68 | 0.69 | 0.44 | 19.75 | 12.05 | 44.97 |
| tiny | 4 | 141.81 | 0.79 | 0.90 | 0.59 | 21.45 | 12.31 | 47.02 |
| wide8 | 1 | 124.35 | 0.68 | 0.77 | 0.60 | 19.14 | 11.32 | 38.76 |
| wide8 | 2 | 125.27 | 0.68 | 0.76 | 0.63 | 18.87 | 11.63 | 41.98 |
| wide8 | 4 | 132.77 | 0.79 | 0.96 | 0.73 | 21.88 | 11.51 | 51.33 |

External/unattributed includes Rust grouping/serialization/diff, OS process startup/exit, polling, and uninstrumented/reporting overhead; it is not a pure interpreter-startup measurement.
