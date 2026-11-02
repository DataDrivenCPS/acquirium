100 streams, one reading per stream every 1 s, a native 15-minute mean of each.

| Readings | Derived rows | Load s | Readings/s | Load RSS MiB | Load peak MiB | Bytes/row loaded | DB MiB | Reopen s | Reopen RSS MiB | Bytes/row reopened | 1 h read ms | Stream read ms | Correction ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 1,200 | 4.4 | 229,065 | 279 | 289 | 292 | 39.9 | 0.13 | 101 | 106 | 0.3 | 0.6 | 21.5 |
| 4,000,000 | 4,500 | 16.2 | 246,919 | 746 | 755 | 195 | 73.0 | 0.47 | 297 | 78 | 0.3 | 3.9 | 22.6 |
| 16,000,000 | 17,800 | 67.2 | 238,121 | 1,838 | 1,846 | 120 | 186.1 | 1.78 | 1,058 | 69 | 0.3 | 13.7 | 20.4 |
| 32,000,000 | 35,600 | 135.4 | 236,308 | 2,854 | 2,866 | 93 | 316.8 | 3.38 | 2,053 | 67 | 0.5 | 26.0 | 22.1 |
