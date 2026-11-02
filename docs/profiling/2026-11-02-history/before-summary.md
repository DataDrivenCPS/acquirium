100 streams, one reading per stream every 1 s, a native 15-minute mean of each.

| Readings | Derived rows | Load s | Readings/s | Load RSS MiB | Load peak MiB | Bytes/row loaded | DB MiB | Reopen s | Reopen RSS MiB | Bytes/row reopened | 1 h read ms | Stream read ms | Correction ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 1,200 | 4.2 | 240,158 | 349 | 348 | 366 | 41.9 | 0.13 | 103 | 108 | 0.3 | 0.8 | 21.3 |
| 4,000,000 | 4,500 | 15.8 | 253,328 | 971 | 970 | 254 | 82.8 | 0.45 | 295 | 77 | 0.3 | 3.9 | 22.6 |
| 16,000,000 | 17,800 | 68.0 | 235,289 | 2,604 | 2,603 | 170 | 184.1 | 1.65 | 1,057 | 69 | 0.3 | 12.8 | 21.4 |
| 32,000,000 | 35,600 | 139.8 | 228,953 | 4,547 | 4,545 | 149 | 338.9 | 3.41 | 2,061 | 67 | 0.4 | 25.9 | 23.6 |
