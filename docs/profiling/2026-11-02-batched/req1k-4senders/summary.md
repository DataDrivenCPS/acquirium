100 streams in requests of 1,000 readings from 4 senders, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Requests/commit | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 9,999 | 9,999 | 22.4 | 32.6 | 64.6 | 77.8 | 10.0 | 1.0 | 15.4 | 8.0 | 11.7 | 1.0 | 0 | 0.00 | sustained |
| 25,000 | 24,988 | 24,988 | 22.1 | 32.5 | 89.6 | 129.9 | 24.8 | 1.0 | 15.3 | 7.7 | 38.9 | 1.0 | 1 | 0.00 | sustained |
| 50,000 | 49,984 | 49,984 | 34.5 | 86.1 | 149.4 | 189.4 | 44.4 | 1.1 | 20.7 | 9.9 | 106.4 | 1.0 | 3 | 0.01 | sustained |
| 100,000 | 99,407 | 100,069 | 94.2 | 206.7 | 336.5 | 428.6 | 15.1 | 6.6 | 65.6 | 15.7 | 112.7 | 1.4 | 20 | 0.00 | sustained |
| 150,000 | 149,110 | 151,760 | 163.5 | 286.2 | 340.3 | 397.2 | 9.6 | 15.9 | 103.6 | 19.4 | 143.4 | 2.4 | 53 | 0.00 | sustained |
| 200,000 | 143,407 | 148,227 | 187.0 | 395.6 | 463.0 | 566.8 | 8.1 | 18.3 | 121.8 | 19.8 | 137.4 | 2.5 | 76 | 0.04 | admission-limited |
