100 streams in requests of 1,000 readings from 1 sender, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 9,999 | 9,999 | 33.3 | 45.9 | 76.5 | 83.9 | 10.0 | 1.0 | 24.7 | 8.1 | 14.7 | 0 | 0.00 | sustained |
| 25,000 | 24,995 | 25,051 | 31.7 | 59.4 | 114.6 | 155.6 | 24.8 | 1.0 | 25.2 | 7.8 | 43.5 | 1 | 0.00 | sustained |
| 50,000 | 49,961 | 50,017 | 44.1 | 100.4 | 164.1 | 200.3 | 34.8 | 1.4 | 28.3 | 7.9 | 63.2 | 6 | 0.03 | sustained |
| 100,000 | 70,577 | 72,381 | 66.3 | 125.2 | 203.5 | 240.8 | 21.8 | 3.3 | 45.3 | 9.1 | 66.5 | 9 | 0.02 | admission-limited |
