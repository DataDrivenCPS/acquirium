100 streams in requests of 1,000 readings from 1 sender, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Requests/commit | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 9,998 | 9,998 | 32.1 | 40.5 | 72.2 | 75.5 | 10.0 | 1.0 | 23.2 | 8.3 | 12.4 | 1.0 | 0 | 0.00 | sustained |
| 25,000 | 24,997 | 24,997 | 32.0 | 61.0 | 89.1 | 148.3 | 24.8 | 1.0 | 25.3 | 7.4 | 36.2 | 1.0 | 1 | 0.00 | sustained |
| 50,000 | 48,906 | 48,797 | 51.4 | 120.8 | 209.6 | 290.8 | 30.7 | 1.6 | 32.1 | 8.3 | 63.6 | 1.0 | 5 | 0.07 | sustained |
| 100,000 | 78,931 | 81,782 | 69.3 | 135.1 | 197.8 | 257.9 | 21.7 | 3.8 | 45.8 | 8.7 | 61.6 | 1.0 | 12 | 0.00 | admission-limited |
