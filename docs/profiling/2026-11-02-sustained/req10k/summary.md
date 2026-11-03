100 streams in requests of 10,000 readings from 1 sender, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 10,000 | 10,000 | 60.5 | 105.4 | 105.4 | 105.4 | 1.0 | 1.0 | 49.8 | 16.3 | 18.1 | 0 | 0.00 | sustained |
| 25,000 | 25,000 | 25,000 | 63.8 | 107.1 | 175.3 | 175.3 | 2.5 | 1.0 | 55.6 | 17.3 | 19.9 | 0 | 0.00 | sustained |
| 50,000 | 50,000 | 50,000 | 60.1 | 109.0 | 196.2 | 196.2 | 5.0 | 1.0 | 53.6 | 15.7 | 21.0 | 0 | 0.00 | sustained |
| 100,000 | 99,986 | 99,986 | 63.6 | 126.6 | 171.0 | 215.7 | 10.0 | 1.0 | 56.6 | 16.3 | 79.8 | 1 | 0.01 | sustained |
| 150,000 | 149,146 | 149,698 | 82.9 | 196.6 | 258.8 | 320.0 | 13.6 | 1.1 | 65.5 | 17.5 | 139.2 | 2 | 0.00 | sustained |
| 200,000 | 198,869 | 201,631 | 151.0 | 263.9 | 327.7 | 368.4 | 10.6 | 1.9 | 93.4 | 17.9 | 125.0 | 5 | 0.00 | sustained |
| 250,000 | 222,756 | 227,211 | 215.6 | 368.9 | 441.0 | 524.8 | 6.9 | 3.3 | 142.9 | 24.3 | 157.4 | 8 | 0.08 | admission-limited |
