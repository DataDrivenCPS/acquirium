100 streams in requests of 10,000 readings from 1 sender, 20 s per rate after 16,000,000 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 50,000 | 50,000 | 50,000 | 68.0 | 111.5 | 194.7 | 194.7 | 5.0 | 1.0 | 59.7 | 14.8 | 44.5 | 0 | 0.00 | sustained |
| 100,000 | 99,991 | 99,991 | 67.2 | 133.8 | 222.3 | 254.3 | 9.9 | 1.0 | 58.6 | 14.9 | 76.9 | 2 | 0.00 | sustained |
| 150,000 | 149,153 | 150,257 | 96.7 | 205.3 | 310.8 | 377.3 | 13.5 | 1.1 | 68.5 | 17.0 | 137.5 | 4 | 0.00 | sustained |
| 200,000 | 199,970 | 199,970 | 161.5 | 274.0 | 374.0 | 430.1 | 9.9 | 2.0 | 99.0 | 17.1 | 129.3 | 5 | 0.11 | sustained |
