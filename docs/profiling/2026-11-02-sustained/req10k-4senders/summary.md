100 streams in requests of 10,000 readings from 4 senders, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 10,000 | 10,000 | 55.5 | 90.4 | 90.4 | 90.4 | 1.0 | 1.0 | 47.3 | 17.3 | 21.8 | 0 | 0.00 | sustained |
| 25,000 | 25,000 | 25,000 | 54.4 | 89.8 | 169.0 | 169.0 | 2.5 | 1.0 | 47.5 | 17.5 | 19.5 | 0 | 0.00 | sustained |
| 50,000 | 50,000 | 50,000 | 61.9 | 125.6 | 160.6 | 160.6 | 5.0 | 1.0 | 56.7 | 18.4 | 37.2 | 0 | 0.00 | sustained |
| 100,000 | 99,987 | 99,987 | 61.0 | 114.8 | 172.5 | 174.7 | 10.0 | 1.0 | 53.3 | 17.4 | 83.3 | 1 | 0.01 | sustained |
| 150,000 | 149,091 | 149,643 | 84.1 | 372.9 | 578.8 | 624.8 | 12.8 | 1.2 | 66.9 | 22.5 | 175.6 | 3 | 0.00 | sustained |
| 200,000 | 194,526 | 192,905 | 410.7 | 985.8 | 1129.0 | 1260.9 | 5.3 | 3.6 | 179.2 | 60.2 | 340.3 | 16 | 0.16 | sustained |
| 250,000 | 189,045 | 193,667 | 519.5 | 974.2 | 1195.6 | 1368.1 | 3.8 | 5.1 | 248.4 | 92.2 | 325.2 | 20 | 0.18 | sender-limited |
