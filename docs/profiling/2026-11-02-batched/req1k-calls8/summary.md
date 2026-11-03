100 streams in requests of 1,000 readings, 8 to a call of `ingest_many`, from 1 sender, 20 s per rate after 0 readings of history, a native 15-minute mean of each stream. Latency is from submission to publication.

| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s | Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Requests/commit | Peak pending | Drain s | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 9,778 | 9,778 | 63.4 | 100.0 | 116.5 | 116.5 | 1.2 | 8.0 | 53.1 | 16.4 | 27.2 | 8.0 | 0 | 0.00 | sustained |
| 25,000 | 24,889 | 24,889 | 58.6 | 129.6 | 176.7 | 176.7 | 3.1 | 8.0 | 49.5 | 15.5 | 26.9 | 8.0 | 8 | 0.00 | sustained |
| 50,000 | 49,778 | 49,778 | 61.7 | 96.9 | 164.0 | 180.8 | 6.2 | 8.0 | 49.2 | 15.5 | 30.3 | 8.0 | 8 | 0.00 | sustained |
| 100,000 | 99,987 | 99,987 | 64.0 | 134.6 | 154.6 | 159.6 | 12.5 | 8.0 | 53.2 | 15.8 | 88.3 | 8.0 | 16 | 0.00 | sustained |
| 150,000 | 149,366 | 151,576 | 97.8 | 191.5 | 227.7 | 246.0 | 15.2 | 10.0 | 65.1 | 17.3 | 114.1 | 8.0 | 40 | 0.00 | sustained |
| 200,000 | 199,971 | 200,416 | 213.7 | 347.2 | 403.9 | 467.1 | 7.3 | 27.3 | 134.1 | 22.4 | 148.0 | 8.0 | 80 | 0.09 | sustained |
| 250,000 | 227,029 | 229,452 | 270.6 | 401.8 | 533.0 | 650.2 | 5.9 | 38.8 | 165.8 | 17.2 | 140.9 | 8.0 | 80 | 0.14 | admission-limited |
