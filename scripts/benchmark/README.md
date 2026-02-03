# Acquirium Benchmark Scripts

This directory contains scripts for benchmarking Acquirium soft sensor performance.

## Prerequisites

1. Acquirium server running (`docker compose up`)
2. A TTL graph file with `hasExternalReference` data nodes (e.g. `deployments/BENICIA/benicia-model-with-refs-thresholds.ttl`)
3. Python environment with `acquirium` package installed

## Scalability Benchmark

Tests horizontal scaling by running one soft sensor per data node with an external reference in a TTL graph.

### Components

| File | Description |
|------|-------------|
| `scalability.py` | Spawns one warning app per data node with `hasExternalReference` |
| `latency_receiver.py` | HTTP server that receives alerts and logs latency metrics |

### Usage

**Terminal 1 - Start the receiver:**
```bash
python scripts/benchmark/latency_receiver.py results_20sensor.csv [port]

# Commands to run the measurements for the 3 graph sizes. Run these separately:
python scripts/benchmark/latency_receiver.py scripts/benchmark/scalability/scalability_results_1.csv 10000
python scripts/benchmark/latency_receiver.py scripts/benchmark/scalability/scalability_results_10.csv 10000
python scripts/benchmark/latency_receiver.py scripts/benchmark/scalability/scalability_results_100.csv 10000

```

**Terminal 2 - Run the benchmark:**
```bash
python scripts/benchmark/scalability.py <ttl_path> [--timeout SECONDS] [--interval SECONDS] [--threshold VALUE]

# Commands to run the measurements for the 3 graph sizes, Run the corresponding command for each receiver above seperately:
python scripts/benchmark/scalability.py deployments/BENICIA/benicia-model-with-refs-1.ttl
python scripts/benchmark/scalability.py deployments/BENICIA/benicia-model-with-refs-10.ttl
python scripts/benchmark/scalability.py deployments/BENICIA/benicia-model-with-refs-100.ttl

```

**On Linux (Docker host networking):**
```bash
ALERT_HOST=172.17.0.1 python scripts/benchmark/scalability.py deployments/BENICIA/benicia-model-with-refs-1.ttl --timeout 60
```

**Plotting Script:**
```bash
python scripts/benchmark/scalability/visual.py scripts/benchmark/scalability/scalability_results_1.csv scripts/benchmark/scalability/scalability_results_10.csv scripts/benchmark/scalability/scalability_results_100.csv --outdir scripts/benchmark/scalability/plots
```

**Optional args:**
`--server-url`, `--server-port`, `--lexicon-path`

### Output

The receiver logs each message with latency breakdown:
```
[1] external_reference_warning_0: meas→recv=12.34ms | recv→done=5.67ms | done→endpoint=8.90ms | total=26.91ms
```

CSV columns: `msg_id`, `app_id`, `measurement_time`, `time_received`, `time_completed`, `endpoint_receipt`, latencies (ms)

---

## Chain Latency Benchmark

Tests vertical scaling by creating chains of dependent soft sensors where each sensor consumes the previous one's output.

### Chain Structure

```
Benicia Data → Level 0 → Level 1 → ... → Level N → Final (trigger)
```

Each intermediate sensor:
- Reads from the previous sensor's timeseries output
- Increments the value by 1
- Writes to its own timeseries output
- Sends a trigger to the receiver for latency tracking

### Components

| File | Description |
|------|-------------|
| `chain_latency.py` | Creates a chain of N+1 sensors with data dependencies |
| `chain_receiver.py` | HTTP server that tracks per-level processing times |

### Usage

**Terminal 1 - Start the receiver:**
```bash
python scripts/benchmark/chain_receiver.py results_depth5.csv [port]

# Example:
python scripts/benchmark/chain_receiver.py chain_results.csv 10000
```

**Terminal 2 - Run the benchmark:**
```bash
python scripts/benchmark/chain_latency.py <chain_depth> [timeout_seconds]

# Example: chain of depth 5 for 60 seconds
python scripts/benchmark/chain_latency.py 5 60
```

**On Linux:**
```bash
ALERT_HOST=172.17.0.1 python scripts/benchmark/chain_latency.py 5 60
```



### Output

The receiver logs each message with processing time:
```
[1] L0/5 chain_level_0_of_5: processing=2.34ms
[2] L1/5 chain_level_1_of_5: processing=1.23ms
[3] L2/5 chain_level_2_of_5: processing=1.45ms
...
[6] L5/5 [FINAL] chain_final_5_of_5: processing=1.12ms
```

On Ctrl-C, prints summary statistics:
```
--- Per-Level Processing Time ---
  Level 0: count=100, mean=2.34ms, median=2.10ms, min=1.50ms, max=5.20ms
  Level 1: count=100, mean=1.23ms, median=1.15ms, min=0.80ms, max=3.10ms
  ...

--- Estimated Chain Processing Time ---
  Depth 5: ~8.45ms (sum of 6 levels)
```

### Note on Clock Skew

Only `processing_time` (time_completed - time_received) is reliable since both timestamps come from the same Docker container. Cross-machine latencies (container → host) are subject to clock skew and are not reported.

---

## System Info

Generate system specifications for benchmark documentation:

```bash
./scripts/benchmark/system_info.sh > system_spec.txt

# With memory type/speed (requires root):
sudo ./scripts/benchmark/system_info.sh > system_spec.txt
```

---

## Environment Variables

(shouldn't need adjusting)

| Variable | Default | Description |
|----------|---------|-------------|
| `ALERT_HOST` | `host.docker.internal` | Host address for alert receiver |
| `ALERT_PORT` | `10000` | Port for alert receiver |

---

## Example Experiment

```bash
# 1. Start Acquirium
docker compose up -d

# 2. Capture system info
./scripts/benchmark/system_info.sh > results/system_spec.txt

# 3. Run scalability test (one app per external-reference data node)
uv run python scripts/benchmark/latency_receiver.py results/scale.csv &
RECEIVER_PID=$!
sleep 2
uv run python scripts/benchmark/scalability.py deployments/BENICIA/benicia-model-with-refs-thresholds.ttl --timeout 60
kill $RECEIVER_PID

# 4. Run chain depth test (1, 3, 5, 10 levels)
for d in 1 3 5 10; do
  uv run python scripts/benchmark/chain_receiver.py results/chain_${d}.csv &
  RECEIVER_PID=$!
  sleep 2
  uv run python scripts/benchmark/chain_latency.py $d 60
  kill $RECEIVER_PID
done
```
