# RQ4 baseline systems

Three systems that maintain the same six views over the same replayed data,
so that maintenance cost can be compared at equal freshness. A baseline that
refreshes once an hour trades freshness for CPU and will always look cheap;
the comparison that means something runs every system at the same update
cadence and measures what each one spends.

| System | Role | Endpoint on the experiment host |
|---|---|---|
| Apache Flink 1.20 | stream processor: event-time windows, allowed lateness; corrections need replay | REST 127.0.0.1:8081, SQL Gateway 127.0.0.1:8083 |
| Feldera | incremental view maintenance (DBSP): SQL views over insert/delete streams, corrections are retractions | REST and console 127.0.0.1:8085 |
| TimescaleDB 2.30 | continuous aggregates with a refresh policy at a chosen interval | 127.0.0.1:5435, container `siv-timescale`, user/password/db `acquirium` |

Flink and Feldera come from `compose.yaml`; TimescaleDB is the container the
experiment host already runs. All ports bind to loopback.

```bash
docker compose -f experiments/baselines/compose.yaml up -d
experiments/baselines/check.sh        # reachability + a continuous-aggregate smoke test
docker compose -f experiments/baselines/compose.yaml down   # keeps the Feldera volume
```

## Measurement protocol (to implement next)

1. **Same input.** The replay in `experiments/replay.py` writes each batch to
   the server and, through a sink adapter, to each baseline: Flink and
   Feldera over their HTTP ingress (Feldera `POST /v0/pipelines/<p>/ingress/<table>`
   with insert/delete envelopes; Flink through a Kafka-less source such as a
   JDBC or filesystem table fed by the adapter), TimescaleDB through `COPY`
   into a hypertable `readings(ts, stream, value)`.
2. **Same views.** The six views are expressed as SQL per system:
   5-minute means (`time_bucket` / `TUMBLE`), 1-hour rolling mean (`OVER
   RANGE INTERVAL '1' HOUR`), threshold alarm (filter), hourly alarm count
   (`TUMBLE` over the alarm view), flow total (bucket + sum), daily pH range
   (bucket + max − min). Chaining is view-on-view in Feldera and Flink and a
   continuous aggregate on a continuous aggregate in TimescaleDB.
3. **Same cadence.** Three settings, each applied to every system:
   per-batch (as fast as the system allows), every 5 simulated minutes, and
   every 60 simulated minutes. Incremental views use `batch_delay` for the
   two slower settings; TimescaleDB uses `refresh_continuous_aggregate` on
   the same schedule; Flink and Feldera are continuous by construction and
   are reported at per-batch cadence only.
4. **Same bookkeeping.** CPU seconds per container from cgroup accounting
   (`docker stats` or `/sys/fs/cgroup/.../cpu.stat` sampled before and after
   the replay), rows read and written from each system's own counters
   (Feldera pipeline metrics, Flink job metrics, `pg_stat_user_tables`),
   wall time, and arrival-to-visibility latency by polling the output views
   for a marker row. The incremental runtime's numbers come from its event
   log as today.
5. **Same correctness check.** The oracle in `experiments/oracle.py` compares
   every system's output tables with the from-scratch result before any cost
   is reported, so late and corrected readings are handled, not dropped.
6. **Same graph change.** The sensor removal and view re-parameterization in
   RQ4 are applied to the baselines as a SQL edit plus restart or
   re-backfill; the operator steps and wall time are recorded as their ΔG/ΔF
   cost, since the baselines have no graph.

Sizing on the experiment host (8 vCPU, 29 GB): Flink JobManager 1.6 GB,
TaskManager 4 GB with 8 slots, Feldera default, TimescaleDB default.
