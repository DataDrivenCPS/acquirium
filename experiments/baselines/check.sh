#!/usr/bin/env bash
# Reachability and smoke checks for the RQ4 baseline systems.
# Exit status is non-zero if any check fails.
set -u
fail=0
ok()   { printf '  ok    %s\n' "$1"; }
bad()  { printf '  FAIL  %s\n' "$1"; fail=1; }

echo "Flink"
if curl -fsS http://127.0.0.1:8081/overview >/tmp/flink.json 2>/dev/null; then
  ok "REST $(python3 -c 'import json; d = json.load(open("/tmp/flink.json")); print("version", d["flink-version"] + ",", d["taskmanagers"], "taskmanager(s),", d["slots-total"], "slots")')"
else bad "REST API on 8081"; fi
if curl -fsS http://127.0.0.1:8083/v1/info >/tmp/gw.json 2>/dev/null; then
  ok "SQL Gateway $(python3 -c 'import json;print(json.load(open("/tmp/gw.json"))["version"])')"
else bad "SQL Gateway on 8083"; fi

echo "Feldera"
if curl -fsS http://127.0.0.1:8085/healthz >/dev/null 2>&1 || curl -fsS http://127.0.0.1:8085/v0/pipelines >/dev/null 2>&1; then
  ok "REST API on 8085 ($(curl -fsS http://127.0.0.1:8085/v0/config 2>/dev/null | python3 -c 'import json,sys;d=json.load(sys.stdin);print("version", d.get("version","?"))' 2>/dev/null || echo 'pipelines endpoint answers'))"
else bad "REST API on 8085"; fi

echo "TimescaleDB (container siv-timescale, 127.0.0.1:5435)"
if docker exec siv-timescale psql -U acquirium -d acquirium -v ON_ERROR_STOP=1 -qtA <<'SQL' >/tmp/tsdb.txt 2>&1
DROP MATERIALIZED VIEW IF EXISTS siv_check_agg;
DROP TABLE IF EXISTS siv_check CASCADE;
CREATE TABLE siv_check (ts timestamptz NOT NULL, stream text NOT NULL, value double precision);
SELECT create_hypertable('siv_check', 'ts');
INSERT INTO siv_check VALUES (now() - interval '10 min', 'a', 1.0), (now() - interval '5 min', 'a', 3.0);
CREATE MATERIALIZED VIEW siv_check_agg WITH (timescaledb.continuous) AS
  SELECT time_bucket('5 minutes', ts) AS bucket, stream, avg(value) FROM siv_check GROUP BY 1, 2 WITH NO DATA;
CALL refresh_continuous_aggregate('siv_check_agg', NULL, NULL);
SELECT count(*) FROM siv_check_agg;
DROP MATERIALIZED VIEW siv_check_agg;
DROP TABLE siv_check;
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
SQL
then ok "continuous aggregate created, refreshed and dropped (timescaledb $(grep -E '^[0-9]+\.' /tmp/tsdb.txt | tail -1))"
else bad "continuous aggregate smoke test: $(tail -3 /tmp/tsdb.txt | tr '\n' ' ')"; fi

echo "Containers"
docker ps --format '  {{.Names}}  {{.Status}}  {{.Ports}}' | grep -E 'siv-' || bad "no siv- containers running"
exit $fail
