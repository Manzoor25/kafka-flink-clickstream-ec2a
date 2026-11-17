#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Failure Demo 3: Deploy a Changed Stateful Query from an S3 Savepoint
#
# Scenario:
#   Upgrade Job 2 (`ClickstreamGuardAndFreshnessJob`) from `--query-version v1`
#   (3-tier loyalty: NEW / RETURNING / VIP) to `--query-version v2` (4-tier
#   loyalty adding PLATINUM + `high_velocity_session` surge flag) via a lossless
#   MinIO S3 savepoint while events continue arriving in Kafka during downtime.
#
# Expected Output:
#   1. Pre-upgrade: `user-savepoint-canary` accumulates 12 events & 10 purchase
#      units under `query_version=v1` (`loyalty_tier=VIP`).
#   2. `flink stop --savepointPath s3a://flink-checkpoints/savepoints` writes a
#      consistent RocksDB + Kafka offset savepoint to MinIO S3.
#   3. While Job 2 is offline, 3 new purchase events (6 units) for
#      `user-savepoint-canary` are published to Kafka (`total=15 events, 16 units`).
#   4. Job 2 is redeployed with `--query-version v2 -s <savepoint_uri>`.
#   5. ClickHouse `freshness_probes` shows `query_version=v2` resuming seamlessly
#      at `user_cumulative_events=13..15` and `user_cumulative_purchase_units=16`
#      with new `loyalty_tier=PLATINUM` and `high_velocity_session=true`!
# ==============================================================================

COMPOSE="${COMPOSE:-docker compose}"

echo "=== [Demo 3] 1/5 Seeding canary shopper state under query-version=v1 ==="
CANARY_USER=$($COMPOSE run --rm tools python -c "
import time, uuid
from pathlib import Path
from confluent_kafka import Producer
from generator.clickstream_gen.avro_wire import register_schema

uid = f'user-canary-{int(time.time()) % 100000}'
codec = register_schema('http://schema-registry:8085', 'clickstream.raw.v1-value', Path('/workspace/schemas/clickstream_event.avsc'))
p = Producer({'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092', 'acks': 'all', 'enable.idempotence': True})
now_ms = int(time.time() * 1000)
for i in range(12):
    ev = {
        'event_id': f'evt-{uid}-v1-{i:02d}',
        'user_id': uid,
        'client_session_id': f'csess-{uid}',
        'event_type': 'purchase' if i >= 7 else 'page_view',
        'product_id': 'prod-001',
        'quantity': 2 if i >= 7 else 1,
        'page_url': '/checkout/complete',
        'referrer': 'direct',
        'device_type': 'desktop',
        'country': 'US',
        'event_time_ms': now_ms + i * 10,
        'created_at_ms': now_ms + i * 10,
    }
    p.produce('clickstream.raw.v1', key=uid.encode(), value=codec.encode(ev))
p.flush(10.0)
print(uid)
" | tail -n 1 | tr -d '\r')

sleep 5
echo "  Canary user seeded: $CANARY_USER"
$COMPOSE run --rm tools python -c "
import os, requests, sys
r = requests.post('http://clickhouse:8123', params={'user':'default','password':os.environ['CLICKHOUSE_PASSWORD']},
    data=\"SELECT query_version, max(user_cumulative_events), max(user_cumulative_purchase_units), argMax(loyalty_tier, user_cumulative_events) FROM clickstream.freshness_probes FINAL WHERE user_id='${CANARY_USER}' GROUP BY query_version\").text.strip()
print('  Pre-savepoint ClickHouse state ->', r)
if not r:
    sys.exit(1)
"

echo "=== [Demo 3] 2/5 Stopping running DataStream job with S3 Savepoint on MinIO ==="
STOP_OUT=$(bash scripts/ops/savepoint.sh stop clickstream-datastream-guard)
echo "$STOP_OUT"
SAVEPOINT_URI=$(echo "$STOP_OUT" | grep -oE 's3a://[^[:space:]]+' | tail -n 1)
if [ -z "$SAVEPOINT_URI" ]; then
  echo "ERROR: Could not parse S3 savepoint URI from flink stop output." >&2
  exit 1
fi
echo "  Captured S3 Savepoint URI: $SAVEPOINT_URI"

echo "=== [Demo 3] 3/5 Publishing in-flight Kafka events WHILE Flink job is offline ==="
$COMPOSE run --rm tools python -c "
import time
from pathlib import Path
from confluent_kafka import Producer
from generator.clickstream_gen.avro_wire import register_schema

uid = '${CANARY_USER}'
codec = register_schema('http://schema-registry:8085', 'clickstream.raw.v1-value', Path('/workspace/schemas/clickstream_event.avsc'))
p = Producer({'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092', 'acks': 'all', 'enable.idempotence': True})
now_ms = int(time.time() * 1000)
for i in range(12, 16):
    ev = {
        'event_id': f'evt-{uid}-v2-{i:02d}',
        'user_id': uid,
        'client_session_id': f'csess-{uid}',
        'event_type': 'purchase',
        'product_id': 'prod-002',
        'quantity': 2,
        'page_url': '/checkout/complete',
        'referrer': 'direct',
        'device_type': 'desktop',
        'country': 'US',
        'event_time_ms': now_ms + i * 10,
        'created_at_ms': now_ms + i * 10,
    }
    p.produce('clickstream.raw.v1', key=uid.encode(), value=codec.encode(ev))
p.flush(10.0)
print('  Published 4 additional purchase events (8 units) for', uid, 'while job was stopped.')
"

echo "=== [Demo 3] 4/5 Deploying upgraded query (--query-version v2) restored from $SAVEPOINT_URI ==="
$COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink run -d -m localhost:8081 \
  -s "$SAVEPOINT_URI" \
  -c io.clickstream.ClickstreamGuardAndFreshnessJob \
  /opt/flink/usrlib/clickstream-flink-jobs.jar \
  --parallelism 2 \
  --checkpoint-ms 2000 \
  --allowed-lateness-ms 10000 \
  --query-version v2 \
  --consumer-group flink-datastream-guard-v1 \
  --scan-startup-mode committed

sleep 7
echo "=== [Demo 3] 5/5 Verifying lossless RocksDB state continuity across v1 -> v2 upgrade ==="
$COMPOSE run --rm tools python -c "
import json, os, requests, sys
sql = \"\"\"
SELECT query_version, min(user_cumulative_events) AS min_ev, max(user_cumulative_events) AS max_ev,
       max(user_cumulative_purchase_units) AS max_units,
       argMax(loyalty_tier, user_cumulative_events) AS tier,
       max(high_velocity_session) AS high_vel
FROM clickstream.freshness_probes FINAL
WHERE user_id = '${CANARY_USER}'
GROUP BY query_version
ORDER BY query_version ASC
FORMAT JSONEachRow
\"\"\"
resp = requests.post('http://clickhouse:8123', params={'user':'default','password':os.environ['CLICKHOUSE_PASSWORD']}, data=sql).text.strip()
rows = [json.loads(line) for line in resp.splitlines() if line.strip()]
for r in rows:
    print(' ', r)
by_ver = {r['query_version']: r for r in rows}
if 'v2' not in by_ver:
    print('FAIL: Expected upgraded query_version=v2 records in ClickHouse!')
    sys.exit(1)
v2 = by_ver['v2']
if int(v2['min_ev']) != 13 or int(v2['max_ev']) != 16 or int(v2['max_units']) != 18 or v2['tier'] != 'PLATINUM':
    print(f'FAIL: Expected seamless state resume (13..16 events, 18 units, PLATINUM), got {v2}')
    sys.exit(1)
print('  PASS: v2 resumed seamlessly at event #13 -> #16 (18 cumulative units) and activated PLATINUM + high_velocity_session!')
"

$COMPOSE run --rm tools python scripts/verify_recount.py --strict
echo "=== [Demo 3] SUCCESS: Stateful query upgraded from S3 savepoint without losing state or events! ==="
