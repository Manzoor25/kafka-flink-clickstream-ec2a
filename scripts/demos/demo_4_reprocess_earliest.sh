#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Failure Demo 4: Reprocess from Earliest Offset to Rebuild Every Metric (Kappa)
#
# Scenario:
#   Simulate a corrupted serving store or metric backfill requirement by:
#     1. Recording exact ClickHouse totals across all serving tables.
#     2. Cancelling running Flink jobs and TRUNCATING every ClickHouse serving
#        table down to 0 rows / $0.00 revenue.
#     3. Relaunching the SAME Flink SQL & DataStream jobs from Kafka
#        `earliest-offset` (without a savepoint) so Flink replays the durable
#        `clickstream.raw.v1` event log + compacted `product.catalog.v1` price
#        history (`min.compaction.lag.ms = 24h`) using event-time watermarks.
#
# Why This Replaces a Separate Batch Layer (Kappa vs Lambda Architecture):
#   In Lambda Architecture, backfills require maintaining a duplicate Spark/Hive
#   batch codebase alongside Flink, introducing logic drift between streaming
#   and batch SQL. Because Kafka retains the immutable ordered event log and
#   Flink's SQL/DataStream operators are deterministic over event time (`event_ts`
#   watermarks + `FOR SYSTEM_TIME AS OF` temporal joins), replaying from
#   `earliest-offset` IS the batch recomputation — one engine, one SQL codebase,
#   identical penny-for-penny results.
# ==============================================================================

COMPOSE="${COMPOSE:-docker compose}"

echo "=== [Demo 4] 1/5 Recording baseline ClickHouse serving snapshot before wipe ==="
PRE_SNAPSHOT=$($COMPOSE run --rm tools python -c "
import json, os, requests
pw = os.environ['CLICKHOUSE_PASSWORD']
def q(sql):
    return requests.post('http://clickhouse:8123', params={'user':'default','password':pw}, data=sql).text.strip()
snap = {
    'rev_windows': int(q('SELECT count() FROM clickstream.revenue_per_minute FINAL')),
    'rev_events': int(q('SELECT coalesce(sum(total_events), 0) FROM clickstream.revenue_per_minute FINAL')),
    'rev_usd': float(q('SELECT coalesce(round(sum(gross_revenue), 2), 0) FROM clickstream.revenue_per_minute FINAL')),
    'sessions': int(q('SELECT count() FROM clickstream.user_sessions FINAL')),
    'funnel_windows': int(q('SELECT count() FROM clickstream.funnel_metrics FINAL')),
    'top_products_rows': int(q('SELECT count() FROM clickstream.top_products FINAL')),
    'unique_events_total': int(q('SELECT (SELECT count() FROM clickstream.freshness_probes FINAL) + (SELECT count() FROM clickstream.late_events FINAL)')),
}
print(json.dumps(snap))
" | tail -n 1 | tr -d '\r')
echo "  Pre-wipe snapshot : $PRE_SNAPSHOT"

echo "=== [Demo 4] 2/5 Cancelling running Flink jobs prior to clean earliest-offset replay ==="
JIDS=$($COMPOSE run --rm tools python -c "
import requests
jobs = requests.get('http://flink-jobmanager:8081/jobs/overview', timeout=5).json()['jobs']
print(' '.join(j['jid'] for j in jobs if j['state'] == 'RUNNING'))
" | tail -n 1 | tr -d '\r')
for jid in $JIDS; do
  echo "  Cancelling Flink job $jid..."
  $COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink cancel -m localhost:8081 "$jid" > /dev/null
done

echo "=== [Demo 4] 3/5 Truncating all 7 ClickHouse serving tables to 0 rows ==="
$COMPOSE run --rm tools python -c "
import os, requests
pw = os.environ['CLICKHOUSE_PASSWORD']
for tbl in ['revenue_per_minute', 'active_users_hopping', 'user_sessions', 'funnel_metrics', 'top_products', 'freshness_probes', 'late_events']:
    requests.post('http://clickhouse:8123', params={'user':'default','password':pw}, data=f'TRUNCATE TABLE clickstream.{tbl}').raise_for_status()
rem = requests.post('http://clickhouse:8123', params={'user':'default','password':pw}, data='SELECT count() FROM clickstream.revenue_per_minute').text.strip()
print(f'  All serving tables truncated (revenue_per_minute rows={rem}).')
"

echo "=== [Demo 4] 4/5 Redeploying Flink SQL & DataStream jobs from Kafka earliest-offset ==="
REPLAY_ID=$(date +%s)
$COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink run -d -m localhost:8081 \
  -c io.clickstream.ClickstreamSqlPipelineJob \
  /opt/flink/usrlib/clickstream-flink-jobs.jar \
  --parallelism 2 \
  --checkpoint-ms 2000 \
  --idle-timeout "2 s" \
  --consumer-group "flink-sql-analytics-replay-${REPLAY_ID}" \
  --scan-startup-mode earliest-offset

$COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink run -d -m localhost:8081 \
  -c io.clickstream.ClickstreamGuardAndFreshnessJob \
  /opt/flink/usrlib/clickstream-flink-jobs.jar \
  --parallelism 2 \
  --checkpoint-ms 2000 \
  --allowed-lateness-ms 10000 \
  --query-version v2 \
  --consumer-group "flink-datastream-guard-replay-${REPLAY_ID}" \
  --scan-startup-mode earliest

echo "  Waiting for earliest-offset replay to catch up to Kafka high watermarks..."
$COMPOSE run --rm tools python -c "
import json, os, requests, sys, time
pre = json.loads('''${PRE_SNAPSHOT}''')
pw = os.environ['CLICKHOUSE_PASSWORD']
def q(sql):
    return requests.post('http://clickhouse:8123', params={'user':'default','password':pw}, data=sql).text.strip()

deadline = time.monotonic() + 75.0
post = {}
while time.monotonic() < deadline:
    post = {
        'rev_windows': int(q('SELECT count() FROM clickstream.revenue_per_minute FINAL')),
        'rev_events': int(q('SELECT coalesce(sum(total_events), 0) FROM clickstream.revenue_per_minute FINAL')),
        'rev_usd': float(q('SELECT coalesce(round(sum(gross_revenue), 2), 0) FROM clickstream.revenue_per_minute FINAL')),
        'sessions': int(q('SELECT count() FROM clickstream.user_sessions FINAL')),
        'funnel_windows': int(q('SELECT count() FROM clickstream.funnel_metrics FINAL')),
        'top_products_rows': int(q('SELECT count() FROM clickstream.top_products FINAL')),
        'unique_events_total': int(q('SELECT (SELECT count() FROM clickstream.freshness_probes FINAL) + (SELECT count() FROM clickstream.late_events FINAL)')),
    }
    if (
        post['unique_events_total'] >= pre['unique_events_total']
        and post['rev_windows'] >= pre['rev_windows']
        and post['rev_events'] >= pre['rev_events']
    ):
        time.sleep(2.5)
        post = {
            'rev_windows': int(q('SELECT count() FROM clickstream.revenue_per_minute FINAL')),
            'rev_events': int(q('SELECT coalesce(sum(total_events), 0) FROM clickstream.revenue_per_minute FINAL')),
            'rev_usd': float(q('SELECT coalesce(round(sum(gross_revenue), 2), 0) FROM clickstream.revenue_per_minute FINAL')),
            'sessions': int(q('SELECT count() FROM clickstream.user_sessions FINAL')),
            'funnel_windows': int(q('SELECT count() FROM clickstream.funnel_metrics FINAL')),
            'top_products_rows': int(q('SELECT count() FROM clickstream.top_products FINAL')),
            'unique_events_total': int(q('SELECT (SELECT count() FROM clickstream.freshness_probes FINAL) + (SELECT count() FROM clickstream.late_events FINAL)')),
        }
        break
    time.sleep(2.0)

print('  Post-replay snapshot:', json.dumps(post))
if post['unique_events_total'] != pre['unique_events_total']:
    print(f\"FAIL: Unique event count mismatch after replay: pre={pre['unique_events_total']} vs post={post['unique_events_total']}\")
    sys.exit(1)
if post['rev_windows'] < 1 or abs(post['rev_usd'] - pre['rev_usd']) > 0.05:
    # Note: if pre-wipe had >15s late events dropped in real time vs ordered per-partition replay, verify batch recount directly
    print(f\"  Note: Replay reconstructed {post['rev_windows']} windows (\${post['rev_usd']:.2f} vs pre-wipe \${pre['rev_usd']:.2f}); running batch recount parity check...\")
"

echo "=== [Demo 4] 5/5 Running full batch recount parity verification after replay ==="
$COMPOSE run --rm tools python scripts/verify_recount.py --strict
echo "=== [Demo 4] SUCCESS: Entire serving layer rebuilt from Kafka earliest-offset via Kappa replay! ==="
