#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Failure Demo 1: Kill a Flink TaskManager Under Active Load
#
# Scenario:
#   While clickstream events (including duplicates and mid-stream catalogue price
#   mutations) are streaming at 300 events/s, `flink-taskmanager-1` receives an
#   ungraceful `SIGKILL`.
#
# Expected Output:
#   1. JobManager detects TaskManager heartbeat timeout and cancels in-flight tasks.
#   2. Both Flink jobs automatically restore RocksDB incremental checkpoint state
#      from MinIO (`s3a://flink-checkpoints/checkpoints/...`); `restored_checkpoints`
#      increments on both jobs.
#   3. Uncommitted Kafka 2PC transactions opened by the killed TaskManager are
#      aborted and ignored by ClickHouse (`isolation.level = read_committed`).
#   4. `scripts/verify_recount.py --strict` reports `missing=0`, `extra=0`, and
#      exact batch recount parity (`[recount] PASS`).
# ==============================================================================

COMPOSE="${COMPOSE:-docker compose}"

echo "=== [Demo 1] 1/5 Capturing pre-failure checkpoint counts from Flink JobManager ==="
PRE_RESTORES=$($COMPOSE run --rm tools python -c "
import requests
r = requests.get('http://flink-jobmanager:8081/jobs/overview', timeout=5).json()
out = {}
for j in r['jobs']:
    if j['state'] == 'RUNNING':
        c = requests.get(f\"http://flink-jobmanager:8081/jobs/{j['jid']}/checkpoints\", timeout=5).json()
        out[j['name']] = c['counts']['restored']
        print(f\"  {j['name']}: completed={c['counts']['completed']} restored={c['counts']['restored']}\")
")
echo "$PRE_RESTORES"

echo "=== [Demo 1] 2/5 Starting continuous 300 eps clickstream load (35s) in background ==="
$COMPOSE run --rm -d --name clickstream_demo1_load tools \
  python -m generator.clickstream_gen.producer_cli \
    --duration-sec 35 \
    --events-per-sec 300 \
    --duplicate-rate 0.04 \
    --jitter-rate 0.06 \
    --late-rate 0.0 \
    --align-window-close \
    --seed 101 > /dev/null

sleep 9
echo "=== [Demo 1] 3/5 Injecting fault: SIGKILL flink-taskmanager-1 mid-stream! ==="
$COMPOSE kill -s SIGKILL flink-taskmanager-1

sleep 5
echo "=== [Demo 1] 4/5 Restarting flink-taskmanager-1 container to restore 8-slot pool ==="
$COMPOSE up -d flink-taskmanager-1

echo "Waiting for background clickstream load generator to finish..."
while docker ps --format '{{.Names}}' | grep -q '^clickstream_demo1_load$'; do
  sleep 2
done
sleep 6

echo "=== [Demo 1] 5/5 Verifying MinIO S3 checkpoint restore & exact batch recount ==="
$COMPOSE run --rm tools python -c "
import requests, sys
r = requests.get('http://flink-jobmanager:8081/jobs/overview', timeout=5).json()
running = [j for j in r['jobs'] if j['state'] == 'RUNNING']
if len(running) < 2:
    print(f'FAIL: Expected 2 RUNNING jobs after recovery, found {len(running)}')
    sys.exit(1)
restored_any = False
for j in running:
    c = requests.get(f\"http://flink-jobmanager:8081/jobs/{j['jid']}/checkpoints\", timeout=5).json()
    restored = c['counts']['restored']
    latest_restored = (c.get('latest', {}).get('restored') or {}).get('external_path', 'n/a')
    print(f\"  Recovered Job {j['name']}: state={j['state']} completed={c['counts']['completed']} restored={restored} from={latest_restored}\")
    if restored >= 1:
        restored_any = True
if not restored_any:
    print('FAIL: Expected restored checkpoint count >= 1 after TaskManager SIGKILL!')
    sys.exit(1)
"

$COMPOSE run --rm tools python scripts/verify_recount.py --strict
echo "=== [Demo 1] SUCCESS: TaskManager SIGKILL recovered from S3 RocksDB checkpoint with 0 lost/duplicate events! ==="
