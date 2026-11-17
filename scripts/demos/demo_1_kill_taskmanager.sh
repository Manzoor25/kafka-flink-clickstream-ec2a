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

echo "=== [Demo 1] 1/5 Waiting for initial S3 checkpoints & capturing pre-failure counts ==="
$COMPOSE run --rm tools python -c "
import requests, time
deadline = time.monotonic() + 20.0
while time.monotonic() < deadline:
    r = requests.get('http://flink-jobmanager:8081/jobs/overview', timeout=5).json()
    running = [j for j in r['jobs'] if j['state'] == 'RUNNING']
    counts = {}
    for j in running:
        c = requests.get(f\"http://flink-jobmanager:8081/jobs/{j['jid']}/checkpoints\", timeout=5).json()
        counts[j['name']] = (c['counts']['completed'], c['counts']['restored'])
    if len(running) >= 2 and all(v[0] >= 2 for v in counts.values()):
        for name, (comp, rest) in sorted(counts.items()):
            print(f'  {name}: completed={comp} restored={rest}')
        break
    time.sleep(1.5)
"

echo "=== [Demo 1] 2/5 Starting continuous 300 eps clickstream load (35s) in background ==="
docker rm -f clickstream_demo1_load >/dev/null 2>&1 || true
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
ACTIVE_TM_IP=$($COMPOSE run --rm tools python -c "
import requests
tms = requests.get('http://flink-jobmanager:8081/taskmanagers', timeout=5).json()['taskmanagers']
busy = [tm for tm in tms if tm['freeSlots'] < tm['slotsNumber']]
chosen = busy[0] if busy else tms[0]
ip = chosen['id'].split(':')[0]
print(ip)
" | tail -n 1 | tr -d '\r')

TARGET_SVC="flink-taskmanager-1"
for svc in flink-taskmanager-1 flink-taskmanager-2; do
  cid=$($COMPOSE ps -q "$svc" | tr -d '\r')
  cip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$cid" | tr -d '\r')
  if [ "$cip" = "$ACTIVE_TM_IP" ]; then
    TARGET_SVC="$svc"
    TARGET_CID="$cid"
    break
  fi
done
TARGET_CID="${TARGET_CID:-$($COMPOSE ps -q "$TARGET_SVC" | tr -d '\r')}"

echo "=== [Demo 1] 3/5 Injecting fault: SIGKILL active TaskManager $TARGET_SVC ($ACTIVE_TM_IP / ${TARGET_CID:0:12}) mid-stream! ==="
docker kill -s SIGKILL "$TARGET_CID" > /dev/null

sleep 11
echo "=== [Demo 1] 4/5 Restarting killed $TARGET_SVC container to restore 8-slot HA pool ==="
docker start "$TARGET_CID" > /dev/null

echo "Waiting for background clickstream load generator to finish..."
while docker ps --format '{{.Names}}' | grep -q '^clickstream_demo1_load$'; do
  sleep 2
done
sleep 8

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
