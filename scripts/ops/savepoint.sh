#!/usr/bin/env bash
set -euo pipefail

# Trigger or stop-with-savepoint a running Flink job onto MinIO S3 (`s3a://flink-checkpoints/savepoints`).
# Usage:
#   bash scripts/ops/savepoint.sh list
#   bash scripts/ops/savepoint.sh trigger <job-name-substring>
#   bash scripts/ops/savepoint.sh stop <job-name-substring>

COMPOSE="${COMPOSE:-docker compose}"
ACTION="${1:-list}"
NAME_FILTER="${2:-clickstream-datastream-guard}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-s3a://flink-checkpoints/savepoints}"

if [ "$ACTION" = "list" ]; then
  $COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink list -m localhost:8081
  exit 0
fi

JID=$($COMPOSE exec -T flink-jobmanager curl -fsS http://localhost:8081/jobs/overview | \
  python3 -c "import json,sys; jobs=json.load(sys.stdin)['jobs']; matches=[j['jid'] for j in jobs if j['state']=='RUNNING' and '${NAME_FILTER}' in j['name']]; print(matches[0] if matches else '')")

if [ -z "$JID" ]; then
  echo "ERROR: No RUNNING Flink job matching '${NAME_FILTER}' found." >&2
  exit 1
fi

if [ "$ACTION" = "trigger" ]; then
  $COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink savepoint -m localhost:8081 "$JID" "$SAVEPOINT_DIR"
elif [ "$ACTION" = "stop" ]; then
  $COMPOSE exec -T flink-jobmanager /opt/flink/bin/flink stop --savepointPath "$SAVEPOINT_DIR" -m localhost:8081 "$JID"
else
  echo "Unknown action: $ACTION (expected list | trigger | stop)" >&2
  exit 1
fi
