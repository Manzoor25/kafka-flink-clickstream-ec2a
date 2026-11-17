#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Failure Demo 2: Push a 10x Traffic Spike — Backpressure, Lag, and Recovery
#
# Scenario:
#   Baseline traffic runs at 800 events/s (1x), surges 10x to ~8,000+ events/s
#   for 10 seconds, then returns to 1x baseline and drains.
#
# Expected Output:
#   1. Live telemetry table shows producer rate surging 10x, Kafka consumer lag
#      building across `flink-sql-analytics-v1` and `flink-datastream-guard-v1`,
#      and Flink operator `busyRatio` / `backpressureRatio` rising under load.
#   2. After the burst ends, both consumer groups drain back to `lag=0` and
#      backpressure returns to `ok`.
#   3. `scripts/verify_recount.py --strict` confirms 0 dropped or duplicated events.
# ==============================================================================

COMPOSE="${COMPOSE:-docker compose}"

echo "=== [Demo 2] Running 1x -> 10x Traffic Spike -> Drain with live lag/backpressure sampling ==="
$COMPOSE run --rm tools python -m scripts.demos._spike_driver \
  --base-eps "${SPIKE_BASE_EPS:-800}" \
  --spike-multiplier 10 \
  --baseline-sec 5 \
  --spike-sec 10 \
  --cooldown-sec 5

sleep 4
echo "=== [Demo 2] Verifying zero data loss after 10x traffic spike recovery ==="
$COMPOSE run --rm tools python scripts/verify_recount.py --strict
echo "=== [Demo 2] SUCCESS: 10x spike absorbed, lag drained to 0, and batch recount matches! ==="
