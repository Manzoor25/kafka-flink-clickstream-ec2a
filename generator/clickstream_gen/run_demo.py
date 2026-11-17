"""End-to-end `make demo` runner: streams realistic traffic, summarizes ClickHouse serving tables, and verifies recount."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

import requests

from generator.clickstream_gen.producer_cli import run_stream


def ch_query(ch_url: str, ch_user: str, ch_password: str, sql: str) -> str:
    resp = requests.post(
        ch_url.rstrip("/"),
        params={"user": ch_user, "password": ch_password},
        data=sql.encode("utf-8"),
        timeout=15.0,
    )
    resp.raise_for_status()
    return resp.text.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real-time clickstream platform demo")
    parser.add_argument("--duration-sec", type=float, default=75.0)
    parser.add_argument("--events-per-sec", type=float, default=250.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--spike-multiplier", type=float, default=4.0)
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8085")
    ch_url = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_dev")

    run_stream(
        bootstrap_servers=bootstrap,
        registry_url=registry,
        duration_sec=args.duration_sec,
        events_per_sec=args.events_per_sec,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        spike_multiplier=args.spike_multiplier,
        spike_start_sec=min(22.0, args.duration_sec * 0.35),
        spike_duration_sec=12.0,
        emit_price_changes=True,
        align_window_close=True,
        seed=int(time.time()) % 100_000,
    )

    print("[demo] Waiting 5s for Flink 2PC checkpoint commit + ClickHouse read_committed flush...")
    time.sleep(5.0)

    print("\n================ CLICKHOUSE REAL-TIME SERVING SUMMARY ================")
    tables = [
        ("revenue_per_minute", "SELECT count(), sum(total_events), sum(purchases), round(sum(gross_revenue), 2) FROM clickstream.revenue_per_minute FINAL"),
        ("active_users_hopping", "SELECT count(), max(active_users_5m), round(max(rolling_revenue_5m), 2) FROM clickstream.active_users_hopping FINAL"),
        ("user_sessions", "SELECT count(), round(avg(duration_sec), 1), sum(converted), round(sum(session_revenue), 2) FROM clickstream.user_sessions FINAL"),
        ("funnel_metrics", "SELECT count(), sum(view_users), sum(cart_users), sum(checkout_users), sum(purchase_users) FROM clickstream.funnel_metrics FINAL"),
        ("top_products", "SELECT count(), uniqExact(product_id), round(sum(product_revenue), 2) FROM clickstream.top_products FINAL"),
        ("freshness_probes", "SELECT count(), round(quantileExact(0.50)(e2e_freshness_ms), 0), round(quantileExact(0.95)(e2e_freshness_ms), 0) FROM clickstream.freshness_probes FINAL"),
        ("late_events", "SELECT count(), round(avg(lateness_ms), 0) FROM clickstream.late_events FINAL"),
    ]
    for name, q in tables:
        out = ch_query(ch_url, ch_user, ch_password, q)
        print(f"  {name:<22} -> {out}")
    print("======================================================================\n")

    rc = subprocess.call([sys.executable, "scripts/verify_recount.py", "--strict"])
    if rc != 0:
        sys.exit(rc)


if __name__ == "__main__":
    main()
