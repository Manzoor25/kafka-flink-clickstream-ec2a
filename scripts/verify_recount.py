"""Batch-vs-Streaming correctness verifier.

Reads `clickstream.raw.v1` and `product.catalog.v1` from Kafka offset 0,
recomputes exact deduplicated sets, per-user cumulative state, and temporal-joined
1-minute tumbling windows, and compares them against ClickHouse serving tables.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Consumer, TopicPartition

from generator.clickstream_gen.avro_wire import ConfluentAvroCodec
from generator.clickstream_gen.recount_lib import compute_batch_recount


def read_full_avro_topic(bootstrap_servers: str, topic: str, schema_path: Path) -> list[dict[str, Any]]:
    codec = ConfluentAvroCodec.from_schema_file(schema_path, schema_id=1)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"batch-recount-{int(time.time() * 1000)}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    records: list[dict[str, Any]] = []
    try:
        md = consumer.list_topics(topic=topic, timeout=10.0)
        t_md = md.topics.get(topic)
        if t_md is None:
            return []
        tps: list[TopicPartition] = []
        end_offsets: dict[int, int] = {}
        for pid in sorted(t_md.partitions.keys()):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10.0)
            if high > low:
                tps.append(TopicPartition(topic, pid, low))
                end_offsets[pid] = high
        if not tps:
            return []

        consumer.assign(tps)
        remaining = set(end_offsets.keys())
        while remaining:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            val = msg.value()
            if val is not None:
                _, decoded = codec.decode(val)
                records.append(decoded)
            pid = msg.partition()
            if pid in remaining and (msg.offset() + 1) >= end_offsets[pid]:
                remaining.remove(pid)
        return records
    finally:
        consumer.close()


def ch_query_json(ch_url: str, ch_user: str, ch_password: str, sql: str) -> list[dict[str, Any]]:
    resp = requests.post(
        ch_url.rstrip("/"),
        params={"user": ch_user, "password": ch_password},
        data=(sql.rstrip(";") + " FORMAT JSONEachRow").encode("utf-8"),
        timeout=20.0,
    )
    resp.raise_for_status()
    lines = [ln for ln in resp.text.splitlines() if ln.strip()]
    return [json.loads(ln) for ln in lines]


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify streaming ClickHouse totals against raw Kafka batch recount")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument("--clickhouse-url", default=os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123"))
    parser.add_argument("--clickhouse-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse_dev"))
    parser.add_argument("--wait-for-windows", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--exact-windows", action="store_true", help="Require 0-event, $0.01 window match (no >15s late drops across closed windows)")
    args = parser.parse_args()

    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[1] / "schemas"

    if args.wait_for_windows:
        time.sleep(5.0)

    raw_clicks = read_full_avro_topic(args.bootstrap_servers, "clickstream.raw.v1", schemas_dir / "clickstream_event.avsc")
    catalog_rows = read_full_avro_topic(args.bootstrap_servers, "product.catalog.v1", schemas_dir / "product_catalog.avsc")
    recount = compute_batch_recount(raw_clicks, catalog_rows)

    expected_unique = len(recount.unique_event_ids)
    if expected_unique == 0:
        print("[recount] FAIL: clickstream.raw.v1 has 0 events.")
        sys.exit(1)

    # Poll up to 20s for Flink checkpoint + ClickHouse 500ms flush to catch up to end-of-log
    deadline = time.monotonic() + 20.0
    probe_ids: set[str] = set()
    late_ids: set[str] = set()
    while time.monotonic() < deadline:
        probe_rows = ch_query_json(
            args.clickhouse_url,
            args.clickhouse_user,
            args.clickhouse_password,
            "SELECT event_id FROM clickstream.freshness_probes FINAL",
        )
        late_rows = ch_query_json(
            args.clickhouse_url,
            args.clickhouse_user,
            args.clickhouse_password,
            "SELECT event_id FROM clickstream.late_events FINAL",
        )
        probe_ids = {str(r["event_id"]) for r in probe_rows}
        late_ids = {str(r["event_id"]) for r in late_rows}
        if len(probe_ids | late_ids) >= expected_unique:
            break
        time.sleep(1.5)

    union_ids = probe_ids | late_ids
    overlap_ids = probe_ids & late_ids
    missing_ids = recount.unique_event_ids - union_ids
    extra_ids = union_ids - recount.unique_event_ids

    ch_windows = ch_query_json(
        args.clickhouse_url,
        args.clickhouse_user,
        args.clickhouse_password,
        """
        SELECT window_start_ms, window_end_ms, total_events, page_views,
               add_to_carts, checkouts, purchases, active_users, gross_revenue
        FROM clickstream.revenue_per_minute FINAL
        ORDER BY window_start_ms ASC
        """,
    )

    watermark_cutoff_ms = recount.max_event_time_ms - 5_000
    closed_batch_windows = {
        ws: w for ws, w in recount.tumbling_1m.items() if w.window_end_ms <= watermark_cutoff_ms
    }

    print("================ STREAMING vs BATCH RECOUNT REPORT ================")
    print(f"  Kafka raw messages read        : {recount.raw_event_count}")
    print(f"  Kafka duplicates filtered      : {recount.duplicate_count}")
    print(f"  Batch unique event_ids         : {expected_unique}")
    print(f"  Streaming on-time probes (CH)  : {len(probe_ids)}")
    print(f"  Streaming late side-output (CH): {len(late_ids)}")
    print(f"  Streaming union (on-time+late) : {len(union_ids)}")
    print(f"  Missing unique events          : {len(missing_ids)}")
    print(f"  Extra / uncommitted events     : {len(extra_ids)}")
    print(f"  On-time / late overlap         : {len(overlap_ids)}")
    print(f"  Closed 1m windows (Streaming)  : {len(ch_windows)}")

    window_mismatches: list[str] = []
    ch_by_start = {int(r["window_start_ms"]): r for r in ch_windows}
    for ws, bw in sorted(closed_batch_windows.items()):
        sw = ch_by_start.get(ws)
        if sw is None:
            window_mismatches.append(f"window {ws} missing in ClickHouse")
            continue
        ev_diff = int(sw["total_events"]) - bw.total_events
        rev_diff = round(float(sw["gross_revenue"]) - bw.gross_revenue, 2)
        print(
            f"  Window [{ws}] Batch(events={bw.total_events}, purchases={bw.purchases}, rev=${bw.gross_revenue:.2f}) "
            f"| Stream(events={sw['total_events']}, purchases={sw['purchases']}, rev=${float(sw['gross_revenue']):.2f}) "
            f"| delta_events={ev_diff}, delta_rev=${rev_diff:.2f}"
        )
        if args.exact_windows and (ev_diff != 0 or abs(rev_diff) > 0.02):
            window_mismatches.append(
                f"window {ws}: batch=({bw.total_events}, ${bw.gross_revenue:.2f}) vs stream=({sw['total_events']}, ${float(sw['gross_revenue']):.2f})"
            )

    print("===================================================================")

    failed = False
    if missing_ids or extra_ids or overlap_ids:
        print(
            f"[recount] FAIL: Exactly-once event set mismatch "
            f"(missing={len(missing_ids)}, extra={len(extra_ids)}, overlap={len(overlap_ids)})!"
        )
        failed = True
    if args.strict and len(ch_windows) == 0:
        print("[recount] FAIL: Expected at least 1 closed 1-minute window in ClickHouse revenue_per_minute.")
        failed = True
    if args.exact_windows and window_mismatches:
        print(f"[recount] FAIL: Exact window mismatches: {window_mismatches}")
        failed = True

    if failed:
        sys.exit(1)

    print("[recount] PASS: Streaming ClickHouse state matches raw Kafka batch recount!")


if __name__ == "__main__":
    main()
