"""End-to-end freshness measurement (event creation -> ClickHouse -> Grafana HTTP query) + read_committed isolation proof."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from generator.clickstream_gen.avro_wire import register_schema


def percentile(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return float(s[idx])


def query_grafana_seen_probes(grafana_url: str, probe_prefix: str) -> dict[str, tuple[int, int]]:
    """Queries ClickHouse THROUGH Grafana's /api/ds/query endpoint, just like browser panels."""
    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-serving"},
                "editorType": "sql",
                "queryType": "table",
                "format": 1,
                "rawSql": (
                    f"SELECT event_id, flink_latency_ms, e2e_freshness_ms "
                    f"FROM clickstream.freshness_probes FINAL "
                    f"WHERE startsWith(event_id, '{probe_prefix}')"
                ),
            }
        ],
        "from": "now-15m",
        "to": "now",
    }
    resp = requests.post(
        f"{grafana_url.rstrip('/')}/api/ds/query",
        json=payload,
        timeout=10.0,
    )
    resp.raise_for_status()
    data = resp.json()
    frames = data.get("results", {}).get("A", {}).get("frames", [])
    if not frames:
        return {}
    values = frames[0].get("data", {}).get("values", [])
    if len(values) < 3:
        return {}
    event_ids, flink_lats, ch_lats = values[0], values[1], values[2]
    return {
        str(eid): (int(fl), int(cl))
        for eid, fl, cl in zip(event_ids, flink_lats, ch_lats)
    }


def verify_read_committed_isolation(bootstrap_servers: str) -> dict[str, int]:
    """Demonstrates why downstream consumers of Flink 2PC sinks MUST set isolation.level=read_committed."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topic = f"demo.txn.isolation.{int(time.time() * 1000) % 100000}"
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=3, config={"min.insync.replicas": "2"})])
    for f in fs.values():
        f.result(timeout=15.0)
    time.sleep(0.5)

    try:
        # 1. Commit 3 valid transactional records
        committed_prod = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "transactional.id": f"txn-committed-{uuid.uuid4().hex[:8]}",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        committed_prod.init_transactions(15.0)
        committed_prod.begin_transaction()
        for i in range(3):
            committed_prod.produce(topic, key=f"k-{i}".encode(), value=f"committed-{i}".encode())
        committed_prod.commit_transaction(15.0)

        # 2. Write 5 uncommitted records and ABORT the transaction (simulating TaskManager crash pre-commit)
        aborted_prod = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "transactional.id": f"txn-aborted-{uuid.uuid4().hex[:8]}",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        aborted_prod.init_transactions(15.0)
        aborted_prod.begin_transaction()
        for i in range(5):
            aborted_prod.produce(topic, key=f"phantom-{i}".encode(), value=f"aborted-phantom-{i}".encode())
        aborted_prod.flush(10.0)
        aborted_prod.abort_transaction(15.0)

        # 3. Commit 2 more valid records after the aborted transaction
        committed_prod.begin_transaction()
        for i in range(3, 5):
            committed_prod.produce(topic, key=f"k-{i}".encode(), value=f"committed-{i}".encode())
        committed_prod.commit_transaction(15.0)

        counts: dict[str, int] = {}
        for level in ("read_uncommitted", "read_committed"):
            c = Consumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": f"iso-check-{level}-{uuid.uuid4().hex[:6]}",
                    "isolation.level": level,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            try:
                c.assign([TopicPartition(topic, 0, 0)])
                seen = 0
                empty_polls = 0
                while empty_polls < 6:
                    msg = c.poll(0.35)
                    if msg is None:
                        empty_polls += 1
                        continue
                    if msg.error():
                        continue
                    seen += 1
                counts[level] = seen
            finally:
                c.close()

        return counts
    finally:
        try:
            for f in admin.delete_topics([topic]).values():
                f.result(timeout=10.0)
        except Exception:  # noqa: BLE001
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure end-to-end event creation -> Grafana dashboard freshness")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8085"),
    )
    parser.add_argument("--grafana-url", default=os.environ.get("GRAFANA_URL", "http://grafana:3000"))
    parser.add_argument("--probe-count", type=int, default=200)
    parser.add_argument("--interval-ms", type=int, default=45)
    parser.add_argument("--max-p95-ms", type=float, default=8000.0)
    parser.add_argument("--test-aborted-txn", action="store_true")
    parser.add_argument("--output-file", default="/workspace/docs/benchmarks/freshness_report.json")
    args = parser.parse_args()

    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[1] / "schemas"

    click_codec = register_schema(
        registry_url=args.schema_registry_url,
        subject="clickstream.raw.v1-value",
        schema_path=schemas_dir / "clickstream_event.avsc",
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
        }
    )

    run_tag = uuid.uuid4().hex[:8]
    prefix = f"evt-probe-{run_tag}-"
    created_map: dict[str, int] = {}
    dashboard_visible_ms: dict[str, int] = {}
    stage_latencies: dict[str, tuple[int, int]] = {}

    print(
        f"[freshness] Emitting {args.probe_count} timestamped probes ({prefix}*) "
        f"while polling Grafana /api/ds/query every 150ms..."
    )

    last_poll = 0.0
    for i in range(args.probe_count):
        now_ms = time.time_ns() // 1_000_000
        eid = f"{prefix}{i:04d}"
        ev = {
            "event_id": eid,
            "user_id": f"user-{(i % 200) + 1:04d}",
            "client_session_id": f"csess-probe-{run_tag}",
            "event_type": "purchase" if i % 10 == 0 else "page_view",
            "product_id": f"prod-{(i % 24) + 1:03d}",
            "quantity": 1,
            "page_url": "/products/prod-001",
            "referrer": "direct",
            "device_type": "mobile" if i % 2 == 0 else "desktop",
            "country": "US",
            "event_time_ms": now_ms,
            "created_at_ms": now_ms,
        }
        created_map[eid] = now_ms
        producer.produce(topic="clickstream.raw.v1", key=ev["user_id"].encode("utf-8"), value=click_codec.encode(ev))
        producer.poll(0)

        if time.monotonic() - last_poll >= 0.15:
            seen = query_grafana_seen_probes(args.grafana_url, prefix)
            vis_ms = time.time_ns() // 1_000_000
            for k, lats in seen.items():
                if k not in dashboard_visible_ms:
                    dashboard_visible_ms[k] = vis_ms
                    stage_latencies[k] = lats
            last_poll = time.monotonic()

        time.sleep(args.interval_ms / 1000.0)

    producer.flush(10.0)

    deadline = time.monotonic() + 20.0
    while len(dashboard_visible_ms) < args.probe_count and time.monotonic() < deadline:
        seen = query_grafana_seen_probes(args.grafana_url, prefix)
        vis_ms = time.time_ns() // 1_000_000
        for k, lats in seen.items():
            if k not in dashboard_visible_ms:
                dashboard_visible_ms[k] = vis_ms
                stage_latencies[k] = lats
        if len(dashboard_visible_ms) < args.probe_count:
            time.sleep(0.15)

    if len(dashboard_visible_ms) < args.probe_count:
        print(
            f"[freshness] FAIL: Only {len(dashboard_visible_ms)}/{args.probe_count} probes reached Grafana within timeout."
        )
        sys.exit(1)

    flink_lats = [float(stage_latencies[k][0]) for k in created_map]
    ch_lats = [float(stage_latencies[k][1]) for k in created_map]
    dash_lats = [float(max(0, dashboard_visible_ms[k] - created_map[k])) for k in created_map]

    isolation_result: dict[str, int] | None = None
    if args.test_aborted_txn:
        isolation_result = verify_read_committed_isolation(args.bootstrap_servers)

    report = {
        "probe_count": args.probe_count,
        "flink_processing_ms": {
            "p50": round(percentile(flink_lats, 50), 1),
            "p95": round(percentile(flink_lats, 95), 1),
            "p99": round(percentile(flink_lats, 99), 1),
        },
        "clickhouse_serving_freshness_ms": {
            "p50": round(percentile(ch_lats, 50), 1),
            "p95": round(percentile(ch_lats, 95), 1),
            "p99": round(percentile(ch_lats, 99), 1),
        },
        "grafana_dashboard_e2e_freshness_ms": {
            "min": round(min(dash_lats), 1),
            "p50": round(percentile(dash_lats, 50), 1),
            "p95": round(percentile(dash_lats, 95), 1),
            "p99": round(percentile(dash_lats, 99), 1),
            "max": round(max(dash_lats), 1),
        },
        "kafka_isolation_demo": isolation_result,
    }

    out_path = Path(args.output_file)
    if not out_path.parent.exists():
        out_path = Path(__file__).resolve().parents[1] / "docs" / "benchmarks" / "freshness_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n================ END-TO-END FRESHNESS REPORT ================")
    print(f"  Probes verified via Grafana API : {args.probe_count}/{args.probe_count}")
    print(
        f"  1. Producer -> Flink Operator   : "
        f"p50={report['flink_processing_ms']['p50']} ms | "
        f"p95={report['flink_processing_ms']['p95']} ms | "
        f"p99={report['flink_processing_ms']['p99']} ms"
    )
    print(
        f"  2. Producer -> 2PC -> ClickHouse: "
        f"p50={report['clickhouse_serving_freshness_ms']['p50']} ms | "
        f"p95={report['clickhouse_serving_freshness_ms']['p95']} ms | "
        f"p99={report['clickhouse_serving_freshness_ms']['p99']} ms"
    )
    print(
        f"  3. Producer -> Grafana Query UI : "
        f"p50={report['grafana_dashboard_e2e_freshness_ms']['p50']} ms | "
        f"p95={report['grafana_dashboard_e2e_freshness_ms']['p95']} ms | "
        f"p99={report['grafana_dashboard_e2e_freshness_ms']['p99']} ms"
    )
    if isolation_result is not None:
        print("-------------------------------------------------------------")
        print("  Kafka Transactional Isolation Proof (5 committed + 5 aborted):")
        print(f"    isolation.level=read_uncommitted observed : {isolation_result['read_uncommitted']} records (leaked 5 aborted!)")
        print(f"    isolation.level=read_committed   observed : {isolation_result['read_committed']} records (0 uncommitted leaked!)")
        if isolation_result["read_committed"] != 5 or isolation_result["read_uncommitted"] != 10:
            print("[freshness] FAIL: Unexpected Kafka transactional isolation counts!")
            sys.exit(1)
    print("=============================================================\n")

    if report["grafana_dashboard_e2e_freshness_ms"]["p95"] > args.max_p95_ms:
        print(
            f"[freshness] FAIL: p95 dashboard freshness {report['grafana_dashboard_e2e_freshness_ms']['p95']}ms "
            f"exceeded SLA {args.max_p95_ms}ms."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
