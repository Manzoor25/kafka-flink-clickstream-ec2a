"""Operational telemetry: Kafka consumer group lag, Flink RocksDB checkpoints, and vertex backpressure."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

import requests
from confluent_kafka import Consumer, TopicPartition

MONITORED_GROUPS: list[tuple[str, str]] = [
    ("flink-sql-analytics-v1", "clickstream.raw.v1"),
    ("flink-datastream-guard-v1", "clickstream.raw.v1"),
    ("ch-revenue-per-minute-v1", "analytics.revenue_per_minute.v1"),
    ("ch-freshness-probes-v1", "analytics.freshness_probes.v1"),
]


def measure_group_lag(bootstrap_servers: str, group_id: str, topic: str) -> dict[str, Any]:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic=topic, timeout=5.0)
        t_md = md.topics.get(topic)
        if not t_md:
            return {"group": group_id, "topic": topic, "total_lag": -1}
        parts = [TopicPartition(topic, pid) for pid in sorted(t_md.partitions.keys())]
        committed = c.committed(parts, timeout=5.0)
        total_lag = 0
        total_high = 0
        for tp in committed:
            low, high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=5.0)
            total_high += high
            comm_off = tp.offset if tp.offset is not None and tp.offset >= 0 else low
            total_lag += max(0, high - comm_off)
        return {
            "group": group_id,
            "topic": topic,
            "partitions": len(parts),
            "end_offset_sum": total_high,
            "total_lag": total_lag,
        }
    finally:
        c.close()


def inspect_flink_jobs(flink_url: str) -> list[dict[str, Any]]:
    base = flink_url.rstrip("/")
    overview = requests.get(f"{base}/jobs/overview", timeout=5.0).json()
    jobs_out: list[dict[str, Any]] = []
    for j in overview.get("jobs", []):
        if j.get("state") != "RUNNING":
            continue
        jid = j["jid"]
        detail = requests.get(f"{base}/jobs/{jid}", timeout=5.0).json()
        ckpts = requests.get(f"{base}/jobs/{jid}/checkpoints", timeout=5.0).json()
        latest_ckpt = ckpts.get("latest", {}).get("completed") or {}

        max_busy = 0.0
        max_bp = 0.0
        bp_status = "ok"
        vertex_summaries: list[dict[str, Any]] = []
        for v in detail.get("vertices", []):
            vid = v["id"]
            bp = requests.get(f"{base}/jobs/{jid}/vertices/{vid}/backpressure", timeout=5.0).json()
            subtasks = bp.get("subtasks", [])
            st_levels = [st.get("backpressureLevel", "ok") for st in subtasks]
            level = (
                "high" if "high" in st_levels
                else ("low" if "low" in st_levels else bp.get("backpressureLevel", "ok"))
            )
            v_bp = max((float(st.get("ratio", 0.0)) for st in subtasks), default=0.0)
            v_busy = max((float(st.get("busyRatio", 0.0)) for st in subtasks), default=0.0)
            if v_bp > max_bp:
                max_bp = v_bp
            if v_busy > max_busy:
                max_busy = v_busy
            if level == "high" or (level == "low" and bp_status == "ok"):
                bp_status = level
            vertex_summaries.append(
                {
                    "name": v["name"][:48],
                    "parallelism": v["parallelism"],
                    "level": level,
                    "max_backpressure_ratio": round(v_bp, 3),
                    "max_busy_ratio": round(0.0 if v_busy < 0 else v_busy, 3),
                }
            )

        jobs_out.append(
            {
                "jid": jid,
                "name": j["name"],
                "state": j["state"],
                "completed_checkpoints": ckpts.get("counts", {}).get("completed", 0),
                "restored_checkpoints": ckpts.get("counts", {}).get("restored", 0),
                "latest_checkpoint_size_bytes": latest_ckpt.get("state_size", 0),
                "latest_checkpoint_path": latest_ckpt.get("external_path", ""),
                "backpressure_level": bp_status,
                "max_backpressure_ratio": round(max_bp, 3),
                "max_busy_ratio": round(max_busy, 3),
                "vertices": vertex_summaries,
            }
        )
    return jobs_out


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor Kafka consumer lag and Flink backpressure")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument("--flink-url", default=os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    lags = [measure_group_lag(args.bootstrap_servers, grp, top) for grp, top in MONITORED_GROUPS]
    jobs = inspect_flink_jobs(args.flink_url)
    snapshot = {"timestamp_ms": int(time.time() * 1000), "consumer_lags": lags, "flink_jobs": jobs}

    if args.json:
        print(json.dumps(snapshot))
        return

    print("================ KAFKA CONSUMER GROUP LAG ================")
    for item in lags:
        print(
            f"  {item['group']:<28} topic={item['topic']:<32} "
            f"end_offsets={item.get('end_offset_sum', 0):<8} lag={item['total_lag']}"
        )
    print("\n================ FLINK ROCKSDB & BACKPRESSURE ============")
    for j in jobs:
        print(
            f"  Job {j['name']} ({j['jid'][:8]}): state={j['state']} | "
            f"ckpts={j['completed_checkpoints']} (restored={j['restored_checkpoints']}, "
            f"size={j['latest_checkpoint_size_bytes']} B) | "
            f"bp={j['backpressure_level']} (max_bp={j['max_backpressure_ratio']:.2f}, busy={j['max_busy_ratio']:.2f})"
        )
        for v in j["vertices"]:
            print(
                f"    - {v['name']:<48} p={v['parallelism']} "
                f"bp={v['level']:<4} bp_ratio={v['max_backpressure_ratio']:.2f} busy={v['max_busy_ratio']:.2f}"
            )
    print("==========================================================")


if __name__ == "__main__":
    main()
