"""Live Kafka 4.4 KRaft producer benchmark: compression codecs (none, lz4, zstd) x linger.ms x batch.size."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from generator.clickstream_gen.avro_wire import ConfluentAvroCodec
from generator.clickstream_gen.session_simulator import ClickstreamSimulator

MATRIX: list[tuple[str, int, int]] = [
    ("none", 0, 16384),
    ("none", 20, 65536),
    ("lz4", 0, 16384),
    ("lz4", 5, 32768),
    ("lz4", 20, 65536),
    ("lz4", 50, 131072),
    ("zstd", 0, 16384),
    ("zstd", 5, 32768),
    ("zstd", 20, 65536),
    ("zstd", 50, 131072),
]


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((p / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def pregenerate_payloads(count: int, schema_path: Path) -> list[tuple[bytes, bytes]]:
    codec = ConfluentAvroCodec.from_schema_file(schema_path, schema_id=1)
    sim = ClickstreamSimulator(user_pool_size=600, duplicate_rate=0.03, late_rate=0.02, seed=2026)
    base_ms = 1762000000000
    items: list[tuple[bytes, bytes]] = []
    for i in range(count):
        ev = sim.next_event(now_ms=base_ms + i * 5)
        items.append((ev["user_id"].encode("utf-8"), codec.encode(ev)))
    return items


def run_single_config(
    bootstrap_servers: str,
    admin: AdminClient,
    payloads: list[tuple[bytes, bytes]],
    codec_name: str,
    linger_ms: int,
    batch_size: int,
) -> dict[str, Any]:
    topic = f"bench.{codec_name}.l{linger_ms}.b{batch_size}.{int(time.time() * 1000) % 100000}"
    fs = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=12,
                replication_factor=3,
                config={"min.insync.replicas": "2", "compression.type": "producer"},
            )
        ]
    )
    for f in fs.values():
        f.result(timeout=15.0)
    time.sleep(0.6)

    latest_stats: dict[str, Any] = {}

    def _stats_cb(stats_json: str) -> None:
        nonlocal latest_stats
        latest_stats = json.loads(stats_json)

    ack_latencies_ms: list[float] = []
    send_times: list[float] = [0.0] * len(payloads)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": codec_name,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
            "queue.buffering.max.messages": 300000,
            "statistics.interval.ms": 100,
            "stats_cb": _stats_cb,
        }
    )

    uncompressed_bytes = sum(len(k) + len(v) for k, v in payloads)

    try:
        t0 = time.perf_counter()
        for idx, (key_b, val_b) in enumerate(payloads):
            # Sample 1-in-10 ACK latencies to avoid callback overhead distorting throughput
            if idx % 10 == 0:
                send_times[idx] = time.perf_counter()

                def _cb(err, _msg, i=idx):
                    if err is None:
                        ack_latencies_ms.append((time.perf_counter() - send_times[i]) * 1000.0)

                cb = _cb
            else:
                cb = None

            while True:
                try:
                    producer.produce(topic=topic, key=key_b, value=val_b, on_delivery=cb)
                    break
                except BufferError:
                    producer.poll(0.01)
            if idx % 2000 == 0:
                producer.poll(0)

        producer.flush(30.0)
        elapsed_s = max(0.001, time.perf_counter() - t0)
        producer.poll(0.15)
    finally:
        try:
            del_fs = admin.delete_topics([topic], operation_timeout=10.0)
            for f in del_fs.values():
                f.result(timeout=10.0)
        except Exception:  # noqa: BLE001
            pass

    # Extract broker wire bytes & batch averages from librdkafka statistics
    broker_tx_bytes = 0
    for b in latest_stats.get("brokers", {}).values():
        if isinstance(b, dict) and b.get("nodeid", -1) >= 0:
            broker_tx_bytes += int(b.get("txbytes", 0))

    topic_stats = latest_stats.get("topics", {}).get(topic, {})
    avg_batch_bytes = float(topic_stats.get("batchsize", {}).get("avg", 0.0))
    avg_batch_cnt = float(topic_stats.get("batchcnt", {}).get("avg", 0.0))

    wire_ratio = round(uncompressed_bytes / max(1, broker_tx_bytes), 2) if broker_tx_bytes > 0 else 1.0
    ack_latencies_ms.sort()

    return {
        "codec": codec_name,
        "linger_ms": linger_ms,
        "batch_size": batch_size,
        "records": len(payloads),
        "elapsed_sec": round(elapsed_s, 3),
        "events_per_sec": round(len(payloads) / elapsed_s, 0),
        "uncompressed_mb_per_sec": round((uncompressed_bytes / (1024 * 1024)) / elapsed_s, 2),
        "uncompressed_kb": round(uncompressed_bytes / 1024.0, 1),
        "broker_tx_kb": round(broker_tx_bytes / 1024.0, 1),
        "compression_ratio": wire_ratio,
        "avg_msgs_per_batch": round(avg_batch_cnt, 1),
        "avg_batch_bytes": round(avg_batch_bytes, 0),
        "ack_p50_ms": round(percentile(ack_latencies_ms, 50), 2),
        "ack_p95_ms": round(percentile(ack_latencies_ms, 95), 2),
        "ack_mean_ms": round(statistics.fmean(ack_latencies_ms), 2) if ack_latencies_ms else 0.0,
    }


def render_markdown(results: list[dict[str, Any]], records_per_run: int) -> str:
    lines = [
        "# Kafka 4.4.0 KRaft Producer Compression & Batching Benchmark",
        "",
        f"- **Cluster**: 3 KRaft combined `broker,controller` nodes (`apache/kafka:4.4.0`), 12 partitions, `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, `enable.idempotence=true`",
        f"- **Payload**: `{records_per_run:,}` Confluent-Avro `ClickstreamEvent` records per run (keyed by `user_id`)",
        "",
        "| Codec | `linger.ms` | `batch.size` (B) | Throughput (events/s) | Uncompressed (MB/s) | Wire TX (KB) | Compression Ratio | Avg Msgs/Batch | ACK p50 (ms) | ACK p95 (ms) |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in results:
        lines.append(
            f"| `{r['codec']}` | {r['linger_ms']} | {r['batch_size']:,} | "
            f"**{int(r['events_per_sec']):,}** | {r['uncompressed_mb_per_sec']:.2f} | "
            f"{r['broker_tx_kb']:,.1f} | **{r['compression_ratio']:.2f}x** | "
            f"{r['avg_msgs_per_batch']:.1f} | {r['ack_p50_ms']:.2f} | {r['ack_p95_ms']:.2f} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Kafka 4.4 compression codecs and batching settings")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument("--records", type=int, default=25000)
    parser.add_argument("--output-dir", default="/workspace/docs/benchmarks")
    args = parser.parse_args()

    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[1] / "schemas"

    out_dir = Path(args.output_dir)
    if not out_dir.exists():
        out_dir = Path(__file__).resolve().parents[1] / "docs" / "benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[benchmark] Pre-generating {args.records:,} Avro-encoded clickstream records...")
    payloads = pregenerate_payloads(args.records, schemas_dir / "clickstream_event.avsc")

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    # Warm up broker connections
    _ = run_single_config(args.bootstrap_servers, admin, payloads[:3000], "lz4", 5, 32768)

    results: list[dict[str, Any]] = []
    for codec_name, linger_ms, batch_size in MATRIX:
        res = run_single_config(
            bootstrap_servers=args.bootstrap_servers,
            admin=admin,
            payloads=payloads,
            codec_name=codec_name,
            linger_ms=linger_ms,
            batch_size=batch_size,
        )
        results.append(res)
        print(
            f"  codec={codec_name:<4} linger={linger_ms:>2}ms batch={batch_size:>6}B -> "
            f"{int(res['events_per_sec']):>7,} eps | wire={res['broker_tx_kb']:>7.1f} KB "
            f"({res['compression_ratio']:.2f}x) | ack_p50={res['ack_p50_ms']:.2f}ms ack_p95={res['ack_p95_ms']:.2f}ms"
        )

    json_path = out_dir / "producer_benchmark.json"
    md_path = out_dir / "producer_benchmark.md"
    json_path.write_text(json.dumps({"records_per_run": args.records, "results": results}, indent=2), encoding="utf-8")
    md_text = render_markdown(results, args.records)
    md_path.write_text(md_text, encoding="utf-8")
    print(f"\n[benchmark] Saved benchmark reports to {json_path} and {md_path}.\n")
    print(md_text)


if __name__ == "__main__":
    main()
