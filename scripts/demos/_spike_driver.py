"""Driver for Failure Demo 2: pushes 1x baseline -> 10x traffic spike -> 1x recovery while sampling lag & Flink backpressure."""

from __future__ import annotations

import argparse
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from generator.clickstream_gen.avro_wire import register_schema
from generator.clickstream_gen.session_simulator import ClickstreamSimulator
from scripts.ops.monitor_lag_and_backpressure import inspect_flink_jobs, measure_group_lag


def main() -> None:
    parser = argparse.ArgumentParser(description="10x traffic spike & backpressure telemetry driver")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8085"),
    )
    parser.add_argument("--flink-url", default=os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081"))
    parser.add_argument("--base-eps", type=int, default=800)
    parser.add_argument("--spike-multiplier", type=int, default=10)
    parser.add_argument("--baseline-sec", type=float, default=5.0)
    parser.add_argument("--spike-sec", type=float, default=10.0)
    parser.add_argument("--cooldown-sec", type=float, default=6.0)
    args = parser.parse_args()

    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[2] / "schemas"

    codec = register_schema(
        registry_url=args.schema_registry_url,
        subject="clickstream.raw.v1-value",
        schema_path=schemas_dir / "clickstream_event.avsc",
    )

    spike_eps = args.base_eps * args.spike_multiplier
    total_events_needed = int(
        args.base_eps * args.baseline_sec + spike_eps * args.spike_sec + args.base_eps * args.cooldown_sec + 5000
    )
    print(
        f"[spike-demo] Pre-encoding {total_events_needed:,} unique Avro clickstream records "
        f"(base={args.base_eps:,} eps -> 10x spike={spike_eps:,} eps)..."
    )

    sim = ClickstreamSimulator(user_pool_size=600, duplicate_rate=0.02, late_rate=0.0, seed=int(time.time()) % 10000)
    run_prefix = uuid.uuid4().hex[:6]
    preencoded: list[tuple[bytes, int]] = []
    base_now = int(time.time() * 1000)
    for i in range(total_events_needed):
        ev = sim.next_event(now_ms=base_now)
        ev["event_id"] = f"evt-spike-{run_prefix}-{i:06d}"
        preencoded.append((ev["user_id"].encode("utf-8"), i))
    # Store raw dicts to stamp real wall-clock timestamps right as each batch fires
    sim2 = ClickstreamSimulator(user_pool_size=600, duplicate_rate=0.02, late_rate=0.0, seed=int(time.time()) % 10000)
    template_events = [sim2.next_event(now_ms=base_now) for _ in range(2048)]

    samples: list[dict[str, Any]] = []
    stop_sampling = threading.Event()
    current_phase = "1x-baseline"
    sent_counter = 0
    t_start = time.monotonic()

    def _sampler() -> None:
        last_sent = 0
        last_t = time.monotonic()
        while not stop_sampling.is_set():
            try:
                now_t = time.monotonic()
                dt = max(0.001, now_t - last_t)
                cur_sent = sent_counter
                inst_eps = int(round((cur_sent - last_sent) / dt))
                last_sent = cur_sent
                last_t = now_t

                sql_lag = measure_group_lag(args.bootstrap_servers, "flink-sql-analytics-v1", "clickstream.raw.v1")["total_lag"]
                guard_lag = measure_group_lag(args.bootstrap_servers, "flink-datastream-guard-v1", "clickstream.raw.v1")["total_lag"]
                jobs = inspect_flink_jobs(args.flink_url)
                max_busy = max((j["max_busy_ratio"] for j in jobs), default=0.0)
                max_bp = max((j["max_backpressure_ratio"] for j in jobs), default=0.0)
                levels = [j["backpressure_level"] for j in jobs]
                bp_level = "high" if "high" in levels else ("low" if "low" in levels else "ok")

                row = {
                    "t_sec": round(now_t - t_start, 1),
                    "phase": current_phase,
                    "prod_eps": inst_eps,
                    "sql_lag": sql_lag,
                    "guard_lag": guard_lag,
                    "busy_ratio": max_busy,
                    "bp_ratio": max_bp,
                    "bp_level": bp_level,
                }
                samples.append(row)
                print(
                    f"  t={row['t_sec']:>5.1f}s | phase={row['phase']:<12} | prod={row['prod_eps']:>6,} eps | "
                    f"sql_lag={row['sql_lag']:>7,} | guard_lag={row['guard_lag']:>7,} | "
                    f"busy={row['busy_ratio']:.2f} | bp_ratio={row['bp_ratio']:.2f} ({row['bp_level']})"
                )
            except Exception as exc:  # noqa: BLE001
                print(f"  [sampler warning] {exc}")
            stop_sampling.wait(1.2)

    sampler_thread = threading.Thread(target=_sampler, daemon=True)
    sampler_thread.start()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 65536,
            "queue.buffering.max.messages": 400000,
        }
    )

    seq = 0

    def _drive_phase(phase_name: str, duration_s: float, target_eps: int) -> None:
        nonlocal current_phase, sent_counter, seq
        current_phase = phase_name
        p_start = time.monotonic()
        next_tick = p_start
        tick_s = 0.04
        while (time.monotonic() - p_start) < duration_s:
            batch_n = max(1, int(round(target_eps * tick_s)))
            now_ms = int(time.time() * 1000)
            for _ in range(batch_n):
                tmpl = dict(template_events[seq & 2047])
                seq += 1
                tmpl["event_id"] = f"evt-spike-{run_prefix}-{seq:07d}"
                tmpl["event_time_ms"] = now_ms
                tmpl["created_at_ms"] = now_ms
                payload = codec.encode(tmpl)
                while True:
                    try:
                        producer.produce("clickstream.raw.v1", key=tmpl["user_id"].encode("utf-8"), value=payload)
                        break
                    except BufferError:
                        producer.poll(0.01)
                sent_counter += 1
            producer.poll(0)
            next_tick += tick_s
            rem = next_tick - time.monotonic()
            if rem > 0:
                time.sleep(rem)

    print("\n================ LIVE 10x SPIKE TELEMETRY ================")
    _drive_phase("1x-baseline", args.baseline_sec, args.base_eps)
    _drive_phase("10x-SPIKE", args.spike_sec, spike_eps)
    _drive_phase("1x-cooldown", args.cooldown_sec, args.base_eps)
    producer.flush(20.0)

    current_phase = "draining"
    drain_deadline = time.monotonic() + 30.0
    while time.monotonic() < drain_deadline:
        sql_lag = measure_group_lag(args.bootstrap_servers, "flink-sql-analytics-v1", "clickstream.raw.v1")["total_lag"]
        guard_lag = measure_group_lag(args.bootstrap_servers, "flink-datastream-guard-v1", "clickstream.raw.v1")["total_lag"]
        if sql_lag == 0 and guard_lag == 0:
            time.sleep(1.5)
            break
        time.sleep(1.0)

    stop_sampling.set()
    sampler_thread.join(timeout=3.0)

    peak_sql_lag = max((s["sql_lag"] for s in samples), default=0)
    peak_guard_lag = max((s["guard_lag"] for s in samples), default=0)
    peak_busy = max((s["busy_ratio"] for s in samples), default=0.0)
    peak_bp = max((s["bp_ratio"] for s in samples), default=0.0)
    peak_eps = max((s["prod_eps"] for s in samples), default=0)
    final_sql_lag = measure_group_lag(args.bootstrap_servers, "flink-sql-analytics-v1", "clickstream.raw.v1")["total_lag"]
    final_guard_lag = measure_group_lag(args.bootstrap_servers, "flink-datastream-guard-v1", "clickstream.raw.v1")["total_lag"]

    print("==========================================================")
    print(f"  Total spike-run events produced : {sent_counter:,}")
    print(f"  Peak producer rate observed     : {peak_eps:,} events/sec")
    print(f"  Peak Kafka consumer lag (SQL)   : {peak_sql_lag:,} records -> recovered to {final_sql_lag}")
    print(f"  Peak Kafka consumer lag (Guard) : {peak_guard_lag:,} records -> recovered to {final_guard_lag}")
    print(f"  Peak Flink operator busyRatio   : {peak_busy:.2f} (max backpressureRatio={peak_bp:.2f})")
    print("==========================================================\n")


if __name__ == "__main__":
    main()
