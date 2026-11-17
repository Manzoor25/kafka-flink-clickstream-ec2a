"""Configurable Kafka Avro clickstream load & fault-injection producer CLI."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer

from generator.clickstream_gen.avro_wire import register_schema
from generator.clickstream_gen.catalog import publish_price_updates
from generator.clickstream_gen.session_simulator import ClickstreamSimulator


def build_producer(
    bootstrap_servers: str,
    compression_type: str = "lz4",
    linger_ms: int = 20,
    batch_size: int = 65536,
) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": compression_type,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
            "queue.buffering.max.messages": 250000,
        }
    )


def run_stream(
    bootstrap_servers: str,
    registry_url: str,
    duration_sec: float,
    events_per_sec: float,
    duplicate_rate: float = 0.03,
    jitter_rate: float = 0.06,
    late_rate: float = 0.02,
    spike_multiplier: float = 1.0,
    spike_start_sec: float = 20.0,
    spike_duration_sec: float = 12.0,
    emit_price_changes: bool = True,
    align_window_close: bool = False,
    seed: int | None = 42,
) -> dict[str, object]:
    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[2] / "schemas"

    click_codec = register_schema(
        registry_url=registry_url,
        subject="clickstream.raw.v1-value",
        schema_path=schemas_dir / "clickstream_event.avsc",
    )
    catalog_codec = register_schema(
        registry_url=registry_url,
        subject="product.catalog.v1-value",
        schema_path=schemas_dir / "product_catalog.avsc",
    )

    producer = build_producer(bootstrap_servers=bootstrap_servers)
    sim = ClickstreamSimulator(
        user_pool_size=600,
        duplicate_rate=duplicate_rate,
        within_watermark_jitter_rate=jitter_rate,
        late_rate=late_rate,
        seed=seed,
    )

    delivery_errors: list[str] = []

    def _on_delivery(err, _msg):
        if err is not None and len(delivery_errors) < 10:
            delivery_errors.append(str(err))

    start_mono = time.monotonic()
    flash_sale_1_done = False
    flash_sale_2_done = False
    price_updates_emitted = 0

    print(
        f"[generator] Streaming to clickstream.raw.v1 for {duration_sec:.1f}s "
        f"(base={events_per_sec:.0f} eps, spike={spike_multiplier:.1f}x, "
        f"dup={duplicate_rate*100:.1f}%, late={late_rate*100:.1f}%)..."
    )

    next_Slice = start_mono
    slice_sec = 0.05  # 50 ms pacing ticks

    while True:
        elapsed = time.monotonic() - start_mono
        if elapsed >= duration_sec:
            break

        # Mid-stream temporal catalogue price updates (flash sale & recovery)
        if emit_price_changes and not flash_sale_1_done and elapsed >= min(12.0, duration_sec * 0.25):
            now_ms = int(time.time() * 1000)
            updated = publish_price_updates(
                producer=producer,
                codec=catalog_codec,
                discount_multiplier=0.80,
                product_ids=["prod-001", "prod-002", "prod-005", "prod-010"],
                updated_at_ms=now_ms,
            )
            price_updates_emitted += len(updated)
            flash_sale_1_done = True
            print(f"[generator] Published 20% flash-sale price drop on 4 SKUs at t+{elapsed:.1f}s.")

        if emit_price_changes and not flash_sale_2_done and elapsed >= min(38.0, duration_sec * 0.65):
            now_ms = int(time.time() * 1000)
            updated = publish_price_updates(
                producer=producer,
                codec=catalog_codec,
                discount_multiplier=1.10,
                product_ids=["prod-001", "prod-004"],
                updated_at_ms=now_ms,
            )
            price_updates_emitted += len(updated)
            flash_sale_2_done = True
            print(f"[generator] Published +10% demand-surge price update on 2 SKUs at t+{elapsed:.1f}s.")

        in_spike = (
            spike_multiplier > 1.0
            and spike_start_sec <= elapsed < (spike_start_sec + spike_duration_sec)
        )
        effective_eps = events_per_sec * (spike_multiplier if in_spike else 1.0)
        batch_target = max(1, int(round(effective_eps * slice_sec)))

        now_ms = int(time.time() * 1000)
        for _ in range(batch_target):
            ev = sim.next_event(now_ms=now_ms)
            while True:
                try:
                    producer.produce(
                        topic="clickstream.raw.v1",
                        key=ev["user_id"].encode("utf-8"),
                        value=click_codec.encode(ev),
                        on_delivery=_on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.05)

        producer.poll(0)
        next_Slice += slice_sec
        sleep_for = next_Slice - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

    if align_window_close:
        # Advance across the next minute boundary + 6.5s watermark margin with 1 event/partition
        # so all completed 1-minute tumbling windows close cleanly in Flink.
        now_s = time.time()
        seconds_into_min = now_s % 60.0
        wait_to_close = (60.0 - seconds_into_min) + 6.5
        if wait_to_close <= 22.0:
            print(f"[generator] Waiting {wait_to_close:.1f}s to cross minute boundary + 5s watermark...")
            time.sleep(wait_to_close)
        else:
            time.sleep(6.5)
        pulse_ms = int(time.time() * 1000)
        for i in range(24):
            ev = {
                "event_id": f"evt-wm-pulse-{pulse_ms}-{i:02d}",
                "user_id": f"user-{(i + 1):04d}",
                "client_session_id": f"csess-pulse-{pulse_ms}",
                "event_type": "page_view",
                "product_id": "prod-001",
                "quantity": 1,
                "page_url": "/products/prod-001",
                "referrer": "direct",
                "device_type": "desktop",
                "country": "US",
                "event_time_ms": pulse_ms,
                "created_at_ms": pulse_ms,
            }
            producer.produce(
                topic="clickstream.raw.v1",
                key=ev["user_id"].encode("utf-8"),
                value=click_codec.encode(ev),
                on_delivery=_on_delivery,
            )
            sim.stats.total_emitted += 1
            sim.stats.unique_emitted += 1
            sim.stats.by_event_type["page_view"] += 1

    producer.flush(30.0)
    total_wall_sec = max(0.001, time.monotonic() - start_mono)
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery errors encountered: {delivery_errors[:3]}")

    summary = {
        "wall_seconds": round(total_wall_sec, 2),
        "achieved_eps": round(sim.stats.total_emitted / total_wall_sec, 1),
        "total_emitted": sim.stats.total_emitted,
        "unique_emitted": sim.stats.unique_emitted,
        "duplicates_emitted": sim.stats.duplicates_emitted,
        "within_watermark_ooo_emitted": sim.stats.within_watermark_ooo_emitted,
        "late_events_emitted": sim.stats.late_events_emitted,
        "price_updates_emitted": price_updates_emitted,
        "by_event_type": sim.stats.by_event_type,
    }
    print(f"[generator] Completed stream: {json.dumps(summary)}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Avro Clickstream Event Generator")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8085"),
    )
    parser.add_argument("--duration-sec", type=float, default=60.0)
    parser.add_argument("--events-per-sec", type=float, default=250.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--jitter-rate", type=float, default=0.06)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--spike-multiplier", type=float, default=1.0)
    parser.add_argument("--spike-start-sec", type=float, default=15.0)
    parser.add_argument("--spike-duration-sec", type=float, default=12.0)
    parser.add_argument("--no-price-changes", action="store_true")
    parser.add_argument("--align-window-close", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    run_stream(
        bootstrap_servers=args.bootstrap_servers,
        registry_url=args.schema_registry_url,
        duration_sec=args.duration_sec,
        events_per_sec=args.events_per_sec,
        duplicate_rate=args.duplicate_rate,
        jitter_rate=args.jitter_rate,
        late_rate=args.late_rate,
        spike_multiplier=args.spike_multiplier,
        spike_start_sec=args.spike_start_sec,
        spike_duration_sec=args.spike_duration_sec,
        emit_price_changes=not args.no_price_changes,
        align_window_close=args.align_window_close,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
