"""Idempotent platform bootstrap: MinIO S3 checkpoint bucket, Karapace schemas, and initial catalogue."""

from __future__ import annotations

import os
import time
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, Producer, TopicPartition

from generator.clickstream_gen.avro_wire import register_schema, wait_for_registry
from generator.clickstream_gen.catalog import publish_catalog_snapshot

BASELINE_CATALOG_TIMESTAMP_MS = 1704067200000  # 2024-01-01T00:00:00Z — valid for all live & replayed clicks


def ensure_minio_bucket(endpoint: str, access_key: str, secret_key: str, bucket: str = "flink-checkpoints") -> None:
    deadline = time.monotonic() + 60.0
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1",
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            )
            try:
                s3.head_bucket(Bucket=bucket)
                print(f"[bootstrap] MinIO bucket s3://{bucket} already exists.")
            except ClientError:
                s3.create_bucket(Bucket=bucket)
                print(f"[bootstrap] Created MinIO bucket s3://{bucket}.")
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(1.5)
    raise TimeoutError(f"MinIO at {endpoint} did not become ready in 60s: {last_err}")


def catalog_topic_has_records(bootstrap_servers: str, topic: str = "product.catalog.v1") -> bool:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"bootstrap-probe-{int(time.time() * 1000)}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = consumer.list_topics(topic=topic, timeout=10.0)
        topic_md = md.topics.get(topic)
        if not topic_md or topic_md.error is not None:
            return False
        for pid in topic_md.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5.0)
            if high > low:
                return True
        return False
    finally:
        consumer.close()


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8085")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_password = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
    schemas_dir = Path(os.environ.get("SCHEMAS_DIR", "/workspace/schemas"))
    if not schemas_dir.exists():
        schemas_dir = Path(__file__).resolve().parents[2] / "schemas"

    ensure_minio_bucket(minio_endpoint, minio_user, minio_password, bucket="flink-checkpoints")
    wait_for_registry(registry_url)

    click_codec = register_schema(
        registry_url=registry_url,
        subject="clickstream.raw.v1-value",
        schema_path=schemas_dir / "clickstream_event.avsc",
        compatibility="BACKWARD",
    )
    catalog_codec = register_schema(
        registry_url=registry_url,
        subject="product.catalog.v1-value",
        schema_path=schemas_dir / "product_catalog.avsc",
        compatibility="BACKWARD",
    )
    print(
        f"[bootstrap] Registered Avro schemas in Karapace "
        f"(clickstream id={click_codec.schema_id}, catalog id={catalog_codec.schema_id})."
    )

    if catalog_topic_has_records(bootstrap_servers):
        print("[bootstrap] Compacted product.catalog.v1 already seeded; preserving existing price history.")
    else:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
            }
        )
        seeded = publish_catalog_snapshot(
            producer=producer,
            codec=catalog_codec,
            updated_at_ms=BASELINE_CATALOG_TIMESTAMP_MS,
        )
        print(f"[bootstrap] Seeded {len(seeded)} baseline products into compacted product.catalog.v1.")


if __name__ == "__main__":
    main()
