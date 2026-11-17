# ADR-0005: RocksDB Incremental Checkpoints on S3 and Post-2026 MinIO Image Pinning

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Our Flink pipelines maintain four distinct stateful structures simultaneously:
1. Per-`event_id` deduplication state (millions of keys within TTL window),
2. Versioned temporal table state for `product.catalog.v1` (`FOR SYSTEM_TIME AS OF`),
3. Per-user `SESSION` window panes and `MATCH_RECOGNIZE` NFA state machines,
4. Tumbling/hopping window accumulators and Top-N rank buffers.

Using `HashMapStateBackend` stores every key as boxed Java objects on the JVM heap (triggering GC pauses) and snapshots full state on every checkpoint. Furthermore, for local S3-compatible object storage: **in September 2026, MinIO deleted `docker.io/minio/minio` and `docker.io/minio/mc` (Sept 11) and disabled anonymous pulls on `quay.io/minio/minio` (Sept 24)** following the April 2026 archival of the upstream OSS repository. Any `docker-compose.yml` still referencing `minio/minio` fails immediately with `401 Unauthorized` / `pull access denied`.

## Decision

1. Configure Flink `2.2.1` with **`state.backend.type: rocksdb`**, **`state.backend.incremental: true`**, and **`state.backend.rocksdb.memory.managed: true`**, persisting checkpoints to `s3://flink-checkpoints/checkpoints` and savepoints to `s3://flink-checkpoints/savepoints` every **5 seconds** (`EXACTLY_ONCE`, `EXTERNALIZED_CHECKPOINT_RETENTION: RETAIN_ON_CANCELLATION`) via `flink-s3-fs-hadoop:2.2.1` (`path.style.access: true`).
2. Bound unbounded SQL relational state with `table.exec.state.ttl: 15 min` (and `StateTtlConfig` in the DataStream job) so deduplication and window keys expire predictably.
3. Pin the publicly verifiable Chainguard MinIO server image by immutable OCI digest: **`cgr.dev/chainguard/minio@sha256:59953701f8f8d0de81aab456f0b0b6c0a1804119415559bf7ede79d944108879`**, and initialize buckets via Python `boto3` rather than the deleted `minio/mc` container.

## Consequences

- **Positive:** Off-heap LSM-tree storage eliminates heap GC spikes; incremental SST uploads keep 5-second checkpoints sub-second; digest-pinned Chainguard MinIO pulls cleanly without registry credentials in CI and clean clones.
- **Production Delta:** Because upstream OSS MinIO archived in 2026, production deployments should target managed cloud object storage (AWS S3, Azure Data Lake Storage Gen2, GCS) or an actively maintained Apache-2.0 S3 gateway such as **SeaweedFS** or **Ceph RGW**.
