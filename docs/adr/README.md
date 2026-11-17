# Architecture Decision Records (ADRs)

Every major architectural and operational choice in this platform is captured in a concise ADR:

| ADR | Decision | Status |
|---|---|---|
| [ADR-0001](0001-kafka-4-kraft-quorum-and-version-pinning.md) | Pin Apache Kafka `4.4.0` (3-node KRaft) and Apache Flink `2.2.1` (connector-matrix parity) | Accepted |
| [ADR-0002](0002-topic-partition-math-keying-and-compression.md) | 12-partition local / 36-partition prod sizing, `user_id` partition key, and `lz4` + `20ms` batching | Accepted |
| [ADR-0003](0003-clickhouse-over-apache-pinot.md) | Serve real-time OLAP from ClickHouse `26.8` LTS over Apache Pinot | Accepted |
| [ADR-0004](0004-karapace-schema-registry-and-avro-evolution.md) | Use Karapace `6.2.4` (Apache-2.0) for Confluent-wire-compatible Avro schema governance | Accepted |
| [ADR-0005](0005-rocksdb-incremental-checkpoints-and-minio-2026.md) | Embedded RocksDB incremental checkpoints on S3 and Chainguard MinIO digest pinning (post-Sept 2026 registry removal) | Accepted |
| [ADR-0006](0006-flink-sql-plus-datastream-hybrid-and-lateness.md) | Hybrid Flink SQL (`STATEMENT SET`) + DataStream API (`KeyedProcessFunction` + side-output late routing) | Accepted |
| [ADR-0007](0007-end-to-end-delivery-guarantees-and-grafana.md) | Kafka transactional sinks (`read_committed`) + ClickHouse `ReplacingMergeTree` + Grafana `13.3.1` as code | Accepted |
