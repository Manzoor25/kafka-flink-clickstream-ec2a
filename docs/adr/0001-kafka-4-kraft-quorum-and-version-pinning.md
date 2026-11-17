# ADR-0001: Pin Apache Kafka 4.4.0 (3-Node KRaft) and Apache Flink 2.2.1

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

We need a reproducible, ZooKeeper-free Kafka cluster on the 4.x release line and a stateful Flink cluster supporting Flink SQL (`MATCH_RECOGNIZE`, Window TVFs, `FOR SYSTEM_TIME AS OF` temporal joins on `upsert-kafka`), the Java DataStream API, RocksDB incremental checkpoints on S3, and Confluent-framed Avro serialization.

Checking upstream Apache releases in November 2026 shows:
- **Apache Kafka:** `4.4.0` (released October 22, 2026; official image `apache/kafka:4.4.0`). Kafka 4.x completely removed ZooKeeper in favor of KRaft metadata quorums.
- **Apache Flink:** `2.3.0` is the newest core Flink release, **however** the latest official Apache Flink Kafka Connector on Maven Central is `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` (compiled against Flink `2.2.x` internal table planner interfaces; no `-2.3` artifact has been published yet). Flink `2.2.1` (`flink:2.2.1-scala_2.12-java21`) has complete binary parity across `flink-connector-kafka:5.0.0-2.2`, `flink-sql-avro-confluent-registry:2.2.1`, and `flink-s3-fs-hadoop:2.2.1`.

## Decision

1. Pin **`apache/kafka:4.4.0`** running as a **3-node combined `broker,controller` KRaft cluster** (`kafka-1`, `kafka-2`, `kafka-3`), with `default.replication.factor=3`, `offsets.topic.replication.factor=3`, `transaction.state.log.replication.factor=3`, and `min.insync.replicas=2`.
2. Pin **`flink:2.2.1-scala_2.12-java21`** (1 JobManager + 2 TaskManagers, 4 task slots per TaskManager) paired with `flink-connector-kafka:5.0.0-2.2` and `flink-sql-connector-kafka:5.0.0-2.2`.

## Consequences

- **Positive:** Zero connector/planner `NoSuchMethodError` mismatches; 3-broker KRaft tolerates single-broker failure while sustaining transactional `EXACTLY_ONCE` commits; Java 21 LTS across build and runtime.
- **Negative / Production Delta:** Combined `broker,controller` roles simplify Docker Compose topology to 3 containers; in production, dedicated controller nodes (3 controllers + $N$ brokers) isolate Raft metadata elections from page-cache and network saturation.
