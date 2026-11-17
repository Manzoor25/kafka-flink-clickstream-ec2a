# ADR-0007: End-to-End Exactly-Once Delivery (`read_committed`), Idempotent Serving, and Grafana Provisioning

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

To guarantee accurate revenue and funnel totals even when a TaskManager crashes mid-window, every hop from producer to dashboard must have well-defined failure semantics, and the dashboard must start ready-to-query from version-controlled configuration without manual UI clicks.

## Decision

1. **Hop 1 (Generator $\rightarrow$ Kafka):** Idempotent producer (`enable.idempotence=true`, `acks=all`) + explicit downstream Flink `event_id` deduplication to handle application-level retries.
2. **Hop 2 (Kafka $\rightarrow$ Flink $\rightarrow$ Kafka):** Flink `EXACTLY_ONCE` checkpoints (5 s interval on MinIO S3) coordinated via two-phase commit (`2PC`) with Kafka transactional sinks (`'sink.delivery-guarantee' = 'exactly-once'`, unique `'sink.transactional-id-prefix'`, `transaction.timeout.ms = 300000` < broker `transaction.max.timeout.ms = 900000`).
3. **Hop 3 (Kafka $\rightarrow$ ClickHouse):** Configure ClickHouse's embedded `librdkafka` consumer explicitly with **`<isolation_level>read_committed</isolation_level>`** (`docker/clickhouse/config.d/kafka.xml`) so uncommitted transactions and transactions aborted during TaskManager failover are never materialized. Back every serving table with **`ReplacingMergeTree(updated_at_ms)`** keyed by natural window/entity primary keys (`(window_start, window_end)`, `(user_id, session_start)`, `(window_start, product_id)`, `(event_id)`) and query with `FINAL` so changelog updates (`upsert`/re-emissions) and full earliest-offset replays are strictly idempotent.
4. **Hop 4 (Dashboard as Code):** Choose **Grafana OSS `13.3.1`** with pinned **`grafana-clickhouse-datasource` `4.23.5`** over Apache Superset. Grafana provisions datasources and JSON dashboards declaratively from mounted files on container boot in <5 seconds with native `5s` live auto-refresh, whereas Superset requires a metadata database migration, admin bootstrap, and dataset/chart import CLI steps.

## Consequences

- **Positive:** End-to-end effective-once accuracy verified by killing a TaskManager under load and proving streaming totals match an offline batch recount to the exact event and cent.
- **Trade-off:** Kafka `EXACTLY_ONCE` two-phase commit holds downstream `read_committed` visibility until each Flink checkpoint completes! Setting the checkpoint interval to **5 seconds** (or **2 seconds** in low-latency profiles) bounds transactional commit latency so p50/p95 dashboard freshness stays within a few seconds.
