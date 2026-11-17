# ADR-0003: Choose ClickHouse 26.8 LTS Over Apache Pinot as the Real-Time Serving Store

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

The platform requires a real-time OLAP serving store ingesting streaming outputs directly from Kafka and serving sub-second dashboard queries (active users, sessions, funnel conversion, top products, revenue/minute, and p50/p95 freshness) on a single developer machine as well as in CI. The candidates are **Apache Pinot** and **ClickHouse**.

| Criterion | ClickHouse (`26.8.38.9-alpine` LTS) | Apache Pinot (`1.x`) |
|---|---|---|
| Container topology | **1 container** (single native C++ binary, ~400 MB RSS idle) | **4+ containers** (ZooKeeper + Controller + Broker + Server JVMs, 3–5 GB RSS idle) |
| Kafka ingestion | Built-in `Kafka` table engine + SQL `Materialized View` | Table config JSON + Realtime segment consumers via Controller REST API |
| Transactional Kafka support | Supports `librdkafka` `isolation.level=read_committed` | Consumes offsets directly; transactional marker awareness varies by consumer |
| Mutable window / replay semantics | `ReplacingMergeTree(version)` + `FINAL` deduplicates updated window emissions & full replays cleanly | Native partition-key Upsert tables (requires primary-key routing & memory overhead per key) |
| Ad-hoc SQL & quantiles | Full ANSI SQL + `quantileTDigest` / `quantileExact` for live p50/p95 latency | Calcite SQL subset; multi-table joins require v2 multi-stage engine |
| License | Apache-2.0 | Apache-2.0 |

## Decision

Select **ClickHouse `26.8.38.9-alpine` (26.8 LTS)** as the serving store, ingesting Flink's transactional Kafka topics via ClickHouse `Kafka` engine tables (`isolation.level = read_committed`, `kafka_flush_interval_ms = 1000`) into `ReplacingMergeTree` serving tables queried with `FINAL` by Grafana.

## Consequences

- **Positive:** Fast cold boot (<5 s), tiny RAM footprint alongside 3 Kafka brokers and 3 Flink JVMs, zero external ZooKeeper dependency, deterministic idempotency during TaskManager failover and earliest-offset reprocessing, and native millisecond freshness quantiles.
- **Negative / When Pinot Wins:** At high-concurrency user-facing serving (>10,000 QPS point/slice queries embedded directly inside a merchant portal), Apache Pinot's Star-Tree pre-aggregation indexes and inverted/range indexes provide lower tail latency than `ReplacingMergeTree ... FINAL` scans.
