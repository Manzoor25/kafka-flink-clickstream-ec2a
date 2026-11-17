# ADR-0006: Hybrid Flink SQL (`STATEMENT SET`) + DataStream API Architecture and Lateness Policy

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Most clickstream aggregations (deduplication, temporal price enrichment, tumbling/hopping/session windows, `MATCH_RECOGNIZE` funnel conversion, and Top-N products) are relational transformations best expressed declaratively in **Flink SQL**. However, two capabilities benefit directly from the lower-level **Java DataStream API**:
1. Routing events that arrive **beyond** the watermark + allowed-lateness threshold into an explicit dead-letter/audit stream (`OutputTag` side output) rather than silently dropping them, and
2. Emitting per-event millisecond freshness probes stamped at exact Flink operator processing time alongside fine-grained RocksDB `ValueState` deduplication and user spend velocity alerts.

Additionally, submitting 5 independent SQL `INSERT INTO` queries as separate Flink jobs would instantiate 5 redundant Kafka consumers, 5 RocksDB deduplication state tables, and 5 temporal catalogue caches, multiplying task slots by 5x.

## Decision

1. **Job 1 — Unified Flink SQL Pipeline (`ClickstreamSqlPipelineJob`):** Compiles all 5 analytical sinks inside a single Flink `StatementSet`, sharing one `clickstream_raw` Kafka source, one RocksDB `ROW_NUMBER()` deduplication operator, and one `FOR SYSTEM_TIME AS OF` temporal join against `product_catalog` (`upsert-kafka`).
2. **Job 2 — Java DataStream API Job (`ClickstreamGuardAndFreshnessJob`):** Uses `KafkaSource`, `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withIdleness(Duration.ofSeconds(5))`, a keyed `KeyedProcessFunction` backed by RocksDB `ValueState` + `StateTtlConfig` that deduplicates by `event_id`, routes events arriving later than `watermark` beyond a `10s` tolerance to `OutputTag("late-events")` $\rightarrow$ `clickstream.late.v1`, and writes per-event freshness & integrity records to `analytics.freshness_probes.v1` via an `EXACTLY_ONCE` transactional `KafkaSink`.
3. **Explicit Event-Time & Lateness Policy:**
   - **0–5 s late:** Absorbed transparently by `WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND` across both SQL and DataStream jobs.
   - **5–15 s late:** Captured by the DataStream audit/recount guard tolerance window; dropped from closed SQL windows (which emit final panes at `window_end + 5s` to keep dashboard staleness low).
   - **>15 s late:** Never silently lost — diverted via DataStream side output to `clickstream.late.v1` for audit/reconciliation.
   - **Idle partitions:** `withIdleness(5s)` / `table.exec.source.idle-timeout = '5 s'` prevents a quiet partition during low traffic from stalling watermarks cluster-wide.

## Consequences

- **Positive:** Combines 80% concise, testable SQL with 20% surgical DataStream control; fits comfortably inside 4 total task slots across 2 TaskManagers (allowing seamless single-TM failover during chaos drills).
- **Negative:** Upgrading a multi-sink SQL `StatementSet` changes the compiled operator DAG topology when new SQL joins/projections are added; ADR and Runbook explicitly distinguish topology-compatible savepoint upgrades vs stateless/reprocessing upgrades.
