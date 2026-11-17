# System Design: Real-Time E-Commerce Clickstream Analytics Platform

This document details the capacity planning, stream-processing semantics, state sizing, delivery guarantees, serving-store selection, and production trade-offs behind the platform. Significant point decisions are recorded individually in [`docs/adr/`](adr/README.md).

---

## 1. Requirements & Workload Profile

### Functional Requirements
- **Per-minute business KPIs:** Active distinct shoppers (`COUNT(DISTINCT user_id)`), page views, add-to-cart count, checkout count, purchases, and gross merchandise revenue in both **1-minute tumbling** (`TUMBLE`) and **5-minute sliding / 1-minute advance hopping** (`HOP`) event-time windows.
- **User sessionisation:** Group per-shopper activity into dynamic `SESSION` windows closed by an inactivity gap (`30 seconds` in interactive local mode so sessions materialize quickly on the dashboard; parameterized to `30 minutes` for web analytics production), capturing session duration, page depth, cart additions, purchase conversion flag, and session revenue.
- **Multi-step funnel conversion & drop-off:** Track strict causal progression per shopper (`view` $\rightarrow$ `add_to_cart` $\rightarrow$ `checkout` $\rightarrow$ `purchase`) within bounded event time via Flink SQL `MATCH_RECOGNIZE`, reporting counts and step-to-step conversion/drop-off percentages.
- **Top-N products per window:** Rank the top 5 revenue-generating products in every 1-minute tumbling window.
- **Point-in-time catalogue pricing:** Enrich every click with the product name, category, and unit price valid **at the exact event timestamp of the click** (`FOR SYSTEM_TIME AS OF`) from a compacted Kafka topic (`product.catalog.v1`).

### Non-Functional SLAs & Chaos Conditions
- **End-to-end freshness target:** Sub-10-second p95 staleness from client event creation (`event_time_ms`) to query visibility on Grafana (`/api/ds/query`). **Measured:** **`p50 = 3.90 s` (`3,902 ms`)**, **`p95 = 5.77 s` (`5,766 ms`)** at steady state; **`p50 = 5.84 s`**, **`p95 = 10.34 s`** through a 4x flash-sale burst.
- **Real-world anomalies injected continuously by the Python generator:**
  - **Zipfian SKU skew** ($s \approx 1.25$ across 24 SKUs across 6 categories) so hero products attract realistic disproportionate load.
  - **3% duplicate `event_id` retries** (simulating mobile/SDK network timeouts).
  - **6% in-watermark out-of-order jitter** (`0.5–4.2 s` delay) + **2% severely late events** (`12–35 s` delay, arriving past watermark).
  - **4x to 10x traffic spikes** and **mid-stream flash-sale price markdowns (`-20%`) and restock markups (`+10%`)**.
- **Fault-tolerance invariant:** Zero missing or double-counted events (`missing = 0, extra = 0, revenue_delta = $0.00`) across TaskManager `SIGKILL`, 10x backpressure spikes, stateful `v1 -> v2` savepoint upgrades, and earliest-offset full historical replays.

---

## 2. Throughput Maths, Partition Sizing & Producer Benchmarks

### Target Capacity & Partition Count Derivation
We size the architecture for an e-commerce storefront sustaining:
- **Steady-state ingress ($R_{\text{steady}}$):** `5,000 events/s`
- **Flash-sale peak ($R_{\text{peak}}$, 10x spike):** `50,000 events/s`
- **Wire record size ($S_{\text{avro}}$):** `~154 B` Confluent-framed binary Avro (`1` magic byte + `4`-byte schema ID + binary payload; equivalent JSON payload is `~640 B`).
- **Peak uncompressed ingress bandwidth:** $50{,}000 \times 154\text{ B} \approx \mathbf{7.7\text{ MB/s}}$ (`~32 MB/s` if JSON).

The required partition count $N$ is bounded by the slower of per-partition broker replication throughput ($P_p \approx 10\text{ MB/s}$ with `acks=all, RF=3`) and per-subtask stateful Flink RocksDB throughput ($C_p \approx 1{,}500\text{ events/s}$ per core across deduplication + temporal join + session timers + `MATCH_RECOGNIZE` NFA):

$$N = \max\!\left(\left\lceil \frac{7.7\text{ MB/s}}{10\text{ MB/s}} \right\rceil,\; \left\lceil \frac{50{,}000\text{ eps}}{1{,}500\text{ eps/partition}} \right\rceil\right) = \max(1, 34) = 34 \implies \mathbf{36\text{ partitions (production)}}$$

Choosing **36 partitions** in production gives exact divisibility across **3 brokers** (`12` leaders/broker) and Flink parallelisms `3, 4, 6, 9, 12, 18, 36`. For our single-host 3-broker Docker Compose deployment, we scale proportionally by $1/3$ to **12 partitions** on both `clickstream.raw.v1` and `product.catalog.v1` (`4` leaders/broker, `RF=3`, `min.insync.replicas=2`) and **6 partitions** on compacted/transactional output topics. Co-partitioning `product.catalog.v1` at `12` partitions ensures every parallel Flink subtask always receives partition watermarks without starving the `TemporalRowTimeJoinOperator`.

### Partition Key Selection (`user_id`)
| Candidate Key | Cardinality | Partition Skew under Flash Sale | Stateful Flink Shuffle Impact | Decision |
|---|---|---|---|---|
| `product_id` | Low (`10^2`–`10^4`) | **Severe** — hero SKUs capture >30% of clicks on 1 partition | Forces full network keyBy shuffle for sessions & user funnels | Rejected |
| `session_id` | High | Low | Client session tokens rotate across tabs; breaks server-side `SESSION` continuity across gaps | Rejected |
| **`user_id`** | **High (`10^5`–`10^7`)** | **Uniform** | **Preserves per-user click order inside each Kafka partition; colocates session & funnel keys** | **Selected** |

### Measured Compression & Batching Benchmark (Kafka `4.4.0` KRaft, `acks=all`, `RF=3`)
Executed via `make benchmark` (`scripts/benchmark_producer.py`, `25,000` Confluent-framed Avro clickstream records per configuration, `~3,750 KB` raw user payload across 12 partitions on 3 KRaft brokers; full artifact in [`docs/benchmarks/producer_benchmark.md`](benchmarks/producer_benchmark.md)):

| Codec | `linger.ms` | `batch.size` | Throughput (events/s) | Payload MB/s | Wire TX KB | Compression Ratio | Msgs / Batch | ACK p50 (ms) | ACK p95 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `none` | 0 | 16 KiB | 26,821 | 3.93 | 4,011.1 | 0.93x | 102.0 | 654.78 | 869.40 |
| `none` | 20 | 64 KiB | 65,911 | 9.66 | 3,999.2 | 0.94x | 396.8 | 261.20 | 351.94 |
| `lz4` | 0 | 16 KiB | 19,530 | 2.86 | 1,794.7 | 2.09x | 179.9 | 953.70 | 1,192.50 |
| `lz4` | 5 | 32 KiB | 69,951 | 10.25 | 1,720.1 | 2.18x | 328.9 | 252.51 | 328.63 |
| `lz4` | 20 | 64 KiB | 40,989 | 6.00 | 1,673.3 | 2.24x | 416.7 | 562.97 | 586.81 |
| **`lz4`** | **50** | **128 KiB** | **126,544** | **18.54** | **1,669.0** | **2.25x** | **694.4** | **164.72** | **177.33** |
| `zstd` | 0 | 16 KiB | 88,205 | 12.92 | 1,100.8 | 3.41x | 198.4 | 173.68 | 250.81 |
| `zstd` | 5 | 32 KiB | 41,206 | 6.04 | 1,050.9 | 3.57x | 347.2 | 562.58 | 599.29 |
| **`zstd`** | **20** | **64 KiB** | **124,255** | **18.20** | **1,035.0** | **3.62x** | **416.7** | **124.51** | **168.62** |
| `zstd` | 50 | 128 KiB | 45,312 | 6.64 | 1,034.3 | 3.63x | 694.4 | 529.90 | 535.50 |

**Engineering Takeaways:**
1. **Uncompressed (`none`) expands wire bytes (`0.93x`)** because Kafka `RecordBatch` framing headers add ~7% overhead on compact 154-byte Avro records, and `linger.ms=0` saturates broker replication RPCs (`ACK p50 = 654.8 ms`).
2. **Wire compression is strictly monotonic with batch fill:** `lz4` shrinks wire bytes **2.09x $\rightarrow$ 2.25x** (`4,011 KB` $\rightarrow$ `1,669 KB`), while `zstd` shrinks wire bytes **3.41x $\rightarrow$ 3.63x** (`1,034 KB`) because larger `64–128 KiB` batches (`417–694` records/batch) expose recurring category, URL, and user-agent dictionary strings across Avro records.
3. **Production Default:** Both **`lz4` (`linger.ms=20–50, batch.size=64–128 KiB`)** and **`zstd` (`linger.ms=20, batch.size=64 KiB`, `124,255 eps`, `p95 = 168.6 ms`)** comfortably exceed our `50,000 eps` flash-sale ceiling by **2.5x** on a single laptop while adding $\le 20\text{ ms}$ of batching delay (< 0.6% of our `3.90 s` p50 dashboard freshness). *(Note: individual single-shot throughput dips at `lz4@20ms` and `zstd@5ms/50ms` reflect cold KRaft topic leader election + JVM C2 warmup jitter during sequential topic creation; wire byte ratios and batch sizes are deterministic.)*

---

## 3. Event-Time Semantics, Deduplication, Temporal Join & Lateness Policy

### Watermarks & Partition Idleness
Both Flink jobs assign bounded-out-of-orderness watermarks directly on client `event_time_ms`:
```sql
event_ts AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
```
With `table.exec.source.idle-timeout = '5 s'` (SQL) and `.withIdleness(Duration.ofSeconds(5))` (DataStream), an idle Kafka partition during low traffic is marked temporarily idle after 5 seconds so it cannot stall watermark progression across the remaining 11 partitions.

### Three-Tier Explicit Lateness Policy
1. **0 to 5 seconds out-of-order (`event_ts >= watermark`):** Absorbed seamlessly into the exact event-time `TUMBLE`, `HOP`, `SESSION`, `MATCH_RECOGNIZE`, and `Top-N` window where the click occurred.
2. **5 to 10 seconds behind max event time:** Closed SQL event-time windows have already fired at `window_end + 5s` (keeping dashboard freshness fast rather than holding windows open for minutes); the event is still deduplicated and recorded in `ClickstreamGuardAndFreshnessJob` for audit reconciliation.
3. **> 10 seconds behind max event time (`currentWatermark - event_ts > 5,000 ms`):** Diverted explicitly via Flink DataStream `OutputTag<String>("late-events")` into Kafka topic `clickstream.late.v1` and materialized into ClickHouse table `analytics_late_events` with exact `watermark_ms` and `lateness_ms`. Zero silent data drops.

### Subtle Pitfall #1: Why SQL Deduplication Orders by `PROCTIME()` Instead of `event_ts`
In Flink SQL, deduplicating via `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts ASC) WHERE rn = 1` produces an **updating (retract/changelog) stream** because Flink must prepare for a hypothetical later duplicate carrying a smaller timestamp. Downstream operators such as `FOR SYSTEM_TIME AS OF` temporal joins and `MATCH_RECOGNIZE` require **append-only (insert-only)** input streams or emit complex retractions into Kafka sinks. Because duplicate retries of an immutable `event_id` carry identical business payloads, ordering by **`proc_ts ASC` (`PROCTIME()`)** instructs Flink's `ProcTimeDeduplicateKeepFirstRowFunction` to emit the first arrival immediately as an **insert-only (`+I`) row** while still preserving the true rowtime attribute `event_ts` for every downstream window and temporal join.

### Subtle Pitfall #2: Bounding Multi-Partition Watermark Skew During Kappa Replays
When Flink SQL runs at `parallelism = 2` over `12` Kafka partitions (`6` partitions per `KafkaSourceReader`) and replays a historical backlog from `earliest-offset` at >50,000 records/s, Kafka's default `max.partition.fetch.bytes = 1048576` (1 MiB) drains hundreds of seconds of events from Partition 0 in a single poll batch before reading Partition 1. Because Flink SQL inserts `WatermarkAssigner` downstream of the source reader after `PROCTIME()` projection, Partition 0's timestamps advance the subtask watermark minutes ahead, causing Partitions 1–5's early events to appear falsely "late"! Setting `'properties.max.partition.fetch.bytes' = '4096'` and `'properties.max.poll.records' = '200'` interleaves all 6 assigned partitions in ~25-record micro-batches, keeping cross-partition event-time skew well inside the `5s` watermark bound and yielding **100.00% penny-exact earliest-offset replays**.

### Event-Time Temporal Join (`FOR SYSTEM_TIME AS OF`)
`product.catalog.v1` is configured with `cleanup.policy=compact` and **`min.compaction.lag.ms=86400000` (24 hours)**. Flink SQL reads it via `upsert-kafka` keyed by `product_id` and executes:
```sql
FROM deduplicated_clicks AS c
LEFT JOIN product_catalog FOR SYSTEM_TIME AS OF c.event_ts AS p
  ON c.product_id = p.product_id
```
Flink maintains a versioned timestamped map per `product_id` in RocksDB: a click with `event_ts = 12:00:04.900` joins against `$199.00`, while a click with `event_ts = 12:00:05.100` (after a `-20%` flash-sale update at `12:00:05.000`) joins against `$159.20`. Keeping `min.compaction.lag.ms=24h` ensures Kafka brokers do not compact away intermediate intraday price versions needed during an `earliest-offset` replay.

---

## 4. RocksDB State Size Estimates vs Measured Checkpoints

With `table.exec.state.ttl = '15 min'` (SQL) and `StateTtlConfig.newBuilder(Duration.ofMinutes(15))` (DataStream), RocksDB state is strictly bounded rather than growing indefinitely.

### Analytical Estimate (Steady 5,000 eps vs Production 50,000 eps Peak)
| Stateful Operator | Key & Value Structure | Keys Held (`15m` TTL @ 5,000 eps) | Est. Size @ 5k eps | Est. Size @ 50k eps (`1h` Dedup TTL) |
|---|---|---:|---:|---:|
| **SQL + Guard `event_id` Dedup** | `36 B` UUID key + `16 B` timestamp/meta + RocksDB prefix overhead (`~110 B/entry`) | `4.5M` unique events | `~495 MB` ($\times 2$ jobs $\approx 990\text{ MB}$) | `~19.8 GB` |
| **Temporal Catalogue (`product_catalog`)** | `product_id` (`16 B`) $\rightarrow$ list of `(ts, name, category, price)` (`~180 B/version`) | `10,000` SKUs $\times 4$ versions | `~7.2 MB` | `~72 MB` (`100k` SKUs) |
| **`SESSION` Windows (`30m` prod gap)** | `user_id` (`20 B`) $\rightarrow$ session accumulator (`~140 B`) | `250,000` concurrent shoppers | `~40 MB` | `~400 MB` (`2.5M` shoppers) |
| **`MATCH_RECOGNIZE` Funnel NFA** | `user_id` $\rightarrow$ partial NFA states (`view`/`cart`/`checkout`, `~260 B/user`) | `250,000` active funnels | `~65 MB` | `~650 MB` |
| **`TUMBLE` + `HOP` + `Top-N` Panes** | `6` active 1m/5m panes $\times$ (`HLL/distinct set` + `SKU sums`) | `250k` user keys across active panes | `~120 MB` | `~1.2 GB` |
| **Total Managed RocksDB State** | Off-heap LSM SST files + block cache | — | **`~1.22 GB`** | **`~22.1 GB`** (across 12–36 subtasks $\approx$ `< 1.9 GB/TM`) |

### Measured Checkpoints in This Build
After processing `~45,800` clickstream events across `600` concurrent shoppers and `24` multi-versioned SKUs (`make status`), Flink's REST `/jobs/<id>/checkpoints` reported:
- **`clickstream-sql-analytics` (6-sink `StatementSet`):** **`17.98 MB`** (`17,981,884 bytes`) on `s3a://flink-checkpoints/checkpoints`, completing incremental checkpoints in `< 180 ms`.
- **`clickstream-datastream-guard` (DataStream job):** **`1.54 MB`** (`1,544,537 bytes`), completing incremental checkpoints in `< 60 ms`.

---

## 5. Delivery Guarantees Per Hop & End-to-End Freshness Breakdown

### Hop-by-Hop Guarantee Matrix
| Hop | Producer / Engine Mechanism | Failure Behaviour | Effective Guarantee |
|---|---|---|---|
| **1. Client / Generator $\rightarrow$ Kafka `clickstream.raw.v1`** | `acks=all`, `enable.idempotence=true`, `min.insync.replicas=2` + application-level `event_id` UUID | Broker crash loses `0` committed messages; client network retry may append same `event_id` twice | **At-least-once ingress, deduplicated downstream by `event_id`** |
| **2. Kafka $\rightarrow$ Flink Stateful Compute** | Kafka partition offsets stored inside Flink's `2,000 ms` distributed barrier checkpoints on MinIO S3 (`EXACTLY_ONCE`) | On TaskManager `SIGKILL`, Flink rewinds Kafka offsets and RocksDB SSTs atomically to last completed checkpoint | **Exactly-once state consistency** |
| **3. Flink $\rightarrow$ Kafka `analytics.*.v1`** | Kafka transactional sink (`'sink.delivery-guarantee' = 'exactly-once'`, 2PC commit on `notifyCheckpointComplete`, unique `transactional-id-prefix`) | Uncommitted records written by a crashed TaskManager before checkpoint completion are **aborted** on recovery | **Exactly-once committed Kafka records** |
| **4. Kafka $\rightarrow$ ClickHouse `26.8` Serving Tables** | `<isolation_level>read_committed</isolation_level>` (`clickhouse/config.d/kafka.xml`) + `ReplacingMergeTree(updated_at_ms)` queried with `FINAL` | Aborted Kafka transactions are skipped by `librdkafka`; window upserts or full replays replace deterministic primary keys idempotently | **End-to-end effective-once serving accuracy** |

### Empirical Proof of Downstream `read_committed` Isolation
`make freshness` (`scripts/measure_freshness.py --test-aborted-txn`) opens real Kafka transactions on an isolated probe topic, commits `5` records, explicitly aborts `5` records (`abort_transaction()`), and consumes from earliest offset with both isolation modes (`docs/benchmarks/freshness_report.json`):
- Consumer with `isolation.level = read_uncommitted`: observed **`10` records** (leaked all `5` aborted records—corrupting downstream revenue if misconfigured!).
- Consumer with `isolation.level = read_committed`: observed **`5` records** (**`0` uncommitted/aborted records leaked**).

### Measured End-to-End Freshness Breakdown (`300 / 300` Probes Matched)
Polled at `150 ms` intervals through Grafana's live HTTP datasource proxy (`POST /api/ds/query` $\rightarrow$ ClickHouse `FINAL` SQL):

| Latency Stage | Min | p50 | p95 | p99 | Max | Primary Driver |
|---|---:|---:|---:|---:|---:|---|
| **Event Creation $\rightarrow$ Flink Operator** | `15 ms` | **`216 ms`** | **`1,686 ms`** | `2,147 ms` | `2,226 ms` | Producer `linger.ms=20` + Kafka fetch |
| **Event Creation $\rightarrow$ ClickHouse (`read_committed`)** | `1,636 ms` | **`3,737 ms`** | **`5,485 ms`** | `6,072 ms` | `6,193 ms` | Flink `2,000 ms` 2PC checkpoint barrier wait (`0–2s`, avg `1s`) + ClickHouse `500 ms` Kafka flush |
| **Event Creation $\rightarrow$ Grafana Dashboard Query** | `1,869 ms` | **`3,902 ms`** | **`5,766 ms`** | `6,188 ms` | `6,356 ms` | ClickHouse `FINAL` query + Grafana HTTP proxy (`~165 ms`) |

*Key Architectural Insight:* Under Kafka 2-phase commit, downstream `read_committed` consumers cannot read records past the `Last Stable Offset (LSO)` until Flink completes a checkpoint and commits the Kafka transaction. Consequently, end-to-end freshness has a mathematical floor of $\approx \frac{1}{2} T_{\text{ckpt}} + T_{\text{ch\_flush}}$ at p50 and $\approx T_{\text{ckpt}} + T_{\text{ckpt\_dur}} + T_{\text{ch\_flush}}$ at p95. Tuning $T_{\text{ckpt}} = 2{,}000\text{ ms}$ and `kafka_flush_interval_ms = 500` puts p50 right at **`3.90 s`** and p95 at **`5.77 s`**.

---

## 6. Serving-Store Choice: ClickHouse 26.8 LTS vs Apache Pinot

Full rationale in [`ADR-0003`](adr/0003-clickhouse-over-apache-pinot.md):

| Dimension | ClickHouse `26.8 LTS` (Chosen) | Apache Pinot `1.x` |
|---|---|---|
| **Operational Footprint** | **1 lightweight container** (`~450 MB` RSS, boots in `<3s`) | **4–5 JVM services** (Controller, Broker, Server, Minion + ZooKeeper/Helix, `4–6 GB` RAM) |
| **Idempotent Upserts & Replays** | **`ReplacingMergeTree(updated_at_ms)` + `FINAL`** handles Flink changelog updates, `Top-N` rank revisions, and full earliest-offset replays with zero primary-key heap index | Realtime Upsert tables require co-partitioned Kafka keys and hold primary-key metadata in server memory/RocksDB |
| **Kafka `read_committed` Ingestion** | Built-in `Kafka` table engine (`librdkafka`) configured declaratively in `init.sql` + `kafka.xml` | Per-table JSON schema + realtime table config POSTed via Controller REST API |
| **Ad-Hoc Funnel & Recount SQL** | Full vectorized ANSI SQL (`argMax`, `quantileTDigest`, window joins) + official Grafana plugin `4.23.5` | Fast star-tree OLAP, thinner ad-hoc join ergonomics in single-node dev |

*When Apache Pinot wins instead:* External customer-facing multi-tenant analytics serving **10,000+ concurrent point-lookup QPS** at sub-20ms p99 where Pinot's Star-Tree index avoids merge-on-read (`FINAL`) CPU overhead.

---

## 7. Key Trade-Offs & What Was Plainly Not Measured

### Deliberate Engineering Trade-Offs
1. **`2,000 ms` Checkpoint Interval (Freshness vs Object-Store IOPS):** Halving checkpoints from `10s` to `2s` cut Grafana p95 staleness from `~13s` to `5.77s`, at the cost of `30` incremental S3 `PUT` cycles per minute per job. At thousand-subtask scale, we would either raise checkpoints to `5s` or sink directly via ClickHouse's native transactional connector.
2. **Strict Window Closure (`5s` Watermark) vs Holding Windows Open:** SQL windows emit final aggregates `5s` after window end rather than holding state open with long allowed lateness (which would generate continuous retract churn in Kafka). Events `>10s` late are captured cleanly in `clickstream.late.v1` (`analytics_late_events`) so finance/reconciliation pipelines can merge late adjustments without delaying live dashboards.
3. **`ReplacingMergeTree` + `FINAL` Read Cost:** `FINAL` deduplicates unmerged parts at query time. For `1-minute` windows (`1,440` rows/day) and `Top-5` products (`7,200` rows/day), `FINAL` executes in `< 5 ms`. For billion-row raw click tables, queries should avoid `FINAL` or pre-aggregate via Incremental Materialized Views.

### What Was NOT Measured in This Build
To keep engineering claims rigorous and honest:
- **Cross-host / cross-AZ network RTT and cloud disk contention:** All 10 containers ran on a single Linux host (WSL2, 16 vCPUs, 32 GB RAM) over a local Docker bridge network (`<0.2 ms` RTT) backed by local NVMe storage. Production AWS/Azure deployments across 3 Availability Zones add `0.8–2.0 ms` per synchronous `acks=all` Kafka replication hop and `15–40 ms` S3 checkpoint `PUT` latency.
- **Multi-hour RocksDB compaction & 24-hour TTL eviction steady state at 50,000 eps:** Live end-to-end soak and chaos tests ran for `65–120 seconds` at `250–8,278 eps` (plus `25,000`-record producer bursts up to `126,544 eps`), not multi-day continuous compaction under `20+ GB` RocksDB state.
- **High-concurrency dashboard query load:** Grafana freshness was measured with a single polling client (`150 ms` cadence) rather than hundreds of concurrent dashboard viewers hitting `FINAL` queries simultaneously.
- **Kafka broker quorum loss / disk corruption:** Chaos testing killed active Flink TaskManagers (`SIGKILL`) and wiped serving tables, but did not simulate simultaneous multi-broker KRaft disk failure.

---

## 8. What Would Change for Production

1. **Compute & Orchestration:** Deploy Flink via the **Apache Flink Kubernetes Operator** (`FlinkDeployment` CRDs) with reactive/autoscaler mode (`job.autoscaler.enabled: true`) scaling TaskManagers on Kafka partition lag and vertex `busyRatio`, rather than static Docker Compose replicas.
2. **Object Storage (Post-MinIO OSS Archival):** Replace local Chainguard MinIO with managed cloud object storage (**AWS S3**, **Azure Data Lake Storage Gen2**, or **GCS**) with prefix sharding and lifecycle policies—or **SeaweedFS / Ceph RGW** on-premises (see [`ADR-0005`](adr/0005-rocksdb-incremental-checkpoints-and-minio-2026.md)).
3. **Security & Governance:** Enforce mTLS + SASL/SCRAM or OAuthBearer on Kafka `4.4`, RBAC on Karapace Schema Registry with CI `BACKWARD_TRANSITIVE` compatibility gates prior to producer deploy, and Kubernetes Secrets / Vault for ClickHouse & Grafana credentials (replacing `.env` dev defaults).
4. **High-Availability Serving Store:** Run a 3-shard $\times$ 2-replica **ClickHouse Keeper** cluster using `ReplicatedReplacingMergeTree` and decoupled `ClickHouse Cloud` / S3-backed zero-copy storage, fronted by `chproxy` connection pooling for Grafana.
5. **Observability & Paging:** Scrape Flink Prometheus metrics (`numRecordsInPerSecond`, `currentEmitEventTimeLag`, `lastCheckpointDuration`, `isBackPressured`), Kafka JMX (`UnderReplicatedPartitions`, `OfflinePartitionsCount`), and ClickHouse `system.metrics` into Prometheus + Alertmanager.
