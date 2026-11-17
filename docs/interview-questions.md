# 10 Customer & Technical Interview Questions (With Engineering Answers)

These 10 questions represent what a Principal Data Engineer interviewer or enterprise e-commerce customer will ask when reviewing this architecture.

---

### 1. Your Flink operators process events in 216 ms at p50, yet end-to-end Grafana freshness is 3.90 s at p50 and 5.77 s at p95. Where do the other 3.7 seconds go, and can you make it sub-second?

**Answer:**  
They go purposefully into **Kafka two-phase commit (`2PC`) transactional isolation**. Because our Flink sinks run with `'sink.delivery-guarantee' = 'exactly-once'`, Flink writes records to Kafka inside an open transaction during the `2,000 ms` checkpoint interval and only commits that transaction inside `notifyCheckpointComplete()` after every operator's RocksDB incremental SST snapshot lands safely on MinIO S3. Downstream, ClickHouse's `librdkafka` consumer runs with `isolation.level = read_committed`, which blocks reading past Kafka's `Last Stable Offset (LSO)` until the transaction commits (plus up to `500 ms` for ClickHouse's `kafka_flush_interval_ms`).  

Mathematically, an event arriving uniformly within a `2.0 s` checkpoint cycle waits `1.0 s` on average (`2.0 s` worst-case) for the next barrier, plus window/flush/HTTP query latency—matching our measured `3.74 s` ClickHouse p50 and `3.90 s` Grafana p50. If a customer wanted **sub-second freshness** for non-financial metrics (like live active page views), we would split that sink onto an `AT_LEAST_ONCE` path (`read_uncommitted`) backed by ClickHouse `ReplacingMergeTree` idempotency, trading strict transactional cut-points for `<800 ms` p95 latency while keeping revenue on `EXACTLY_ONCE`.

---

### 2. Why did you key `clickstream.raw.v1` by `user_id` instead of `product_id` or `event_id`? What happens during a celebrity product drop when 40% of shoppers click one SKU?

**Answer:**  
If we keyed Kafka by `product_id`, 40% of the entire site's flash-sale traffic would land on **one single Kafka partition** and overwhelm a single Flink source subtask, starving the other 11 (or 35 in production) partitions. Conversely, if we keyed by `event_id`, per-user click ordering inside Kafka would be lost before watermarks are assigned, and every user-scoped operator (`SESSION` windows and `MATCH_RECOGNIZE` funnels) would incur a full network shuffle.  

Keying `clickstream.raw.v1` by `user_id` spreads clicks evenly across hundreds of thousands of shoppers even when they all view the same hero SKU, while preserving per-user causal click ordering inside each partition. Only the final pre-aggregated `Top-N` product count shuffles by `(window_start, product_id)`—after Flink's local mini-batch/window pre-aggregation has already collapsed thousands of clicks into partial sums.

---

### 3. Why does your Flink SQL deduplication order by `proc_ts ASC` (`PROCTIME()`) instead of `event_ts ASC` (`ROWTIME`)? Doesn't event-time ordering seem more natural?

**Answer:**  
This is one of the sharpest Flink SQL planner subtleties in production. Writing `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts ASC) WHERE rn = 1` forces Flink to treat the output as a **changelog/retract stream (`-U` / `+U`)**, because Flink cannot prove that a later duplicate won't arrive carrying an *earlier* `event_ts` that displaces the previously emitted row. Once a stream turns into a retract stream, downstream event-time `MATCH_RECOGNIZE` and append-only Kafka window pipelines either fail planner validation or emit retraction storms.  

Because retries of the **same immutable `event_id`** carry identical payload fields (`user_id`, `product_id`, `quantity`, `event_time_ms`), keeping the **first-seen copy** (`ORDER BY proc_ts ASC`) is semantically identical while allowing Flink's `ProcTimeDeduplicateKeepFirstRowFunction` to emit a pure **insert-only (`+I`) stream** that still carries the original `event_ts` rowtime attribute into every downstream temporal join and event-time window.

---

### 4. How does the `FOR SYSTEM_TIME AS OF` temporal join work when a price changes mid-minute, and why did you set `min.compaction.lag.ms = 86400000` on `product.catalog.v1`?

**Answer:**  
`product.catalog.v1` is read via Flink's `upsert-kafka` connector keyed by `product_id` with its own event-time watermark (`updated_at_ms - 5s`). Flink's `TemporalRowTimeJoinOperator` stores a timestamp-indexed version history per SKU in RocksDB. When a shopper's click at `12:00:04.900` is joined, Flink looks up the latest catalogue version with `updated_ts <= 12:00:04.900` (`$199.00`), even if the `$159.20` flash-sale markdown at `12:00:05.000` has already arrived in Kafka! RocksDB only prunes superseded versions once the clickstream watermark advances safely past them.  

Setting **`min.compaction.lag.ms = 86400000` (24 hours)** on Kafka is critical for **Kappa reprocessing**: without a compaction lag floor, Kafka's log cleaner could compact `product.catalog.v1` down to *only* the latest `$159.20` record within minutes, causing an `earliest-offset` replay hours later to retroactively misprice pre-sale clicks at the discounted price.

---

### 5. During Failure Demo 4 (`earliest-offset` replay), why did `max.partition.fetch.bytes` matter when Flink `parallelism (2) < Kafka partitions (12)`?

**Answer:**  
In live steady-state streaming (`250–800 eps`), every partition trickles records continuously, so all 6 partitions assigned to one `KafkaSourceReader` stay within milliseconds of each other in event time. During an **`earliest-offset` catch-up replay**, however, all 12 partitions already hold thousands of historical messages on disk. With Kafka's default `max.partition.fetch.bytes = 1 MiB`, a single `poll()` drains the entire multi-minute history of Partition 0 before moving to Partition 1. Because Flink SQL places `WatermarkAssigner` *after* the source's `PROCTIME()` projection, Partition 0's final timestamp pushes the subtask watermark minutes forward—causing early historical events read milliseconds later from Partitions 1–5 to be dropped as "late"!  

Capping `'properties.max.partition.fetch.bytes' = '4096'` and `'properties.max.poll.records' = '200'` forces round-robin micro-batch interleaving (~25 Avro records per partition per fetch), keeping cross-partition skew well within our `5s` watermark bound and producing a **$0.00-delta** replay (`$213,693.70 == $213,693.70`).

---

### 6. When you `SIGKILL` an active TaskManager mid-window under 300 eps load, why doesn't ClickHouse double-count the half-processed records?

**Answer:**  
Three mechanisms work together across the failure boundary:
1. **Uncommitted Kafka sink transactions are aborted:** Records emitted by the killed TaskManager since the last completed `2,000 ms` checkpoint (`chk-N`) were written to Kafka inside an uncommitted transaction. When the standby TaskManager recovers `chk-N` from MinIO S3, Flink fences the old producer epoch using its deterministic `transactional-id-prefix` and aborts those in-flight transactional batches.
2. **ClickHouse enforces `read_committed`:** Configured in `clickhouse/config.d/kafka.xml`, ClickHouse's `librdkafka` consumer ignores aborted batches completely (proven in `make freshness`, where `5` aborted records leaked under `read_uncommitted` vs `0` under `read_committed`).
3. **Deterministic `ReplacingMergeTree` keys:** Even if a downstream consumer group is manually rewound or replayed, ClickHouse deduplicates rows on `(window_start, window_end)`, `(user_id, session_start)`, and `(event_id)` using `updated_at_ms`.

---

### 7. When can you upgrade a Flink job from a savepoint (Demo 3), and when does a savepoint upgrade break?

**Answer:**  
Savepoint upgrades succeed cleanly whenever **stateful operator UIDs, key types, and state schemas remain compatible**—which is why our Java DataStream job (`ClickstreamGuardAndFreshnessJob`) assigns explicit `.uid("guard-dedup-and-freshness")` on every stateful operator and uses POJO/primitive RocksDB `ValueState` descriptors. In Demo 3, upgrading `--query-version v1` to `v2` changed business tier thresholds (`VIP` $\rightarrow$ `PLATINUM`) and velocity flags while seamlessly resuming `user-canary`'s cumulative unit counter from event `#12` to `#13`.  

Savepoints **cannot** seamlessly bridge arbitrary topology rewrites inside compiled **Flink SQL `StatementSet` plans** (for example, adding a brand-new `JOIN` or changing `GROUP BY` keys), because Flink SQL's cost-based optimizer generates new internal operator hashes and composite state serializers (though Flink `1.19+` / `2.x` Compiled JSON Plans help preserve operator UIDs for minor SQL edits). When a SQL schema change alters grouping keys, we rely on **Demo 4 (Kappa earliest-offset replay)** into a `v2` sink table and atomically swap the Grafana view once caught up.

---

### 8. Why did you pick ClickHouse `ReplacingMergeTree` + `FINAL` over Apache Pinot, and at what scale would `FINAL` become a bottleneck?

**Answer:**  
For a 12-panel internal operational dashboard refreshing every `2 seconds`, our serving tables are compact pre-aggregated time series (`1,440` minute rows/day, `7,200` Top-5 product rows/day, `~50k` session rows/day). On ClickHouse `26.8`, `SELECT ... FINAL` merges uncompacted parts in parallel across cores in `< 5 ms`, while running in a single `~450 MB` binary instead of Pinot's 4-component + ZooKeeper JVM footprint (`ADR-0003`).  

`FINAL` starts to hurt when two conditions coincide: **(a) tables reach hundreds of millions of high-cardinality rows** (e.g., raw unaggregated clicks) **and (b) hundreds of external tenants issue thousands of concurrent QPS**. At that scale, we either partition tightly by `toYYYYMMDD(window_start)` with `do_not_merge_across_partitions_select_final = 1`, switch additive metrics to `SummingMergeTree` / `AggregatingMergeTree` (no `FINAL` needed), or adopt Apache Pinot's co-partitioned Realtime Upsert + Star-Tree indexes.

---

### 9. How does your `MATCH_RECOGNIZE` funnel query handle users browsing multiple products in separate browser tabs, or skipping straight from `view` to `purchase` ("Buy Now")?

**Answer:**  
Look at our `PATTERN (V O* A O* K O* P)` and `AFTER MATCH SKIP PAST LAST ROW` definition in `flink/sql/02_views.sql`:
- Defining `O AS O.event_type NOT IN ('add_to_cart', 'checkout', 'purchase')` between steps allows intermediate page refreshes, category searches, or secondary `view` events in other tabs without breaking an active checkout sequence, bounded within a `1-hour` `WITHIN` horizon (enforced via RocksDB state TTL).
- To measure **step-by-step funnel drop-off** accurately (not just completed 4-step purchases!), our `funnel_metrics_view` evaluates per-user reach across all four causal prefix states within each window (`reached_view`, `reached_cart` after `view`, `reached_checkout` after `cart`, and `reached_purchase` after `checkout`), so a shopper who abandons at `checkout` increments the first three funnel stages and surfaces exact `cart -> checkout` vs `checkout -> purchase` drop-off rates. If the storefront introduces a 1-click "Buy Now" flow (`V -> P`), we add an explicit `buy_now` event attribute or union `PATTERN (V O* P)` into a dedicated express-checkout funnel metric.

---

### 10. If traffic grew 10x beyond your peak design target—from 50,000 events/s to 500,000 events/s—what breaks first in this architecture, and how would you re-architect it?

**Answer:**  
Three bottlenecks hit in strict order as you scale toward `500,000 events/s` (`~77 MB/s` Avro ingress):
1. **Global `event_id` Deduplication RocksDB IOPS (`~1.8 billion` keys/hour):** Point-looking up every single click in RocksDB across `1.8B` UUIDs dominates disk read IOPS during LSM compaction. *Fix:* Replace exact RocksDB UUID retention beyond `5 minutes` with a **partitioned RocksDB Bloom filter** (or keep exact UUID dedup strictly on high-value `checkout`/`purchase` events while sampling `view` impressions).
2. **`COUNT(DISTINCT user_id)` in Overlapping Hopping Windows:** Exact distinct sets inside `HOP(1m, 5m)` replicate user keys across 5 window panes. *Fix:* Switch real-time active-user gauges to **Apache DataSketches HyperLogLog (`uniqTheta` / HLL)** (< 1.5% error in `4 KB` state per window instead of megabytes of distinct keys).
3. **Kafka Partition & S3 Checkpoint Scaling:** Expand `clickstream.raw.v1` from `36` to **`144` partitions** across `9–12` NVMe KRaft brokers (`zstd` compression saves `3.6x` cross-AZ replication bandwidth), enable **Flink Changelog State Backend (DSTL)** or raise checkpoints from `2s` to `5s` with S3 prefix sharding to avoid S3 `5,500 GET / 3,500 PUT per prefix` throttling, and run ClickHouse on a sharded Keeper cluster.
