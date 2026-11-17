# From click to dashboard in seconds: a real-time analytics stack on Kafka and Flink

Most "real-time" e-commerce pipelines look simple on a whiteboard until reality hits at 2:00 PM during a flash sale. Mobile clients retry timed-out requests and inject duplicate `event_id` payloads. Cellular connections buffer clicks and deliver them ten seconds out of order. Merchandising drops the price of a hero product by 20% mid-minute while shoppers are actively checking out. Then a stream-processing worker crashes mid-window—right as leadership is watching live revenue per minute.

I built an open-source, fully runnable real-time clickstream analytics platform on **Apache Kafka 4.4.0**, **Apache Flink 2.2.1**, **ClickHouse 26.8 LTS**, **Karapace Schema Registry 6.2.4**, and **Grafana 13.3.1** to solve those exact failure modes end to end on a single Docker Compose stack—and to measure where every millisecond actually goes.

---

## The Problem

An e-commerce business wants a live operational dashboard that is never more than a few seconds stale across five metrics: active users, shopper sessions, four-step causal funnel conversion (`view` to `add_to_cart` to `checkout` to `purchase`) with per-step drop-off, top-selling products per window, and gross revenue per minute.

Accuracy cannot be traded away for speed. Every click must pick up the catalogue price that was valid **at the exact event timestamp of the click**, duplicate retries must never inflate conversion or revenue, late events beyond the watermark policy must be audited rather than silently discarded, and killing a worker node under load must leave streaming totals matching an offline batch recount to the exact cent.

---

## The Design

I structured the stack around four explicit architectural boundaries, each documented in its own ADR in the repository:

1. **ZooKeeper-free Kafka 4.4.0 KRaft & schema-governed Avro ingress.** Three combined `broker,controller` KRaft nodes run with `replication.factor=3`, `min.insync.replicas=2`, and `acks=all`. A Python generator produces realistic shopper personas with Zipf-skewed product popularity, 3% duplicate retries, 6% within-watermark jitter, 2% >10-second late arrivals, 4x–10x flash-sale bursts, and mid-stream catalogue price changes, serialized in Confluent-compatible binary Avro (~154 bytes/event) via Karapace `6.2.4`. We key `clickstream.raw.v1` (12 local partitions; sized to 36 partitions for a 50,000 events/s peak budget) by `user_id` rather than `product_id`, preventing celebrity-SKU hot partitions while keeping per-user clicks ordered for session and funnel operators.
2. **Hybrid Flink 2.2.1 compute (SQL `StatementSet` + Java DataStream API) on RocksDB.** Rather than running six isolated SQL jobs that each duplicate Kafka consumers and state, Job 1 compiles all six relational pipelines—`PROCTIME` event deduplication, `FOR SYSTEM_TIME AS OF` temporal join against compacted `product.catalog.v1`, 1-minute tumbling windows, 5-minute hopping windows, 30-second inactivity `SESSION` windows, `MATCH_RECOGNIZE` causal funnels, and `Top-5` product rankings—into a single shared `StatementSet`. Job 2 uses the Java DataStream API (`KeyedProcessFunction` + RocksDB `ValueState` with 15-minute TTL) to emit millisecond freshness probes, track cumulative shopper spend tiers across savepoint upgrades, and route events arriving more than 10 seconds behind max event time to a `clickstream.late.v1` dead-letter topic via an `OutputTag` side output. Both jobs snapshot incremental RocksDB SSTs every `2,000 ms` to S3-compatible MinIO storage (`cgr.dev/chainguard/minio` pinned by digest).
3. **End-to-end effective-once delivery.** Flink writes back to Kafka using `EXACTLY_ONCE` two-phase-commit (`2PC`) transactional sinks. Downstream, ClickHouse `26.8` consumes via its built-in Kafka Engine configured explicitly with `isolation_level = read_committed` (`kafka_flush_interval_ms = 500`) and materializes into `ReplacingMergeTree(updated_at_ms)` tables queried with `FINAL` by a provisioned 12-panel Grafana `13.3.1` dashboard (`2s` auto-refresh).

---

## The One Thing That Surprised Me

What surprised me most came when I built Failure Demo 4: truncating every ClickHouse table to `$0.00` and replaying Kafka from `earliest-offset` to prove a pure Kappa architecture could rebuild identical historical metrics without a separate batch layer.

In live streaming at 250–800 events/s, our streaming totals matched the offline batch recount with zero diff. Yet on my first 100x-speed `earliest-offset` replay (`parallelism = 2` reading `12` partitions, or 6 partitions per subtask), the rebuilt revenue came back slightly short! Digging into Flink's operator metrics revealed why: with Kafka's default `max.partition.fetch.bytes = 1 MiB`, a single poll drained the entire multi-minute backlog of Partition 0 before touching Partition 1. Because Flink SQL inserts `WatermarkAssigner` downstream of the source projection (`PROCTIME()`), Partition 0 pushed the subtask watermark four minutes into the future in milliseconds—causing early historical clicks sitting in Partitions 1 through 5 to be discarded as "late" against Partition 0's watermark.

Setting `'properties.max.partition.fetch.bytes' = '4096'` and `'properties.max.poll.records' = '200'` forced fine-grained round-robin interleaving (~25 Avro events per partition per fetch). Re-running the wipe-and-replay rebuilt `$213,693.70` of revenue, `598` sessions, and `10,134` unique events penny-for-penny (`$0.00` delta across every closed window).

---

## Measured Results

Every number below comes directly from automated harnesses committed in the repo (`make benchmark`, `make freshness`, and `make demo-*`):

- **Producer compression & batching (25,000 Avro records/run, `acks=all`, `RF=3`):** Uncompressed `none` (`linger.ms=0, batch=16 KiB`) achieved `26,821 events/s` and expanded wire bytes to `4,011.1 KB` (`0.93x` due to batch headers). `lz4` (`linger.ms=50, batch=128 KiB`) reached **`126,544 events/s`** (`18.54 MB/s`) and shrank wire bytes by **`2.25x`** (`1,669.0 KB`). `zstd` (`linger.ms=20, batch=64 KiB`) reached **`124,255 events/s`** (`18.20 MB/s`, ACK `p50 = 124.51 ms`, `p95 = 168.62 ms`) while shrinking wire bytes by **`3.62x`** (`1,035.0 KB`).
- **End-to-end freshness (300 live probes polled via Grafana `/api/ds/query` every 150 ms):** Event creation to Flink operator took **`p50 = 216 ms`** (`p95 = 1,686 ms`). Waiting for Flink's `2,000 ms` 2PC checkpoint commit plus ClickHouse `read_committed` ingestion brought ClickHouse visibility to **`p50 = 3.74 s`** (`p95 = 5.49 s`), and full **Event Creation to Grafana UI query visibility** measured **`p50 = 3.90 s` (`3,902 ms`)** and **`p95 = 5.77 s` (`5,766 ms`)** (`max = 6.36 s`). In our transactional isolation test (`5` committed + `5` aborted records), `read_uncommitted` leaked all `5` aborted records (`10` read) while `read_committed` leaked **`0`** (`5` read).
- **RocksDB state & chaos drills:** Incremental checkpoints after ~45,800 events measured `17.98 MB` (SQL job) and `1.54 MB` (DataStream job). Killing an active TaskManager (`SIGKILL`) at 300 eps restored from MinIO S3 in ~9 seconds with **`0` missing and `0` extra events** (`10,134 / 10,134`). A **10x spike** (`800` to `8,278 eps`) pushed SQL lag to `84,000` and Flink `backpressureRatio` to `0.49` (`busyRatio = 0.57`) before draining to `0` (`98,110 / 98,110`). Upgrading `v1` to `v2` via an S3 savepoint across an offline gap preserved cumulative RocksDB user state (`98,126 / 98,126`).

---

## What Was Not Measured

To be plain about lab boundaries: all 10 containers ran on a single 16-vCPU, 32-GB RAM WSL2 Linux machine over a local Docker bridge network backed by local NVMe storage. I did **not** measure cross-AZ cloud network RTT on `acks=all` replication and real AWS S3 checkpoint `PUT` tail latency, multi-hour RocksDB compaction steady state at a sustained 50,000 events/s (`20+ GB` state), concurrent multi-user Grafana `FINAL` query load, or simultaneous multi-broker KRaft disk quorum loss.

The full source code, ADRs, JUnit MiniCluster tests, benchmark scripts, and one-command quickstart (`make up && make demo`) are available here: **https://github.com/Manzoor25/kafka-flink-clickstream-ec2a**
