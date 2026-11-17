# ADR-0002: Topic Partition Sizing, `user_id` Keying, and Compression/Batching Defaults

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Clickstream traffic is bursty (flash sales produce 10x spikes), exhibits heavy Zipf skew across hero products, and feeds stateful downstream operators grouped by user (`SESSION` windows and `MATCH_RECOGNIZE` funnels) as well as event-ID deduplication.

### Throughput & Partition Calculation
- **Design target:** 5,000 events/s steady state, **50,000 events/s peak (10x flash-sale spike)**; average Avro-encoded event ~260 B (JSON ~680 B) $\approx$ 13 MB/s Avro (34 MB/s JSON) ingress at peak.
- **Per-partition throughput budgets:**
  - Safe single-partition Flink stateful RocksDB consumer budget ($C_p$): ~1,500–4,500 events/s per subtask (accounting for RocksDB point lookups on dedup + temporal join + session timers).
  - Safe single-partition replicated KRaft producer budget ($P_p$ with `acks=all`, RF=3): ~10 MB/s.
- **Production formula:** $N_{\text{prod}} = \lceil 50{,}000 / 1{,}500 \rceil = 34 \rightarrow \mathbf{36\text{ partitions}}$ (evenly divisible by 3 brokers = 12 leaders/broker and Flink parallelisms 4, 6, 9, 12, 18, 36).
- **Local Docker Compose sizing:** Scaled proportionally to **12 partitions** on `clickstream.raw.v1` (4 leaders per broker across 3 brokers, evenly divisible by Flink parallelism 2 and 4) and **6 partitions** on compacted `product.catalog.v1` and derived analytics topics (`replication.factor=3`, `min.insync.replicas=2`).

### Partition Key Choice
- `product_id`: Rejected — top 1% of hero SKUs generate >30% of clicks during promotions, causing severe hot-partition lag and unbalanced RocksDB subtasks.
- `session_id`: Rejected — client session IDs can be missing or reset across tabs, whereas server-side sessionization (`SESSION` windows) requires all clicks of a `user_id` to arrive ordered per user.
- **`user_id` (Selected):** Distributes traffic uniformly across tens of thousands of active shoppers while preserving per-user Kafka partition ordering before watermarks and colocating per-user funnel/session keys.

## Decision

1. Use **12 partitions** (`RF=3`, `min.insync.replicas=2`) for `clickstream.raw.v1` locally (36 in production), keyed by **`user_id`**.
2. Default producer configuration to **`compression.type=lz4`**, **`linger.ms=20`**, **`batch.size=65536` (64 KiB)**, **`acks=all`**, **`enable.idempotence=true`**, validated empirically against `none` and `zstd` via `scripts/benchmark_producer.py`.

## Consequences

- **Positive:** `lz4` with `linger.ms=20` and `batch.size=64 KiB` achieves multi-fold wire/disk reduction with minimal CPU overhead and keeps batching delay at $\le 20\text{ ms}$ (less than 1% of our sub-few-second dashboard freshness budget). `zstd` achieves slightly smaller byte size on JSON/Avro batches at higher producer CPU cost and remains available for archival topics.
- **Negative:** Keying by `user_id` means anonymous bot floods sharing a synthetic `user_id` would skew one partition; production edge gateways mitigate this by salting unauthenticated bot traffic or rate-limiting at the CDN.
