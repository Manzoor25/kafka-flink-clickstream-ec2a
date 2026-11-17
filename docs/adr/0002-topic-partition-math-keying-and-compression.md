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
- **Local Docker Compose sizing:** Scaled proportionally to **12 partitions** on both `clickstream.raw.v1` and compacted `product.catalog.v1` (4 leaders per broker across 3 brokers, evenly divisible by Flink parallelisms 2 and 4 so every parallel temporal-join subtask receives watermark advances) and **6 partitions** on derived analytics topics (`replication.factor=3`, `min.insync.replicas=2`).

### Partition Key Choice
- `product_id`: Rejected — top 1% of hero SKUs generate >30% of clicks during promotions, causing severe hot-partition lag and unbalanced RocksDB subtasks.
- `session_id`: Rejected — client session IDs can be missing or reset across tabs, whereas server-side sessionization (`SESSION` windows) requires all clicks of a `user_id` to arrive ordered per user.
- **`user_id` (Selected):** Distributes traffic uniformly across tens of thousands of active shoppers while preserving per-user Kafka partition ordering before watermarks and colocating per-user funnel/session keys.

## Decision

1. Use **12 partitions** (`RF=3`, `min.insync.replicas=2`) for `clickstream.raw.v1` locally (36 in production), keyed by **`user_id`**.
2. Default producer configuration to **`compression.type=lz4`**, **`linger.ms=20`**, **`batch.size=65536` (64 KiB)**, **`acks=all`**, **`enable.idempotence=true`**, validated empirically against `none` and `zstd` via `scripts/benchmark_producer.py`.

## Consequences

- **Positive:** Measured on our 3-broker Kafka `4.4.0` KRaft cluster (`docs/benchmarks/producer_benchmark.md`, 25,000 Avro events/run), `lz4` shrinks wire payload by **2.09x–2.25x** (`4,011 KB` uncompressed $\rightarrow$ `1,669 KB`) and reaches **126,544 events/s** (`18.54 MB/s`) at `linger.ms=50, batch.size=128 KiB`, while `zstd` (`linger.ms=20, batch.size=64 KiB`) shrinks wire payload by **3.62x** (`1,035 KB`) at **124,255 events/s** (`18.20 MB/s`, ACK `p50 = 124.51 ms`, `p95 = 168.62 ms`). Keeping `linger.ms=20` bounds producer batching delay to $\le 20\text{ ms}$ (< 0.6% of our measured `3.90 s` p50 dashboard freshness).
- **Negative:** Keying by `user_id` means anonymous bot floods sharing a synthetic `user_id` would skew one partition; production edge gateways mitigate this by salting unauthenticated bot traffic or rate-limiting at the CDN.
