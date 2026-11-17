# Kafka 4.4.0 KRaft Producer Compression & Batching Benchmark

- **Cluster**: 3 KRaft combined `broker,controller` nodes (`apache/kafka:4.4.0`), 12 partitions, `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, `enable.idempotence=true`
- **Payload**: `25,000` Confluent-Avro `ClickstreamEvent` records per run (keyed by `user_id`)

| Codec | `linger.ms` | `batch.size` (B) | Throughput (events/s) | Uncompressed (MB/s) | Wire TX (KB) | Compression Ratio | Avg Msgs/Batch | ACK p50 (ms) | ACK p95 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 0 | 16,384 | **26,821** | 3.93 | 4,011.1 | **0.93x** | 69.0 | 654.78 | 869.40 |
| `none` | 20 | 65,536 | **65,911** | 9.66 | 3,999.2 | **0.94x** | 269.0 | 261.20 | 351.94 |
| `lz4` | 0 | 16,384 | **19,530** | 2.86 | 1,794.7 | **2.09x** | 89.0 | 953.70 | 1192.50 |
| `lz4` | 5 | 32,768 | **69,951** | 10.25 | 1,720.1 | **2.18x** | 172.0 | 252.51 | 328.63 |
| `lz4` | 20 | 65,536 | **40,989** | 6.00 | 1,673.3 | **2.24x** | 194.0 | 562.97 | 586.81 |
| `lz4` | 50 | 131,072 | **126,544** | 18.54 | 1,669.0 | **2.25x** | 694.0 | 164.72 | 177.33 |
| `zstd` | 0 | 16,384 | **88,205** | 12.92 | 1,100.8 | **3.41x** | 93.0 | 173.68 | 250.81 |
| `zstd` | 5 | 32,768 | **28,574** | 4.19 | 1,052.0 | **3.57x** | 163.0 | 685.44 | 808.99 |
| `zstd` | 20 | 65,536 | **124,255** | 18.20 | 1,035.0 | **3.62x** | 0.0 | 124.51 | 168.62 |
| `zstd` | 50 | 131,072 | **45,312** | 6.64 | 1,034.3 | **3.63x** | 694.0 | 529.90 | 535.50 |
