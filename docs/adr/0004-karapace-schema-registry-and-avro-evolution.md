# ADR-0004: Karapace 6.2.4 (Apache-2.0) for Schema Registry and Avro Wire Governance

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Raw clickstream events (`clickstream.raw.v1`) and compacted product catalogue updates (`product.catalog.v1`) require schema enforcement, compact binary serialization, and compatibility checks so producer changes cannot silently break stateful Flink SQL jobs.

We evaluated three registries:
1. `confluentinc/cp-schema-registry`: Governed by the Confluent Community License (non-OSI, source-available restriction), conflicting with a pure Apache-2.0 portfolio stack.
2. `apicurio/apicurio-registry`: Apache-2.0, exposes Confluent compatibility under `/apis/ccompat/v7` sub-path.
3. **`ghcr.io/aiven-open/karapace:6.2.4` (Karapace by Aiven):** Pure **Apache-2.0**, stateless Python/FastAPI service storing schemas directly in Kafka's compacted `_schemas` topic (`replication_factor=3`), and serving the exact Confluent Schema Registry REST API at root (`/subjects`, `/schemas/ids/{id}`).

## Decision

1. Deploy **`ghcr.io/aiven-open/karapace:6.2.4`** backed by our 3-broker Kafka 4.4.0 KRaft cluster.
2. Encode `clickstream.raw.v1` and `product.catalog.v1` in **Confluent-framed Avro** (`0x00` magic byte + 4-byte big-endian schema ID + binary Avro payload) consumed by Flink SQL via `'format' = 'avro-confluent'` (`flink-sql-avro-confluent-registry:2.2.1`) and `upsert-kafka` `'value.format' = 'avro-confluent'`.
3. Enforce `BACKWARD` compatibility on registered subjects (`schemas/*.avsc`).
4. Emit downstream pre-aggregated analytics topics (`analytics.*.v1`) in UTF-8 JSON with explicit schemas so ClickHouse `Kafka` engine tables, CLI debuggers, and failure-demo verifiers can inspect committed transactions directly without coupled registry lookups.

## Consequences

- **Positive:** 100% Apache-2.0 stack; ~60% smaller raw event payloads vs JSON; schema compatibility validated automatically on startup; downstream analytics sinks remain trivially inspectable during ops & failure drills.
- **Negative:** Requires bootstrapping schema registration before launching Flink jobs (automated in `scripts/register_schemas.py` during `make up`).
