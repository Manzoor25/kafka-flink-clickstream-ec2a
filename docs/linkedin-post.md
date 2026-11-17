Your Flink operator processes a click in 216 ms—so why does a strictly accurate real-time dashboard still show 3.90 s p50 freshness?

Because end-to-end "exactly-once" has a physics floor: under Kafka two-phase commit, downstream read_committed consumers cannot advance past the Last Stable Offset until Flink completes its RocksDB checkpoint to S3 and commits the transaction.

I built an open-source, one-command clickstream analytics stack (make up && make demo) on Kafka 4.4.0 (3-broker KRaft), Flink 2.2.1 (SQL + DataStream API, 2s RocksDB checkpoints on MinIO S3), Karapace Avro Registry, ClickHouse 26.8 LTS, and Grafana 13.3.1:

• 3.90 s p50 / 5.77 s p95 freshness from click creation to Grafana query (300 probes)
• 3.62x wire compression at 124,255 events/s with zstd (linger.ms=20, 64 KiB batches)
• FOR SYSTEM_TIME AS OF price joins, MATCH_RECOGNIZE funnels, sessions & Top-N SKUs
• Chaos-verified: SIGKILL active TaskManager, 10x spike (8,278 eps), S3 savepoint v1->v2 upgrade, and earliest-offset Kappa replay—all with 0 missing events and $0.00 batch-recount drift.

Repo + ADRs + benchmarks: https://github.com/Manzoor25/kafka-flink-clickstream-ec2a

#DataEngineering #ApacheKafka #ApacheFlink #ClickHouse #StreamProcessing
