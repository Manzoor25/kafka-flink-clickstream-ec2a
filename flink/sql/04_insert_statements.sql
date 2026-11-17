-- ============================================================================
-- 04_insert_statements.sql
-- Executed inside a single Flink StatementSet so all 5 sinks share one Kafka
-- source, one RocksDB deduplicator, and one temporal join state cache.
-- ============================================================================

INSERT INTO revenue_per_minute_sink SELECT * FROM revenue_per_minute_view;
INSERT INTO active_users_hopping_sink SELECT * FROM active_users_hopping_view;
INSERT INTO user_sessions_sink SELECT * FROM user_sessions_view;
INSERT INTO funnel_metrics_sink SELECT * FROM funnel_metrics_view;
INSERT INTO top_products_sink SELECT * FROM top_products_view;
