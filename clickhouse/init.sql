-- ============================================================================
-- ClickHouse 26.8 LTS Real-Time Serving Schema
-- Consumes Flink transactional Kafka topics (`isolation.level = read_committed`
-- via config.d/kafka.xml) into idempotent ReplacingMergeTree serving tables.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS clickstream;

-- ----------------------------------------------------------------------------
-- 1. Revenue & Volume per Minute (1m Tumbling Window)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.revenue_per_minute (
    window_time DateTime64(3, 'UTC'),
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    total_events UInt64,
    page_views UInt64,
    add_to_carts UInt64,
    checkouts UInt64,
    purchases UInt64,
    active_users UInt64,
    gross_revenue Float64,
    avg_order_value Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (window_start_ms);

CREATE TABLE IF NOT EXISTS clickstream.kafka_revenue_per_minute (
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    total_events UInt64,
    page_views UInt64,
    add_to_carts UInt64,
    checkouts UInt64,
    purchases UInt64,
    active_users UInt64,
    gross_revenue Float64,
    avg_order_value Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.revenue_per_minute.v1',
    kafka_group_name = 'ch-revenue-per-minute-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_revenue_per_minute
TO clickstream.revenue_per_minute AS
SELECT
    fromUnixTimestamp64Milli(window_start_ms, 'UTC') AS window_time,
    window_start_utc,
    window_end_utc,
    window_start_ms,
    window_end_ms,
    total_events,
    page_views,
    add_to_carts,
    checkouts,
    purchases,
    active_users,
    gross_revenue,
    avg_order_value,
    max_created_at_ms,
    emitted_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_revenue_per_minute;

-- ----------------------------------------------------------------------------
-- 2. Rolling 5-Minute Active Users & Buyer Conversion (5m Hopping, 1m Slide)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.active_users_hopping (
    window_time DateTime64(3, 'UTC'),
    window_end_time DateTime64(3, 'UTC'),
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    active_users_5m UInt64,
    total_events_5m UInt64,
    purchases_5m UInt64,
    rolling_revenue_5m Float64,
    buyer_conversion_pct Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (window_start_ms, window_end_ms);

CREATE TABLE IF NOT EXISTS clickstream.kafka_active_users_hopping (
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    active_users_5m UInt64,
    total_events_5m UInt64,
    purchases_5m UInt64,
    rolling_revenue_5m Float64,
    buyer_conversion_pct Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.active_users_hopping.v1',
    kafka_group_name = 'ch-active-users-hopping-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_active_users_hopping
TO clickstream.active_users_hopping AS
SELECT
    fromUnixTimestamp64Milli(window_start_ms, 'UTC') AS window_time,
    fromUnixTimestamp64Milli(window_end_ms, 'UTC') AS window_end_time,
    window_start_utc,
    window_end_utc,
    window_start_ms,
    window_end_ms,
    active_users_5m,
    total_events_5m,
    purchases_5m,
    rolling_revenue_5m,
    buyer_conversion_pct,
    max_created_at_ms,
    emitted_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_active_users_hopping;

-- ----------------------------------------------------------------------------
-- 3. User Sessions (30-Second Inactivity Gap Session Windows)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.user_sessions (
    session_time DateTime64(3, 'UTC'),
    user_id String,
    session_start_utc String,
    session_end_utc String,
    session_start_ms Int64,
    session_end_ms Int64,
    duration_sec UInt64,
    event_count UInt64,
    page_views UInt64,
    add_to_carts UInt64,
    checkouts UInt64,
    purchases UInt64,
    session_revenue Float64,
    converted UInt8,
    device_type String,
    country String,
    max_created_at_ms Int64,
    emitted_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (user_id, session_start_ms, session_end_ms);

CREATE TABLE IF NOT EXISTS clickstream.kafka_user_sessions (
    user_id String,
    session_start_utc String,
    session_end_utc String,
    session_start_ms Int64,
    session_end_ms Int64,
    duration_sec UInt64,
    event_count UInt64,
    page_views UInt64,
    add_to_carts UInt64,
    checkouts UInt64,
    purchases UInt64,
    session_revenue Float64,
    converted UInt8,
    device_type String,
    country String,
    max_created_at_ms Int64,
    emitted_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.user_sessions.v1',
    kafka_group_name = 'ch-user-sessions-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_user_sessions
TO clickstream.user_sessions AS
SELECT
    fromUnixTimestamp64Milli(session_start_ms, 'UTC') AS session_time,
    user_id,
    session_start_utc,
    session_end_utc,
    session_start_ms,
    session_end_ms,
    duration_sec,
    event_count,
    page_views,
    add_to_carts,
    checkouts,
    purchases,
    session_revenue,
    converted,
    device_type,
    country,
    max_created_at_ms,
    emitted_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_user_sessions;

-- ----------------------------------------------------------------------------
-- 4. Causal Conversion Funnel & Step Drop-Off % (MATCH_RECOGNIZE)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.funnel_metrics (
    window_time DateTime64(3, 'UTC'),
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    view_users UInt64,
    cart_users UInt64,
    checkout_users UInt64,
    purchase_users UInt64,
    view_to_cart_dropoff_pct Float64,
    cart_to_checkout_dropoff_pct Float64,
    checkout_to_purchase_dropoff_pct Float64,
    overall_conversion_pct Float64,
    funnel_revenue Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (window_start_ms);

CREATE TABLE IF NOT EXISTS clickstream.kafka_funnel_metrics (
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    view_users UInt64,
    cart_users UInt64,
    checkout_users UInt64,
    purchase_users UInt64,
    view_to_cart_dropoff_pct Float64,
    cart_to_checkout_dropoff_pct Float64,
    checkout_to_purchase_dropoff_pct Float64,
    overall_conversion_pct Float64,
    funnel_revenue Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.funnel_metrics.v1',
    kafka_group_name = 'ch-funnel-metrics-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_funnel_metrics
TO clickstream.funnel_metrics AS
SELECT
    fromUnixTimestamp64Milli(window_start_ms, 'UTC') AS window_time,
    window_start_utc,
    window_end_utc,
    window_start_ms,
    window_end_ms,
    view_users,
    cart_users,
    checkout_users,
    purchase_users,
    view_to_cart_dropoff_pct,
    cart_to_checkout_dropoff_pct,
    checkout_to_purchase_dropoff_pct,
    overall_conversion_pct,
    funnel_revenue,
    max_created_at_ms,
    emitted_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_funnel_metrics;

-- ----------------------------------------------------------------------------
-- 5. Top-N Products per Window (Window Rank Top-10 + Temporal Catalogue Price)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.top_products (
    window_time DateTime64(3, 'UTC'),
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    rank_num UInt64,
    product_id String,
    product_name String,
    category String,
    page_views UInt64,
    cart_adds UInt64,
    purchases UInt64,
    product_revenue Float64,
    unit_price_avg Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (window_start_ms, rank_num, product_id);

CREATE TABLE IF NOT EXISTS clickstream.kafka_top_products (
    window_start_utc String,
    window_end_utc String,
    window_start_ms Int64,
    window_end_ms Int64,
    rank_num UInt64,
    product_id String,
    product_name String,
    category String,
    page_views UInt64,
    cart_adds UInt64,
    purchases UInt64,
    product_revenue Float64,
    unit_price_avg Float64,
    max_created_at_ms Int64,
    emitted_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.top_products.v1',
    kafka_group_name = 'ch-top-products-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_top_products
TO clickstream.top_products AS
SELECT
    fromUnixTimestamp64Milli(window_start_ms, 'UTC') AS window_time,
    window_start_utc,
    window_end_utc,
    window_start_ms,
    window_end_ms,
    rank_num,
    product_id,
    product_name,
    category,
    page_views,
    cart_adds,
    purchases,
    product_revenue,
    unit_price_avg,
    max_created_at_ms,
    emitted_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_top_products;

-- ----------------------------------------------------------------------------
-- 6. End-to-End Freshness Probes & Stateful Shopper Loyalty (DataStream API Job)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.freshness_probes (
    created_time DateTime64(3, 'UTC'),
    event_id String,
    user_id String,
    event_type String,
    product_id String,
    quantity Int32,
    country String,
    device_type String,
    event_time_ms Int64,
    created_at_ms Int64,
    flink_processed_ms Int64,
    flink_latency_ms Int64,
    ingested_at_ms UInt64,
    e2e_freshness_ms Int64,
    user_cumulative_events UInt64,
    user_cumulative_purchase_units UInt64,
    query_version String,
    loyalty_tier String,
    high_velocity_session Bool
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (event_id);

CREATE TABLE IF NOT EXISTS clickstream.kafka_freshness_probes (
    event_id String,
    user_id String,
    event_type String,
    product_id String,
    quantity Int32,
    country String,
    device_type String,
    event_time_ms Int64,
    created_at_ms Int64,
    flink_processed_ms Int64,
    flink_latency_ms Int64,
    user_cumulative_events UInt64,
    user_cumulative_purchase_units UInt64,
    query_version String,
    loyalty_tier String,
    high_velocity_session Bool
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'analytics.freshness_probes.v1',
    kafka_group_name = 'ch-freshness-probes-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_freshness_probes
TO clickstream.freshness_probes AS
WITH toUInt64(toUnixTimestamp64Milli(now64(3))) AS now_ms
SELECT
    fromUnixTimestamp64Milli(created_at_ms, 'UTC') AS created_time,
    event_id,
    user_id,
    event_type,
    product_id,
    quantity,
    country,
    device_type,
    event_time_ms,
    created_at_ms,
    flink_processed_ms,
    flink_latency_ms,
    now_ms AS ingested_at_ms,
    greatest(toInt64(0), toInt64(now_ms) - created_at_ms) AS e2e_freshness_ms,
    user_cumulative_events,
    user_cumulative_purchase_units,
    query_version,
    loyalty_tier,
    high_velocity_session
FROM clickstream.kafka_freshness_probes;

-- ----------------------------------------------------------------------------
-- 7. Late-Event Audit Stream (> Watermark - Allowed Lateness Side Output)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickstream.late_events (
    routed_time DateTime64(3, 'UTC'),
    event_id String,
    user_id String,
    event_type String,
    product_id String,
    quantity Int32,
    event_time_ms Int64,
    watermark_ms Int64,
    lateness_ms Int64,
    routed_at_ms Int64,
    ingested_at_ms UInt64
) ENGINE = ReplacingMergeTree(ingested_at_ms)
ORDER BY (event_id);

CREATE TABLE IF NOT EXISTS clickstream.kafka_late_events (
    event_id String,
    user_id String,
    event_type String,
    product_id String,
    quantity Int32,
    event_time_ms Int64,
    watermark_ms Int64,
    lateness_ms Int64,
    routed_at_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'clickstream.late.v1',
    kafka_group_name = 'ch-late-events-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS clickstream.mv_late_events
TO clickstream.late_events AS
SELECT
    fromUnixTimestamp64Milli(routed_at_ms, 'UTC') AS routed_time,
    event_id,
    user_id,
    event_type,
    product_id,
    quantity,
    event_time_ms,
    watermark_ms,
    lateness_ms,
    routed_at_ms,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS ingested_at_ms
FROM clickstream.kafka_late_events;
