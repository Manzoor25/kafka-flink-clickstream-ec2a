-- ============================================================================
-- 03_sinks_kafka.sql
-- Transactional Kafka sinks configured with 2-phase commit (`exactly-once`)
-- coordinated with Flink checkpoints. Downstream consumers MUST configure
-- `isolation.level = read_committed` so uncommitted/aborted transactions
-- from TaskManager failovers are never observed.
-- ============================================================================

CREATE TABLE revenue_per_minute_sink (
    window_start_utc STRING,
    window_end_utc STRING,
    window_start_ms BIGINT,
    window_end_ms BIGINT,
    total_events BIGINT,
    page_views BIGINT,
    add_to_carts BIGINT,
    checkouts BIGINT,
    purchases BIGINT,
    active_users BIGINT,
    gross_revenue DOUBLE,
    avg_order_value DOUBLE,
    max_created_at_ms BIGINT,
    emitted_at_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.revenue_per_minute.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-rev-min',
    'format' = 'json',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE active_users_hopping_sink (
    window_start_utc STRING,
    window_end_utc STRING,
    window_start_ms BIGINT,
    window_end_ms BIGINT,
    active_users_5m BIGINT,
    total_events_5m BIGINT,
    purchases_5m BIGINT,
    rolling_revenue_5m DOUBLE,
    buyer_conversion_pct DOUBLE,
    max_created_at_ms BIGINT,
    emitted_at_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.active_users_hopping.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-hop-users',
    'format' = 'json'
);

CREATE TABLE user_sessions_sink (
    user_id STRING,
    session_start_utc STRING,
    session_end_utc STRING,
    session_start_ms BIGINT,
    session_end_ms BIGINT,
    duration_sec BIGINT,
    event_count BIGINT,
    page_views BIGINT,
    add_to_carts BIGINT,
    checkouts BIGINT,
    purchases BIGINT,
    session_revenue DOUBLE,
    converted INT,
    device_type STRING,
    country STRING,
    max_created_at_ms BIGINT,
    emitted_at_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.user_sessions.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-sessions',
    'format' = 'json'
);

CREATE TABLE funnel_metrics_sink (
    window_start_utc STRING,
    window_end_utc STRING,
    window_start_ms BIGINT,
    window_end_ms BIGINT,
    view_users BIGINT,
    cart_users BIGINT,
    checkout_users BIGINT,
    purchase_users BIGINT,
    view_to_cart_dropoff_pct DOUBLE,
    cart_to_checkout_dropoff_pct DOUBLE,
    checkout_to_purchase_dropoff_pct DOUBLE,
    overall_conversion_pct DOUBLE,
    funnel_revenue DOUBLE,
    max_created_at_ms BIGINT,
    emitted_at_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.funnel_metrics.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-funnel',
    'format' = 'json'
);

CREATE TABLE top_products_sink (
    window_start_utc STRING,
    window_end_utc STRING,
    window_start_ms BIGINT,
    window_end_ms BIGINT,
    rank_num BIGINT,
    product_id STRING,
    product_name STRING,
    category STRING,
    page_views BIGINT,
    cart_adds BIGINT,
    purchases BIGINT,
    product_revenue DOUBLE,
    unit_price_avg DOUBLE,
    max_created_at_ms BIGINT,
    emitted_at_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.top_products.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-top-prod',
    'format' = 'json'
);
