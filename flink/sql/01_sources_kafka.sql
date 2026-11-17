-- ============================================================================
-- 01_sources_kafka.sql
-- Kafka sources with 5s bounded-out-of-orderness watermarks and upsert-kafka
-- compacted product catalogue table for event-time temporal joins.
-- Placeholders (${...}) are substituted by ClickstreamSqlPipelineJob.
-- ============================================================================

CREATE TABLE clickstream_raw (
    event_id STRING,
    user_id STRING,
    client_session_id STRING,
    event_type STRING,
    product_id STRING,
    quantity INT,
    page_url STRING,
    referrer STRING,
    device_type STRING,
    country STRING,
    event_time_ms BIGINT,
    created_at_ms BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
    proc_time AS PROCTIME(),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream.raw.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id' = '${CONSUMER_GROUP}',
    'scan.startup.mode' = '${SCAN_STARTUP_MODE}',
    'format' = 'avro-confluent',
    'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);

CREATE TABLE product_catalog (
    product_id STRING,
    product_name STRING,
    category STRING,
    price DOUBLE,
    currency STRING,
    updated_at_ms BIGINT,
    updated_ts AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
    WATERMARK FOR updated_ts AS updated_ts - INTERVAL '5' SECOND,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'product.catalog.v1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id' = '${CONSUMER_GROUP}-catalog',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);
