-- ============================================================================
-- 02_views.sql
-- Connector-agnostic Flink SQL transformation views shared identically between
-- the live Kafka pipeline and deterministic fixed-file JUnit tests.
-- Requires `clickstream_raw` (with event_ts ROWTIME + proc_time PROCTIME)
-- and versioned `product_catalog` (PRIMARY KEY product_id, updated_ts ROWTIME).
-- ============================================================================

-- 1. Stateful Deduplication by event_id (RocksDB state bounded by table.exec.state.ttl)
-- Keeping the first arrival per event_id produces an append-only stream while
-- preserving the event_ts rowtime watermark attribute for downstream windows,
-- temporal joins, and MATCH_RECOGNIZE pattern matching.
CREATE TEMPORARY VIEW deduplicated_clicks AS
SELECT
    event_id,
    user_id,
    client_session_id,
    event_type,
    product_id,
    quantity,
    page_url,
    referrer,
    device_type,
    country,
    event_time_ms,
    created_at_ms,
    event_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY proc_time ASC
        ) AS row_num
    FROM clickstream_raw
)
WHERE row_num = 1;

-- 2. Event-Time Temporal Join against Compacted Product Catalogue (upsert-kafka)
-- Each click resolves the exact product price valid at `c.event_ts` (even if
-- the catalogue price later changed during a flash sale or the click arrived late).
CREATE TEMPORARY VIEW enriched_clicks AS
SELECT
    c.event_id,
    c.user_id,
    c.client_session_id,
    c.event_type,
    c.product_id,
    COALESCE(p.product_name, c.product_id) AS product_name,
    COALESCE(p.category, 'Unknown') AS category,
    COALESCE(p.price, 0.0) AS unit_price,
    COALESCE(p.currency, 'USD') AS currency,
    c.quantity,
    CASE
        WHEN c.event_type = 'purchase' THEN ROUND(CAST(c.quantity AS DOUBLE) * COALESCE(p.price, 0.0), 2)
        ELSE 0.0
    END AS line_revenue,
    c.page_url,
    c.referrer,
    c.device_type,
    c.country,
    c.event_time_ms,
    c.created_at_ms,
    c.event_ts
FROM deduplicated_clicks AS c
LEFT JOIN product_catalog FOR SYSTEM_TIME AS OF c.event_ts AS p
    ON c.product_id = p.product_id;

-- 3. 1-Minute Tumbling Window Metrics (Revenue per Minute, Orders, Active Users)
CREATE TEMPORARY VIEW revenue_per_minute_view AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start_utc,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end_utc,
    UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_start_ms,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_end_ms,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
    COUNT(*) FILTER (WHERE event_type = 'checkout') AS checkouts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    COUNT(DISTINCT user_id) AS active_users,
    ROUND(SUM(line_revenue), 2) AS gross_revenue,
    CASE
        WHEN COUNT(*) FILTER (WHERE event_type = 'purchase') > 0
        THEN ROUND(SUM(line_revenue) / CAST(COUNT(*) FILTER (WHERE event_type = 'purchase') AS DOUBLE), 2)
        ELSE 0.0
    END AS avg_order_value,
    MAX(created_at_ms) AS max_created_at_ms,
    UNIX_TIMESTAMP() * 1000 AS emitted_at_ms
FROM TABLE(
    TUMBLE(TABLE enriched_clicks, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;

-- 4. 5-Minute Hopping Window (1-Minute Slide) for Rolling Active Users & Conversion
CREATE TEMPORARY VIEW active_users_hopping_view AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start_utc,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end_utc,
    UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_start_ms,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_end_ms,
    COUNT(DISTINCT user_id) AS active_users_5m,
    COUNT(*) AS total_events_5m,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases_5m,
    ROUND(SUM(line_revenue), 2) AS rolling_revenue_5m,
    ROUND(
        100.0 * CAST(COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id ELSE NULL END) AS DOUBLE)
        / CAST(GREATEST(COUNT(DISTINCT user_id), CAST(1 AS BIGINT)) AS DOUBLE),
        2
    ) AS buyer_conversion_pct,
    MAX(created_at_ms) AS max_created_at_ms,
    UNIX_TIMESTAMP() * 1000 AS emitted_at_ms
FROM TABLE(
    HOP(TABLE enriched_clicks, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end;

-- 5. User Sessionisation with Event-Time Session Windows (30-second inactivity gap)
CREATE TEMPORARY VIEW user_sessions_view AS
SELECT
    user_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS session_start_utc,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS session_end_utc,
    UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS session_start_ms,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS session_end_ms,
    GREATEST(
        UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss'))
        - UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) - 30,
        CAST(0 AS BIGINT)
    ) AS duration_sec,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
    COUNT(*) FILTER (WHERE event_type = 'checkout') AS checkouts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    ROUND(SUM(line_revenue), 2) AS session_revenue,
    CASE WHEN COUNT(*) FILTER (WHERE event_type = 'purchase') > 0 THEN 1 ELSE 0 END AS converted,
    MAX(device_type) AS device_type,
    MAX(country) AS country,
    MAX(created_at_ms) AS max_created_at_ms,
    UNIX_TIMESTAMP() * 1000 AS emitted_at_ms
FROM TABLE(
    SESSION(TABLE enriched_clicks PARTITION BY user_id, DESCRIPTOR(event_ts), INTERVAL '30' SECOND)
)
GROUP BY user_id, window_start, window_end;

-- 6. Causal Sequential Funnel Progression via MATCH_RECOGNIZE + Drop-Off per Step
-- Each step k>1 is emitted ONLY when the shopper completes steps 1..k in strict
-- event-time sequence within 15 minutes, guaranteeing monotonic step counts.
CREATE TEMPORARY VIEW funnel_step_events AS
SELECT user_id, 'step1_view' AS step_name, event_ts, created_at_ms, 0.0 AS step_revenue
FROM enriched_clicks
WHERE event_type = 'page_view'

UNION ALL

SELECT user_id, 'step2_cart' AS step_name, step_ts AS event_ts, created_at_ms, 0.0 AS step_revenue
FROM enriched_clicks
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY event_ts
    MEASURES
        MATCH_ROWTIME() AS step_ts,
        C.created_at_ms AS created_at_ms
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V X*? C) WITHIN INTERVAL '15' MINUTE
    DEFINE
        V AS V.event_type = 'page_view',
        X AS X.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart'
)

UNION ALL

SELECT user_id, 'step3_checkout' AS step_name, step_ts AS event_ts, created_at_ms, 0.0 AS step_revenue
FROM enriched_clicks
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY event_ts
    MEASURES
        MATCH_ROWTIME() AS step_ts,
        K.created_at_ms AS created_at_ms
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V X*? C Y*? K) WITHIN INTERVAL '15' MINUTE
    DEFINE
        V AS V.event_type = 'page_view',
        X AS X.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart',
        Y AS Y.event_type <> 'checkout',
        K AS K.event_type = 'checkout'
)

UNION ALL

SELECT user_id, 'step4_purchase' AS step_name, step_ts AS event_ts, created_at_ms, order_revenue AS step_revenue
FROM enriched_clicks
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY event_ts
    MEASURES
        MATCH_ROWTIME() AS step_ts,
        P.created_at_ms AS created_at_ms,
        P.line_revenue AS order_revenue
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V X*? C Y*? K Z*? P) WITHIN INTERVAL '15' MINUTE
    DEFINE
        V AS V.event_type = 'page_view',
        X AS X.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart',
        Y AS Y.event_type <> 'checkout',
        K AS K.event_type = 'checkout',
        Z AS Z.event_type <> 'purchase',
        P AS P.event_type = 'purchase'
);

CREATE TEMPORARY VIEW funnel_metrics_view AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start_utc,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end_utc,
    UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_start_ms,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_end_ms,
    view_users,
    cart_users,
    checkout_users,
    purchase_users,
    ROUND(100.0 * CAST(GREATEST(view_users - cart_users, CAST(0 AS BIGINT)) AS DOUBLE) / CAST(GREATEST(view_users, CAST(1 AS BIGINT)) AS DOUBLE), 2) AS view_to_cart_dropoff_pct,
    ROUND(100.0 * CAST(GREATEST(cart_users - checkout_users, CAST(0 AS BIGINT)) AS DOUBLE) / CAST(GREATEST(cart_users, CAST(1 AS BIGINT)) AS DOUBLE), 2) AS cart_to_checkout_dropoff_pct,
    ROUND(100.0 * CAST(GREATEST(checkout_users - purchase_users, CAST(0 AS BIGINT)) AS DOUBLE) / CAST(GREATEST(checkout_users, CAST(1 AS BIGINT)) AS DOUBLE), 2) AS checkout_to_purchase_dropoff_pct,
    ROUND(100.0 * CAST(purchase_users AS DOUBLE) / CAST(GREATEST(view_users, CAST(1 AS BIGINT)) AS DOUBLE), 2) AS overall_conversion_pct,
    ROUND(funnel_revenue, 2) AS funnel_revenue,
    max_created_at_ms,
    UNIX_TIMESTAMP() * 1000 AS emitted_at_ms
FROM (
    SELECT
        window_start,
        window_end,
        COUNT(DISTINCT CASE WHEN step_name = 'step1_view' THEN user_id ELSE NULL END) AS view_users,
        COUNT(DISTINCT CASE WHEN step_name = 'step2_cart' THEN user_id ELSE NULL END) AS cart_users,
        COUNT(DISTINCT CASE WHEN step_name = 'step3_checkout' THEN user_id ELSE NULL END) AS checkout_users,
        COUNT(DISTINCT CASE WHEN step_name = 'step4_purchase' THEN user_id ELSE NULL END) AS purchase_users,
        SUM(step_revenue) AS funnel_revenue,
        MAX(created_at_ms) AS max_created_at_ms
    FROM TABLE(
        TUMBLE(TABLE funnel_step_events, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end
);

-- 7. Top-N Products per 1-Minute Window (Window Rank Top-10)
CREATE TEMPORARY VIEW top_products_view AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start_utc,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end_utc,
    UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_start_ms,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS window_end_ms,
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
    UNIX_TIMESTAMP() * 1000 AS emitted_at_ms
FROM (
    SELECT
        window_start,
        window_end,
        product_id,
        COALESCE(MAX(CASE WHEN product_name <> product_id THEN product_name ELSE NULL END), product_id) AS product_name,
        COALESCE(MAX(CASE WHEN category <> 'Unknown' THEN category ELSE NULL END), 'Unknown') AS category,
        COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_adds,
        COALESCE(SUM(CAST(quantity AS BIGINT)) FILTER (WHERE event_type = 'purchase'), CAST(0 AS BIGINT)) AS purchases,
        ROUND(SUM(line_revenue), 2) AS product_revenue,
        ROUND(AVG(unit_price), 2) AS unit_price_avg,
        MAX(created_at_ms) AS max_created_at_ms,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end
            ORDER BY
                SUM(line_revenue) DESC,
                COUNT(*) FILTER (WHERE event_type = 'page_view') DESC,
                product_id ASC
        ) AS rank_num
    FROM TABLE(
        TUMBLE(TABLE enriched_clicks, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, product_id
)
WHERE rank_num <= 10;
