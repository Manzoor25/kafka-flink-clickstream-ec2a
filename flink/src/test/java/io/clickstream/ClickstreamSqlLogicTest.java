package io.clickstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the production `flink/sql/02_views.sql` queries against deterministic fixed input files
 * (`fixtures/clicks.jsonl` and `fixtures/catalog.jsonl`), including:
 *   - Event deduplication by event_id
 *   - Point-in-time temporal join (`FOR SYSTEM_TIME AS OF`) across mid-minute catalogue price changes
 *   - 1-minute tumbling window revenue, order count, and distinct users
 *   - 30-second session windows splitting inactivity gaps
 *   - Causal sequential `MATCH_RECOGNIZE` funnel counts and step-by-step drop-off percentages
 *   - Window Top-N products ranking
 *   - Streaming vs independent batch recount exactness check
 */
class ClickstreamSqlLogicTest {

    private StreamTableEnvironment tEnv;
    private Path clicksPath;
    private Path catalogPath;

    @BeforeEach
    void setUp() throws Exception {
        URL clicksUrl = Objects.requireNonNull(getClass().getResource("/fixtures/clicks.jsonl"));
        URL catalogUrl = Objects.requireNonNull(getClass().getResource("/fixtures/catalog.jsonl"));
        clicksPath = Path.of(clicksUrl.toURI());
        catalogPath = Path.of(catalogUrl.toURI());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);

        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("table.local-time-zone", "UTC");

        // Bind fixed JSONL files to the exact table schemas expected by 02_views.sql
        String clicksUri = clicksPath.toUri().toString();
        String catalogUri = catalogPath.toUri().toString();

        tEnv.executeSql("""
                CREATE TEMPORARY TABLE clickstream_raw (
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
                    'connector' = 'filesystem',
                    'path' = '%s',
                    'format' = 'json'
                )
                """.formatted(clicksUri));

        // Upsert changelog over fixed catalog.jsonl using PRIMARY KEY + json format
        // Note: filesystem connector represents append files; to exercise PRIMARY KEY versioned table
        // temporal join (`FOR SYSTEM_TIME AS OF`), we expose `product_catalog` as a versioned view
        // deduplicated by `(product_id, updated_at)` just like upsert-kafka produces in production!
        tEnv.executeSql("""
                CREATE TEMPORARY TABLE product_catalog_file (
                    product_id STRING,
                    product_name STRING,
                    category STRING,
                    price DOUBLE,
                    currency STRING,
                    updated_at_ms BIGINT,
                    updated_ts AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
                    WATERMARK FOR updated_ts AS updated_ts - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = '%s',
                    'format' = 'json'
                )
                """.formatted(catalogUri));

        tEnv.executeSql("""
                CREATE TEMPORARY VIEW product_catalog AS
                SELECT
                    product_id,
                    product_name,
                    category,
                    price,
                    currency,
                    updated_at_ms,
                    updated_ts
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY product_id
                            ORDER BY updated_ts DESC
                        ) AS rn
                    FROM product_catalog_file
                )
                WHERE rn = 1
                """);

        // Load the exact production SQL views (`flink/sql/02_views.sql`)
        Path viewsFile = resolveSqlPath("02_views.sql");
        String viewsSql = SqlScriptUtils.loadSqlResourceOrFile(viewsFile.toString(), Map.of());
        SqlScriptUtils.executeDdlScript(tEnv, viewsSql);
    }

    @Test
    void deduplicatesByEventIdAndAppliesPointInTimeTemporalCatalogPrices() throws Exception {
        List<Row> enriched = collectRows(tEnv.executeSql(
                "SELECT event_id, user_id, event_type, product_id, unit_price, line_revenue FROM enriched_clicks"));

        // Input has 16 raw rows with evt-a1 repeated 3 times -> must yield exactly 14 unique events
        assertThat(enriched).hasSize(14);

        Map<String, Double> priceByEvent = new HashMap<>();
        for (Row row : enriched) {
            priceByEvent.put((String) row.getField(0), (Double) row.getField(4));
        }

        // evt-a4 happened at 1762000020000 (before 1762000030000 price cut) -> $1000.00
        assertThat(priceByEvent.get("evt-a4")).isCloseTo(1000.0, Offset.offset(0.01));
        // evt-e1 happened at 1762000045000 (after 1762000030000 price cut) -> $850.00
        assertThat(priceByEvent.get("evt-e1")).isCloseTo(850.0, Offset.offset(0.01));
    }

    @Test
    void tumblingWindowRevenueMatchesIndependentBatchRecountExactly() throws Exception {
        List<Row> windows = collectRows(tEnv.executeSql(
                "SELECT window_start_utc, total_events, active_users, purchases, gross_revenue FROM revenue_per_minute_view"));

        assertThat(windows).isNotEmpty();

        double streamingRevenue = windows.stream()
                .mapToDouble(r -> (Double) r.getField(4))
                .sum();
        long streamingEvents = windows.stream()
                .mapToLong(r -> (Long) r.getField(1))
                .sum();
        long streamingPurchases = windows.stream()
                .mapToLong(r -> (Long) r.getField(3))
                .sum();

        // Independent pure-Java batch recount over the raw JSONL files
        BatchSummary batch = computeIndependentBatchRecount(clicksPath, catalogPath);

        assertThat(streamingEvents).isEqualTo(batch.uniqueEvents());
        assertThat(streamingPurchases).isEqualTo(batch.purchases());
        assertThat(streamingRevenue).isCloseTo(batch.grossRevenue().doubleValue(), Offset.offset(0.01));
        // Specifically: $1000.00 (evt-a4) + $850.00 (evt-e1) + 2 * $50.00 (evt-e2) = $1950.00
        assertThat(streamingRevenue).isCloseTo(1950.00, Offset.offset(0.01));
    }

    @Test
    void sessionWindowsSplitUserSessionsAcrossThirtySecondInactivityGap() throws Exception {
        List<Row> allSessions = collectRows(tEnv.executeSql(
                "SELECT user_id, session_start_utc, event_count, converted, session_revenue FROM user_sessions_view"));
        List<Row> sessions = allSessions.stream()
                .filter(r -> "u-alice".equals(r.getField(0)))
                .sorted(Comparator.comparingLong(r -> (Long) r.getField(2)))
                .toList();

        // u-alice has 4 clicks between +5s..+20s (Session 1, converted=1, $1000.00)
        // then 55s gap (> 30s threshold) and 1 click at +75s (Session 2, converted=0, $0.00)
        // Because WindowAggregate(SESSION) runs in pure append-only mode (changelogMode=[I]),
        // exactly 2 final +I rows are emitted for u-alice when watermarks close each session.
        assertThat(sessions).hasSize(2);

        Row secondSession = sessions.get(0);
        Row firstSession = sessions.get(1);

        assertThat((Long) firstSession.getField(2)).isEqualTo(4L);
        assertThat((Integer) firstSession.getField(3)).isEqualTo(1);
        assertThat((Double) firstSession.getField(4)).isCloseTo(1000.0, Offset.offset(0.01));

        assertThat((Long) secondSession.getField(2)).isEqualTo(1L);
        assertThat((Integer) secondSession.getField(3)).isEqualTo(0);
        assertThat((Double) secondSession.getField(4)).isCloseTo(0.0, Offset.offset(0.01));
    }

    @Test
    void matchRecognizeFunnelComputesSequentialStepCountsAndDropOffRates() throws Exception {
        List<Row> funnels = collectRows(tEnv.executeSql("""
                SELECT
                    view_users,
                    cart_users,
                    checkout_users,
                    purchase_users,
                    view_to_cart_dropoff_pct,
                    cart_to_checkout_dropoff_pct,
                    checkout_to_purchase_dropoff_pct,
                    overall_conversion_pct
                FROM funnel_metrics_view
                WHERE view_users >= 4
                """));

        assertThat(funnels).hasSize(1);
        Row f = funnels.get(0);

        // First minute has 4 viewers (alice, bob, carol, dave):
        //   alice -> page_view, add_to_cart, checkout, purchase (reaches step 4)
        //   bob   -> page_view, add_to_cart, checkout           (reaches step 3)
        //   carol -> page_view, add_to_cart                     (reaches step 2)
        //   dave  -> page_view                                  (reaches step 1)
        // Note: erin only had direct purchase events without a page_view, proving MATCH_RECOGNIZE enforces causal order!
        assertThat((Long) f.getField(0)).isEqualTo(4L);
        assertThat((Long) f.getField(1)).isEqualTo(3L);
        assertThat((Long) f.getField(2)).isEqualTo(2L);
        assertThat((Long) f.getField(3)).isEqualTo(1L);

        assertThat((Double) f.getField(4)).isCloseTo(25.00, Offset.offset(0.01));
        assertThat((Double) f.getField(5)).isCloseTo(33.33, Offset.offset(0.01));
        assertThat((Double) f.getField(6)).isCloseTo(50.00, Offset.offset(0.01));
        assertThat((Double) f.getField(7)).isCloseTo(25.00, Offset.offset(0.01));
    }

    @Test
    void topProductsPerWindowRanksLaptopFirstByRevenue() throws Exception {
        List<Row> topProducts = collectRows(tEnv.executeSql("""
                SELECT rank_num, product_id, purchases, product_revenue
                FROM top_products_view
                WHERE product_revenue > 0
                """));

        assertThat(topProducts).hasSizeGreaterThanOrEqualTo(2);
        topProducts.sort(Comparator.comparingLong(r -> (Long) r.getField(0)));
        Row rank1 = topProducts.get(0);
        Row rank2 = topProducts.get(1);

        assertThat((Long) rank1.getField(0)).isEqualTo(1L);
        assertThat((String) rank1.getField(1)).isEqualTo("prod-laptop");
        assertThat((Double) rank1.getField(3)).isCloseTo(1850.00, Offset.offset(0.01));

        assertThat((Long) rank2.getField(0)).isEqualTo(2L);
        assertThat((String) rank2.getField(1)).isEqualTo("prod-mouse");
        assertThat((Double) rank2.getField(3)).isCloseTo(100.00, Offset.offset(0.01));
    }

    @Test
    void transactionalKafkaStatementSetCompilesPhysicalPlanCleanly() throws Exception {
        Map<String, String> vars = Map.of(
                "KAFKA_BOOTSTRAP", "kafka-1:9092",
                "SCHEMA_REGISTRY_URL", "http://schema-registry:8085",
                "SCAN_STARTUP_MODE", "earliest-offset",
                "CONSUMER_GROUP", "test-cg",
                "TXN_PREFIX", "test-txn"
        );
        String sinksSql = SqlScriptUtils.loadSqlResourceOrFile(resolveSqlPath("03_sinks_kafka.sql").toString(), vars);
        String insertsSql = SqlScriptUtils.loadSqlResourceOrFile(resolveSqlPath("04_insert_statements.sql").toString(), vars);

        SqlScriptUtils.executeDdlScript(tEnv, sinksSql);
        StatementSet statementSet = tEnv.createStatementSet();
        for (String stmt : SqlScriptUtils.splitStatements(insertsSql)) {
            statementSet.addInsertSql(stmt);
        }
        String plan = statementSet.explain(ExplainDetail.CHANGELOG_MODE);
        assertThat(plan).contains(
                "TemporalJoin",
                "Match",
                "WindowAggregate(groupBy=[user_id], window=[SESSION",
                "Sink(table=[default_catalog.default_database.revenue_per_minute_sink]");
        assertThat(plan).doesNotContain("changelogMode=[I,UB,UA]");
    }

    private static Path resolveSqlPath(String fileName) {
        Path p = Path.of("sql", fileName);
        if (!Files.exists(p)) {
            p = Path.of("flink", "sql", fileName);
        }
        return p;
    }

    private static List<Row> collectRows(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    private record BatchSummary(long uniqueEvents, long purchases, BigDecimal grossRevenue) {
    }

    private static BatchSummary computeIndependentBatchRecount(Path clicksFile, Path catalogFile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, NavigableMap<Long, BigDecimal>> priceTimeline = new HashMap<>();

        for (String line : Files.readAllLines(catalogFile)) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode n = mapper.readTree(line);
            priceTimeline
                    .computeIfAbsent(n.get("product_id").asText(), k -> new TreeMap<>())
                    .put(n.get("updated_at_ms").asLong(), new BigDecimal(n.get("price").asText()));
        }

        Set<String> seenEventIds = new HashSet<>();
        long purchases = 0L;
        BigDecimal grossRevenue = BigDecimal.ZERO;

        for (String line : Files.readAllLines(clicksFile)) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode e = mapper.readTree(line);
            String eventId = e.get("event_id").asText();
            if (!seenEventIds.add(eventId)) {
                continue;
            }
            if ("purchase".equals(e.get("event_type").asText())) {
                purchases++;
                String productId = e.get("product_id").asText();
                long ts = e.get("event_time_ms").asLong();
                int qty = e.get("quantity").asInt();
                Map.Entry<Long, BigDecimal> priceAtTime = priceTimeline.get(productId).floorEntry(ts);
                BigDecimal unitPrice = priceAtTime.getValue();
                grossRevenue = grossRevenue.add(unitPrice.multiply(BigDecimal.valueOf(qty)));
            }
        }
        return new BatchSummary(seenEventIds.size(), purchases, grossRevenue.setScale(2, RoundingMode.HALF_UP));
    }
}
