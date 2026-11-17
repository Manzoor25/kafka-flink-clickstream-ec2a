package io.clickstream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Job 1: Unified Flink SQL real-time clickstream analytics pipeline.
 *
 * Executes all 5 analytical queries inside one StatementSet:
 *   1. Deduplication by event_id + Temporal join on upsert-kafka product catalogue
 *   2. 1-minute tumbling window revenue, orders, and active users
 *   3. 5-minute hopping window (1m slide) rolling active users & buyer conversion
 *   4. 30-second inactivity gap session windows
 *   5. Causal sequential funnel analysis with MATCH_RECOGNIZE + step drop-off %
 *   6. Top-10 products per 1-minute window
 */
public final class ClickstreamSqlPipelineJob {

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);

        String kafkaBootstrap = cli.getOrDefault("kafka-bootstrap",
                envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"));
        String schemaRegistryUrl = cli.getOrDefault("schema-registry-url",
                envOrDefault("SCHEMA_REGISTRY_URL", "http://schema-registry:8085"));
        String scanStartupMode = cli.getOrDefault("scan-startup-mode",
                envOrDefault("SCAN_STARTUP_MODE", "earliest-offset"));
        String consumerGroup = cli.getOrDefault("consumer-group",
                envOrDefault("SQL_CONSUMER_GROUP", "flink-sql-analytics-v1"));
        String txnPrefix = cli.getOrDefault("txn-prefix",
                envOrDefault("SQL_TXN_PREFIX", "clickstream-sql-" + System.currentTimeMillis()));
        String sqlDir = cli.getOrDefault("sql-dir",
                envOrDefault("FLINK_SQL_DIR", "/opt/flink/sql"));
        String jobName = cli.getOrDefault("job-name", "clickstream-sql-analytics");
        int parallelism = Integer.parseInt(cli.getOrDefault("parallelism", "2"));
        long checkpointMs = Long.parseLong(cli.getOrDefault("checkpoint-ms", "5000"));
        String stateTtl = cli.getOrDefault("state-ttl", "15 min");
        String idleTimeout = cli.getOrDefault("idle-timeout", "5 s");

        Configuration envConf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(envConf);
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Configuration tableConf = tEnv.getConfig().getConfiguration();
        tableConf.setString("pipeline.name", jobName);
        tableConf.setString("table.local-time-zone", "UTC");
        tableConf.setString("table.exec.state.ttl", stateTtl);
        tableConf.setString("table.exec.source.idle-timeout", idleTimeout);
        tableConf.setString("table.exec.sink.not-null-enforcer", "DROP");

        Map<String, String> vars = new HashMap<>();
        vars.put("KAFKA_BOOTSTRAP", kafkaBootstrap);
        vars.put("SCHEMA_REGISTRY_URL", schemaRegistryUrl);
        vars.put("SCAN_STARTUP_MODE", scanStartupMode);
        vars.put("CONSUMER_GROUP", consumerGroup);
        vars.put("TXN_PREFIX", txnPrefix);

        String sourcesSql = SqlScriptUtils.loadSqlResourceOrFile(
                Path.of(sqlDir, "01_sources_kafka.sql").toString(), vars);
        String viewsSql = SqlScriptUtils.loadSqlResourceOrFile(
                Path.of(sqlDir, "02_views.sql").toString(), vars);
        String sinksSql = SqlScriptUtils.loadSqlResourceOrFile(
                Path.of(sqlDir, "03_sinks_kafka.sql").toString(), vars);
        String insertsSql = SqlScriptUtils.loadSqlResourceOrFile(
                Path.of(sqlDir, "04_insert_statements.sql").toString(), vars);

        SqlScriptUtils.executeDdlScript(tEnv, sourcesSql);
        SqlScriptUtils.executeDdlScript(tEnv, viewsSql);
        SqlScriptUtils.executeDdlScript(tEnv, sinksSql);
        SqlScriptUtils.executeInsertScriptAsStatementSet(tEnv, insertsSql);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                map.put(args[i].substring(2), args[i + 1]);
                i++;
            } else if (args[i].startsWith("--")) {
                map.put(args[i].substring(2), "true");
            }
        }
        return map;
    }

    private static String envOrDefault(String key, String fallback) {
        String val = System.getenv(key);
        return (val == null || val.isBlank()) ? fallback : val;
    }
}
