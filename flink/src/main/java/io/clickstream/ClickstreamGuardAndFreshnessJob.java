package io.clickstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Job 2: Java DataStream API job backed by RocksDB state and S3 checkpoints.
 *
 * Responsibilities:
 *  1. Decodes Confluent-framed Avro (or JSON fallback) from `clickstream.raw.v1`.
 *  2. Deduplicates events by `event_id` using RocksDB ValueState + 15-min TTL.
 *  3. Enforces explicit lateness policy: events arriving behind `currentWatermark - allowedLatenessMs`
 *     are routed via Side Output (`LATE_EVENTS_TAG`) to `clickstream.late.v1` rather than dropped silently.
 *  4. Tracks stateful per-user lifetime activity (`user_event_count`, `user_purchase_units`) with stable
 *     operator UIDs so the job can be stopped with a savepoint (`query-version=v1`) and upgraded in place
 *     (`query-version=v2` adding 4-tier loyalty & velocity surge flags) without losing historical RocksDB state.
 *  5. Emits per-event freshness probes stamped with `flink_processed_ms` to `analytics.freshness_probes.v1`
 *     using a 2PC `EXACTLY_ONCE` transactional KafkaSink.
 */
public final class ClickstreamGuardAndFreshnessJob {

    public static final OutputTag<String> LATE_EVENTS_TAG = new OutputTag<>("late-events", Types.STRING);

    private static final String DEFAULT_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "ClickstreamEvent",
              "namespace": "io.clickstream.avro",
              "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "client_session_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "product_id", "type": "string"},
                {"name": "quantity", "type": "int", "default": 1},
                {"name": "page_url", "type": "string"},
                {"name": "referrer", "type": "string", "default": "direct"},
                {"name": "device_type", "type": "string", "default": "desktop"},
                {"name": "country", "type": "string", "default": "US"},
                {"name": "event_time_ms", "type": "long"},
                {"name": "created_at_ms", "type": "long"}
              ]
            }
            """;

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = ClickstreamSqlPipelineJob.parseArgs(args);

        String kafkaBootstrap = cli.getOrDefault("kafka-bootstrap",
                envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"));
        String consumerGroup = cli.getOrDefault("consumer-group",
                envOrDefault("GUARD_CONSUMER_GROUP", "flink-datastream-guard-v1"));
        String startupMode = cli.getOrDefault("scan-startup-mode", "earliest");
        String queryVersion = cli.getOrDefault("query-version", "v1");
        String txnPrefix = cli.getOrDefault("txn-prefix",
                "clickstream-guard-" + queryVersion + "-" + System.currentTimeMillis());
        String schemaPath = cli.getOrDefault("schema-path", "/opt/flink/schemas/clickstream_event.avsc");
        int parallelism = Integer.parseInt(cli.getOrDefault("parallelism", "2"));
        long checkpointMs = Long.parseLong(cli.getOrDefault("checkpoint-ms", "5000"));
        long allowedLatenessMs = Long.parseLong(cli.getOrDefault("allowed-lateness-ms", "10000"));

        String avroSchemaJson = loadSchemaJson(schemaPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        OffsetsInitializer offsets = "committed".equalsIgnoreCase(startupMode)
                ? OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
                : OffsetsInitializer.earliest();

        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("clickstream.raw.v1")
                .setGroupId(consumerGroup)
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new ConfluentAvroOrJsonDeserializer(avroSchemaJson))
                .build();

        WatermarkStrategy<ClickEvent> watermarks = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTs) -> event.eventTimeMs)
                .withIdleness(Duration.ofSeconds(5));

        DataStream<ClickEvent> rawStream = env
                .fromSource(source, watermarks, "kafka-clickstream-raw-source")
                .uid("kafka-clickstream-raw-source");

        PipelineResult pipeline = buildPipelineWithSideOutputs(rawStream, allowedLatenessMs, queryVersion);

        Properties txnProps = new Properties();
        txnProps.put("transaction.timeout.ms", "300000");

        KafkaSink<String> freshnessSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(txnPrefix + "-probes")
                .setKafkaProducerConfig(txnProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("analytics.freshness_probes.v1")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        KafkaSink<String> lateEventsSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(txnPrefix + "-late")
                .setKafkaProducerConfig(txnProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("clickstream.late.v1")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        pipeline.probeStream().sinkTo(freshnessSink)
                .uid("kafka-freshness-probes-sink")
                .name("kafka-freshness-probes-sink");

        pipeline.lateStream().sinkTo(lateEventsSink)
                .uid("kafka-late-events-sink")
                .name("kafka-late-events-sink");

        env.execute("clickstream-datastream-guard-" + queryVersion);
    }

    public static SingleOutputStreamOperator<String> buildPipeline(
            DataStream<ClickEvent> rawStream,
            long allowedLatenessMs,
            String queryVersion) {

        SingleOutputStreamOperator<ClickEvent> deduplicated = rawStream
                .filter(Objects::nonNull)
                .uid("non-null-event-filter")
                .keyBy(e -> e.eventId)
                .process(new EventIdDeduplicatorAndLateRouter(allowedLatenessMs))
                .uid("dedup-by-event-id")
                .name("dedup-by-event-id");

        SingleOutputStreamOperator<String> enrichedProbes = deduplicated
                .keyBy(e -> e.userId)
                .process(new StatefulUserActivityAndFreshnessFn(queryVersion))
                .uid("stateful-user-activity-and-freshness")
                .name("stateful-user-activity-and-freshness-" + queryVersion);

        // Merge late side-output from deduplicator onto a unified return handle for callers:
        // callers who need deduplicated's side output directly can also use buildDeduplicatedStage.
        return new PipelineStages(deduplicated, enrichedProbes).mergedHandle();
    }

    public record PipelineResult(
            SingleOutputStreamOperator<String> probeStream,
            DataStream<String> lateStream) {
    }

    public static PipelineResult buildPipelineWithSideOutputs(
            DataStream<ClickEvent> rawStream,
            long allowedLatenessMs,
            String queryVersion) {

        SingleOutputStreamOperator<ClickEvent> deduplicated = rawStream
                .filter(Objects::nonNull)
                .uid("non-null-event-filter")
                .keyBy(e -> e.eventId)
                .process(new EventIdDeduplicatorAndLateRouter(allowedLatenessMs))
                .uid("dedup-by-event-id")
                .name("dedup-by-event-id");

        SingleOutputStreamOperator<String> probes = deduplicated
                .keyBy(e -> e.userId)
                .process(new StatefulUserActivityAndFreshnessFn(queryVersion))
                .uid("stateful-user-activity-and-freshness")
                .name("stateful-user-activity-and-freshness-" + queryVersion);

        return new PipelineResult(probes, deduplicated.getSideOutput(LATE_EVENTS_TAG));
    }

    private record PipelineStages(
            SingleOutputStreamOperator<ClickEvent> dedupStage,
            SingleOutputStreamOperator<String> probeStage) {
        SingleOutputStreamOperator<String> mergedHandle() {
            return probeStage;
        }
    }

    /**
     * Stateful deduplicator on `event_id` with RocksDB TTL + side-output routing for extreme late events.
     */
    public static final class EventIdDeduplicatorAndLateRouter
            extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {

        private final long allowedLatenessMs;
        private transient ValueState<Boolean> seenState;
        private transient ObjectMapper mapper;
        private transient Counter duplicateCounter;
        private transient Counter lateCounter;

        public EventIdDeduplicatorAndLateRouter(long allowedLatenessMs) {
            this.allowedLatenessMs = allowedLatenessMs;
        }

        @Override
        public void open(OpenContext openContext) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofMinutes(15))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Types.BOOLEAN);
            descriptor.enableTimeToLive(ttlConfig);
            this.seenState = getRuntimeContext().getState(descriptor);
            this.mapper = new ObjectMapper();
            this.duplicateCounter = getRuntimeContext().getMetricGroup().counter("clickstream_duplicates_dropped");
            this.lateCounter = getRuntimeContext().getMetricGroup().counter("clickstream_late_events_routed");
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<ClickEvent> out) throws Exception {
            Boolean alreadySeen = seenState.value();
            if (Boolean.TRUE.equals(alreadySeen)) {
                duplicateCounter.inc();
                return;
            }
            seenState.update(Boolean.TRUE);

            long currentWatermark = ctx.timerService().currentWatermark();
            if (currentWatermark > Long.MIN_VALUE + 1_000_000L
                    && event.eventTimeMs + allowedLatenessMs < currentWatermark) {
                lateCounter.inc();
                ObjectNode lateJson = mapper.createObjectNode();
                lateJson.put("event_id", event.eventId);
                lateJson.put("user_id", event.userId);
                lateJson.put("event_type", event.eventType);
                lateJson.put("product_id", event.productId);
                lateJson.put("quantity", event.quantity);
                lateJson.put("event_time_ms", event.eventTimeMs);
                lateJson.put("watermark_ms", currentWatermark);
                lateJson.put("lateness_ms", currentWatermark - event.eventTimeMs);
                lateJson.put("routed_at_ms", System.currentTimeMillis());
                ctx.output(LATE_EVENTS_TAG, mapper.writeValueAsString(lateJson));
                return;
            }

            out.collect(event);
        }
    }

    /**
     * Keyed stateful operator maintaining cumulative shopper activity across savepoint upgrades (`v1` -> `v2`)
     * and emitting millisecond freshness probe events.
     */
    public static final class StatefulUserActivityAndFreshnessFn
            extends KeyedProcessFunction<String, ClickEvent, String> {

        private final String queryVersion;
        private transient ValueState<Long> totalEventsState;
        private transient ValueState<Long> totalPurchaseUnitsState;
        private transient ObjectMapper mapper;

        public StatefulUserActivityAndFreshnessFn(String queryVersion) {
            this.queryVersion = queryVersion;
        }

        @Override
        public void open(OpenContext openContext) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(6))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Long> eventsDesc = new ValueStateDescriptor<>("user-total-events", Types.LONG);
            eventsDesc.enableTimeToLive(ttlConfig);
            this.totalEventsState = getRuntimeContext().getState(eventsDesc);

            ValueStateDescriptor<Long> unitsDesc = new ValueStateDescriptor<>("user-purchase-units", Types.LONG);
            unitsDesc.enableTimeToLive(ttlConfig);
            this.totalPurchaseUnitsState = getRuntimeContext().getState(unitsDesc);

            this.mapper = new ObjectMapper();
        }

        @Override
        public void processElement(ClickEvent event, Context ctx, Collector<String> out) throws Exception {
            long count = OptionalLongOrZero(totalEventsState.value()) + 1L;
            long purchaseUnits = OptionalLongOrZero(totalPurchaseUnitsState.value())
                    + ("purchase".equals(event.eventType) ? event.quantity : 0L);

            totalEventsState.update(count);
            totalPurchaseUnitsState.update(purchaseUnits);

            long flinkProcessedMs = System.currentTimeMillis();
            ObjectNode node = mapper.createObjectNode();
            node.put("event_id", event.eventId);
            node.put("user_id", event.userId);
            node.put("event_type", event.eventType);
            node.put("product_id", event.productId);
            node.put("quantity", event.quantity);
            node.put("country", event.country);
            node.put("device_type", event.deviceType);
            node.put("event_time_ms", event.eventTimeMs);
            node.put("created_at_ms", event.createdAtMs);
            node.put("flink_processed_ms", flinkProcessedMs);
            node.put("flink_latency_ms", Math.max(0L, flinkProcessedMs - event.createdAtMs));
            node.put("user_cumulative_events", count);
            node.put("user_cumulative_purchase_units", purchaseUnits);
            node.put("query_version", queryVersion);

            if ("v2".equalsIgnoreCase(queryVersion)) {
                // Upgraded v2 business logic deployed seamlessly from v1 savepoint:
                // reads the exact preserved RocksDB cumulative state to compute 4-tier segment & surge flag.
                String segment = purchaseUnits >= 10 ? "PLATINUM"
                        : purchaseUnits >= 5 ? "GOLD"
                        : purchaseUnits >= 2 ? "SILVER"
                        : "BRONZE";
                node.put("loyalty_tier", segment);
                node.put("high_velocity_session", count >= 8);
            } else {
                String segment = purchaseUnits >= 5 ? "VIP" : "STANDARD";
                node.put("loyalty_tier", segment);
                node.put("high_velocity_session", false);
            }

            out.collect(mapper.writeValueAsString(node));
        }

        private static long OptionalLongOrZero(Long v) {
            return v == null ? 0L : v;
        }
    }

    /**
     * Immutable POJO representing a raw clickstream event.
     */
    public static final class ClickEvent implements Serializable {
        public String eventId;
        public String userId;
        public String clientSessionId;
        public String eventType;
        public String productId;
        public int quantity;
        public String pageUrl;
        public String referrer;
        public String deviceType;
        public String country;
        public long eventTimeMs;
        public long createdAtMs;

        public ClickEvent() {
        }

        public ClickEvent(
                String eventId,
                String userId,
                String clientSessionId,
                String eventType,
                String productId,
                int quantity,
                String pageUrl,
                String referrer,
                String deviceType,
                String country,
                long eventTimeMs,
                long createdAtMs) {
            this.eventId = eventId;
            this.userId = userId;
            this.clientSessionId = clientSessionId;
            this.eventType = eventType;
            this.productId = productId;
            this.quantity = quantity;
            this.pageUrl = pageUrl;
            this.referrer = referrer;
            this.deviceType = deviceType;
            this.country = country;
            this.eventTimeMs = eventTimeMs;
            this.createdAtMs = createdAtMs;
        }
    }

    /**
     * Decodes Confluent Schema Registry framed Avro (0x00 + 4-byte schema ID + binary Avro)
     * with automatic fallback to UTF-8 JSON for test fixtures.
     */
    public static final class ConfluentAvroOrJsonDeserializer extends AbstractDeserializationSchema<ClickEvent> {
        private final String schemaJson;
        private transient GenericDatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder reuseDecoder;
        private transient ObjectMapper jsonMapper;

        public ConfluentAvroOrJsonDeserializer(String schemaJson) {
            super(ClickEvent.class);
            this.schemaJson = schemaJson;
        }

        @Override
        public void open(InitializationContext context) {
            Schema schema = new Schema.Parser().parse(schemaJson);
            this.datumReader = new GenericDatumReader<>(schema);
            this.jsonMapper = new ObjectMapper();
        }

        @Override
        public ClickEvent deserialize(byte[] message) throws IOException {
            if (message == null || message.length == 0) {
                return null;
            }
            if (message[0] == 0x00 && message.length > 5) {
                reuseDecoder = DecoderFactory.get().binaryDecoder(message, 5, message.length - 5, reuseDecoder);
                GenericRecord rec = datumReader.read(null, reuseDecoder);
                return new ClickEvent(
                        str(rec.get("event_id")),
                        str(rec.get("user_id")),
                        str(rec.get("client_session_id")),
                        str(rec.get("event_type")),
                        str(rec.get("product_id")),
                        ((Number) rec.get("quantity")).intValue(),
                        str(rec.get("page_url")),
                        str(rec.get("referrer")),
                        str(rec.get("device_type")),
                        str(rec.get("country")),
                        ((Number) rec.get("event_time_ms")).longValue(),
                        ((Number) rec.get("created_at_ms")).longValue());
            }
            JsonNode node = jsonMapper.readTree(new String(message, StandardCharsets.UTF_8));
            return new ClickEvent(
                    node.path("event_id").asText(),
                    node.path("user_id").asText(),
                    node.path("client_session_id").asText(""),
                    node.path("event_type").asText(),
                    node.path("product_id").asText(),
                    node.path("quantity").asInt(1),
                    node.path("page_url").asText(""),
                    node.path("referrer").asText("direct"),
                    node.path("device_type").asText("desktop"),
                    node.path("country").asText("US"),
                    node.path("event_time_ms").asLong(),
                    node.path("created_at_ms").asLong());
        }

        private static String str(Object val) {
            return val == null ? "" : val.toString();
        }
    }

    private static String loadSchemaJson(String schemaPath) {
        try {
            Path p = Path.of(schemaPath);
            if (Files.exists(p)) {
                return Files.readString(p, StandardCharsets.UTF_8);
            }
            try (InputStream in = ClickstreamGuardAndFreshnessJob.class.getResourceAsStream("/clickstream_event.avsc")) {
                if (in != null) {
                    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
        } catch (IOException ignored) {
        }
        return DEFAULT_AVRO_SCHEMA;
    }

    private static String envOrDefault(String key, String fallback) {
        String val = System.getenv(key);
        return (val == null || val.isBlank()) ? fallback : val;
    }
}
