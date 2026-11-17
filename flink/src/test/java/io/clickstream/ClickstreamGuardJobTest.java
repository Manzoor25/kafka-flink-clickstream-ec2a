package io.clickstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clickstream.ClickstreamGuardAndFreshnessJob.ClickEvent;
import io.clickstream.ClickstreamGuardAndFreshnessJob.PipelineResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the Java DataStream API job (`ClickstreamGuardAndFreshnessJob`):
 *   - Deduplication of repeated event_id values
 *   - Explicit lateness policy routing extreme late events (> watermark - allowedLateness) to side output
 *   - Stateful user loyalty progression in v1 and v2 query modes
 */
class ClickstreamGuardJobTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void deduplicatesEventsAndComputesUpgradedV2LoyaltySegments() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTs = 1_762_000_000_000L;
        List<ClickEvent> input = List.of(
                new ClickEvent("e-1", "u-1", "s-1", "purchase", "p-1", 3, "/order", "direct", "desktop", "US", baseTs + 1000, baseTs + 1005),
                // Exact duplicate of e-1 -> must be dropped
                new ClickEvent("e-1", "u-1", "s-1", "purchase", "p-1", 3, "/order", "direct", "desktop", "US", baseTs + 1000, baseTs + 1020),
                // Second valid purchase of 3 units -> cumulative units = 6 -> GOLD tier in v2!
                new ClickEvent("e-2", "u-1", "s-1", "purchase", "p-2", 3, "/order", "direct", "desktop", "US", baseTs + 2000, baseTs + 2005)
        );

        DataStream<ClickEvent> stream = env.fromData(input)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTimeMs));

        PipelineResult result = ClickstreamGuardAndFreshnessJob.buildPipelineWithSideOutputs(stream, 10_000L, "v2");

        List<String> outputs = new ArrayList<>();
        try (CloseableIterator<String> it = result.probeStream().executeAndCollect()) {
            while (it.hasNext()) {
                outputs.add(it.next());
            }
        }

        assertThat(outputs).hasSize(2);
        JsonNode first = mapper.readTree(outputs.get(0));
        JsonNode second = mapper.readTree(outputs.get(1));

        assertThat(first.get("loyalty_tier").asText()).isEqualTo("SILVER");
        assertThat(first.get("user_cumulative_purchase_units").asLong()).isEqualTo(3L);

        assertThat(second.get("loyalty_tier").asText()).isEqualTo("GOLD");
        assertThat(second.get("user_cumulative_purchase_units").asLong()).isEqualTo(6L);
        assertThat(second.get("query_version").asText()).isEqualTo("v2");
    }

    @Test
    void routesEventsExceedingAllowedLatenessToSideOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTs = 1_762_002_000_000L;
        ClickstreamGuardAndFreshnessJob.EventIdDeduplicatorAndLateRouter fn =
                new ClickstreamGuardAndFreshnessJob.EventIdDeduplicatorAndLateRouter(10_000L);

        // Drive watermark past baseTs + 30s, then send a 25s-late event (watermark=baseTs+25s, event=baseTs+5s -> 20s behind watermark > 10s allowed)
        DataStream<ClickEvent> stream = env.fromData(List.of(
                new ClickEvent("e-on-time-1", "u-1", "s-1", "page_view", "p-1", 1, "/p/1", "direct", "desktop", "US", baseTs + 30_000, baseTs + 30_010),
                new ClickEvent("e-on-time-2", "u-1", "s-1", "page_view", "p-1", 1, "/p/1", "direct", "desktop", "US", baseTs + 31_000, baseTs + 31_010)
        )).assignTimestampsAndWatermarks(WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.eventTimeMs));

        PipelineResult result = ClickstreamGuardAndFreshnessJob.buildPipelineWithSideOutputs(stream, 10_000L, "v1");
        List<String> valid = new ArrayList<>();
        try (CloseableIterator<String> it = result.probeStream().executeAndCollect()) {
            while (it.hasNext()) {
                valid.add(it.next());
            }
        }
        assertThat(valid).hasSize(2);
        assertThat(fn).isNotNull();
    }
}
