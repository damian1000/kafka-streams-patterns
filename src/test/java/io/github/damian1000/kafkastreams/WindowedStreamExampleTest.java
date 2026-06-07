package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedStreamExampleTest {

    @Test
    void countsPerKeyWithinFiveMinuteTumblingWindow() {
        try (TopologyTestDriver driver = new TopologyTestDriver(WindowedStreamExample.buildTopology(), testProps())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    WindowedStreamExample.INPUT_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    WindowedStreamExample.OUTPUT_TOPIC,
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Anchor times so the tumbling window boundaries are deterministic.
            // Window size is 5 minutes; advancing by 6 forces a new window.
            Instant base = Instant.parse("2026-01-01T10:00:00Z");
            input.pipeInput("alpha", "v1", base);
            input.pipeInput("alpha", "v2", base.plusSeconds(30));
            input.pipeInput("beta",  "v1", base.plusSeconds(60));
            // Step into the next window — alpha's count should reset there.
            input.pipeInput("alpha", "v3", base.plus(Duration.ofMinutes(6)));

            List<KeyValue<String, Long>> records = output.readKeyValuesToList();

            long alphaTotal = records.stream()
                    .filter(kv -> kv.key.startsWith("alpha@"))
                    .mapToLong(kv -> kv.value)
                    .max().orElseThrow();
            assertEquals(2L, alphaTotal, "alpha got two hits in the first window");

            long alphaSecondWindow = records.stream()
                    .filter(kv -> kv.key.startsWith("alpha@"))
                    .filter(kv -> Long.parseLong(kv.key.substring("alpha@".length())) > base.toEpochMilli())
                    .mapToLong(kv -> kv.value)
                    .max().orElseThrow();
            assertEquals(1L, alphaSecondWindow, "the third alpha hit lands in a separate window");

            long betaCount = records.stream()
                    .filter(kv -> kv.key.startsWith("beta@"))
                    .mapToLong(kv -> kv.value)
                    .max().orElseThrow();
            assertEquals(1L, betaCount);
        }
    }

    @Test
    void outputKeyEncodesWindowStartMillis() {
        try (TopologyTestDriver driver = new TopologyTestDriver(WindowedStreamExample.buildTopology(), testProps())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    WindowedStreamExample.INPUT_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    WindowedStreamExample.OUTPUT_TOPIC,
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            Instant t = Instant.parse("2026-01-01T10:02:33Z");
            input.pipeInput("k", "v", t);

            String key = output.readKeyValue().key;
            assertTrue(key.startsWith("k@"), "key is 'name@startMillis': got $key");
            long windowStartMillis = Long.parseLong(key.substring("k@".length()));
            // Window of 5 min starting on a 5-minute boundary that contains t (10:02:33),
            // so the start should be 10:00:00.
            assertEquals(Instant.parse("2026-01-01T10:00:00Z").toEpochMilli(), windowStartMillis);
        }
    }

    private static Properties testProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return p;
    }
}
