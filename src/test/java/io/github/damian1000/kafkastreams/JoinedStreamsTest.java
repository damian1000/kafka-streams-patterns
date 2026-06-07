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

class JoinedStreamsTest {

    @Test
    void clickAndImpressionWithinJoinWindowAreJoined() {
        try (TopologyTestDriver driver = new TopologyTestDriver(JoinedStreams.buildTopology(), testProps())) {
            TestInputTopic<String, String> clicks = driver.createInputTopic(
                    JoinedStreams.CLICKS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> impressions = driver.createInputTopic(
                    JoinedStreams.IMPRESSIONS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> joined = driver.createOutputTopic(
                    JoinedStreams.JOINED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            Instant t = Instant.parse("2026-01-01T10:00:00Z");
            clicks.pipeInput("user1", "click-data", t);
            impressions.pipeInput("user1", "impression-data", t.plusSeconds(2));

            KeyValue<String, String> result = joined.readKeyValue();
            assertEquals("user1", result.key);
            assertEquals("click=click-data, impression=impression-data", result.value);
            assertTrue(joined.isEmpty(), "exactly one join result");
        }
    }

    @Test
    void clickAndImpressionOutsideJoinWindowAreNotJoined() {
        try (TopologyTestDriver driver = new TopologyTestDriver(JoinedStreams.buildTopology(), testProps())) {
            TestInputTopic<String, String> clicks = driver.createInputTopic(
                    JoinedStreams.CLICKS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> impressions = driver.createInputTopic(
                    JoinedStreams.IMPRESSIONS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> joined = driver.createOutputTopic(
                    JoinedStreams.JOINED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            Instant t = Instant.parse("2026-01-01T10:00:00Z");
            clicks.pipeInput("user1", "click", t);
            // 10 seconds later — outside the 5-second join window.
            impressions.pipeInput("user1", "imp", t.plus(Duration.ofSeconds(10)));

            assertTrue(joined.isEmpty(), "no join result when records are outside the window");
        }
    }

    @Test
    void differentKeysAreNotJoined() {
        try (TopologyTestDriver driver = new TopologyTestDriver(JoinedStreams.buildTopology(), testProps())) {
            TestInputTopic<String, String> clicks = driver.createInputTopic(
                    JoinedStreams.CLICKS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> impressions = driver.createInputTopic(
                    JoinedStreams.IMPRESSIONS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> joined = driver.createOutputTopic(
                    JoinedStreams.JOINED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            Instant t = Instant.parse("2026-01-01T10:00:00Z");
            clicks.pipeInput("user1", "click", t);
            impressions.pipeInput("user2", "imp", t);

            List<KeyValue<String, String>> results = joined.readKeyValuesToList();
            assertEquals(0, results.size());
        }
    }

    private static Properties testProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "joined-streams-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return p;
    }
}
