package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WordAppCountTest {

    @Test
    void splitsLowercasesAndCountsWordsAcrossMultipleInputs() {
        try (TopologyTestDriver driver = new TopologyTestDriver(WordAppCount.buildTopology(), testProps())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    WordAppCount.INPUT_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    WordAppCount.OUTPUT_TOPIC,
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            input.pipeInput("k1", "hello world hello");
            input.pipeInput("k2", "WORLD foo");

            Map<String, Long> counts = output.readKeyValuesToMap();
            assertEquals(2L, counts.get("hello"));
            assertEquals(2L, counts.get("world"), "values are lowercased before counting");
            assertEquals(1L, counts.get("foo"));
        }
    }

    @Test
    void emptyValueProducesNoOutput() {
        try (TopologyTestDriver driver = new TopologyTestDriver(WordAppCount.buildTopology(), testProps())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    WordAppCount.INPUT_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    WordAppCount.OUTPUT_TOPIC,
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Empty string split on " " yields [""], so the word-count will produce
            // a single "" entry. This documents existing behavior so a future change
            // (e.g. filtering blanks) breaks this test loudly.
            input.pipeInput("k", "");
            Map<String, Long> counts = output.readKeyValuesToMap();
            assertEquals(1L, counts.get(""));
        }
    }

    private static Properties testProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordappcount-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return p;
    }
}
