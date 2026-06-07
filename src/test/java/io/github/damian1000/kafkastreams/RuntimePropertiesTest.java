package io.github.damian1000.kafkastreams;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Pins the runtime configuration each topology ships with, so a future change
 * (e.g. accidentally dropping the cache config or pointing at the wrong default
 * broker) breaks loudly.
 */
class RuntimePropertiesTest {

    @Test
    void wordAppCountUsesCacheDisabledAndDefaultLocalBroker() {
        Properties p = WordAppCount.runtimeProperties();
        assertEquals("WordApp", p.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("localhost:9092", p.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(0, p.get(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG));
        assertNotNull(p.get(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertNotNull(p.get(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }

    @Test
    void windowedStreamExampleUsesCacheDisabledAndDefaultLocalBroker() {
        Properties p = WindowedStreamExample.runtimeProperties();
        assertEquals("windowed-stream-example", p.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("localhost:9092", p.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(0, p.get(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG));
    }

    @Test
    void joinedStreamsUsesEarliestOffsetResetAndCacheDisabled() {
        Properties p = JoinedStreams.runtimeProperties();
        assertEquals("joined-streams", p.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("earliest", p.get("auto.offset.reset"));
        assertEquals(0, p.get(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG));
    }

    @Test
    void enrichedOrderDataUsesDefaultLocalBroker() {
        Properties p = EnrichedOrderData.runtimeProperties();
        assertEquals("enriched-order-data", p.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("localhost:9092", p.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void implicitConstructorsAreReachable() {
        // These classes only carry static `buildTopology` / `runtimeProperties` /
        // `main` methods, but the implicit default constructor still shows up in
        // bytecode. Instantiating each once keeps coverage honest about what's
        // actually a launch wrapper vs. dead code.
        assertNotNull(new WordAppCount());
        assertNotNull(new WindowedStreamExample());
        assertNotNull(new JoinedStreams());
        assertNotNull(new EnrichedOrderData());
    }
}
