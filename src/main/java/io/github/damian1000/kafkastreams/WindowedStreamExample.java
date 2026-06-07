package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

public class WindowedStreamExample {

    static final String INPUT_TOPIC = "input";
    static final String OUTPUT_TOPIC = "output";
    static final Duration WINDOW_SIZE = Duration.ofMinutes(5);

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(buildTopology(), runtimeProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "kafka-streams-shutdown"));
        streams.start();
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(INPUT_TOPIC);
        input
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start(), value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    static Properties runtimeProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-stream-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return props;
    }
}
