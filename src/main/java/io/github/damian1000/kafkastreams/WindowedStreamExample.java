package io.github.damian1000.kafkastreams;

import java.util.Properties;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

public class WindowedStreamExample {

    public static void main(String[] args) {
        // set up the configuration for the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-stream-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create a builder to define the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();

        // create a KStream that reads from the "input" topic
        KStream<String, String> input = builder.stream("input");

        // group the data into 5-minute time windows and count the occurrences of each key
        input
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start(), value))
                .to("output");

        // create and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}
