package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordAppCount {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordApp");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.<String, String>stream("sentences")
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
    }
}
