package io.github.damian1000.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Properties;

public class JoinedStreams {

    static final String CLICKS_TOPIC = "clicks";
    static final String IMPRESSIONS_TOPIC = "impressions";
    static final String JOINED_TOPIC = "enriched-clicks";
    static final Duration JOIN_WINDOW = Duration.ofSeconds(5);

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(buildTopology(), runtimeProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "kafka-streams-shutdown"));
        streams.start();
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clicks = builder.stream(CLICKS_TOPIC);
        KStream<String, String> impressions = builder.stream(IMPRESSIONS_TOPIC);

        KStream<String, String> joinedStream = clicks.join(
                impressions,
                (clickValue, impressionValue) -> "click=" + clickValue + ", impression=" + impressionValue,
                JoinWindows.ofTimeDifferenceWithNoGrace(JOIN_WINDOW),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        joinedStream.to(JOINED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    static Properties runtimeProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "joined-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return props;
    }
}
