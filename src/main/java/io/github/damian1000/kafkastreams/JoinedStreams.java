package io.github.damian1000.kafkastreams;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class JoinedStreams {

    public static void main(String[] args) {
        // set up the configuration for the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "joined-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // create a builder to define the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();

        // create two KStream instances that read from the "clicks" and "impressions" topics
        KStream<String, String> clicks = builder.stream("clicks");
        clicks.peek((key, value) -> System.out.println("clicks: key=" + key + ", value=" + value));

        KStream<String, String> impressions = builder.stream("impressions");
        impressions.peek((key, value) -> System.out.println("impressions: key=" + key + ", value=" + value));

        // join the clicks and impressions streams on the user ID
        KStream<String, String> joinedStream = clicks.join(
                impressions,
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String clickValue, String impressionValue) {
                        System.out.println("apply called click=" + clickValue + ", impression=" + impressionValue);
                        return "click=" + clickValue + ", impression=" + impressionValue;
                    }
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        joinedStream.foreach((key, value) -> System.out.println("Enriched record: " + key + " -> " + value));

        // create and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}
