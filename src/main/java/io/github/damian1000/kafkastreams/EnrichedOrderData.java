package io.github.damian1000.kafkastreams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class EnrichedOrderData {

    public static void main(String[] args) {
        // set up the configuration for the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enriched-order-data");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create a builder to define the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();

        // create a KStream that reads from the "customer-orders" topic
        KStream<String, String> customerOrders = builder.stream("customer-orders");

        // create a KTable that reads from the "customer-details" topic
        KTable<String, String> customerDetails = builder.table("customer-details");

        // join the customer orders with the customer details. Returning null from the
        // ValueJoiner drops the record from the output stream — important for resilience
        // because any uncaught exception here would otherwise kill the stream thread.
        KStream<String, String> enrichedOrderData = customerOrders
                .selectKey((key, value) -> {
                    String[] fields = value.split(",");
                    return fields.length > 0 ? fields[0] : null;
                })
                .join(customerDetails, (order, details) -> {
                    if (details == null) return null;
                    try {
                        String[] orderFields = order.split(",");
                        String[] detailsFields = details.split(",");
                        if (orderFields.length < 5 || detailsFields.length < 4) return null;
                        return String.format(
                                "{\"customer_id\":\"%s\",\"order_id\":\"%s\",\"product\":\"%s\",\"quantity\":%d,\"customer_name\":\"%s\",\"customer_email\":\"%s\",\"customer_phone\":\"%s\",\"timestamp\":%d}",
                                orderFields[0], orderFields[1], orderFields[2], Integer.parseInt(orderFields[3]),
                                detailsFields[1], detailsFields[2], detailsFields[3], Long.parseLong(orderFields[4]));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // write the enriched order data to a new topic
        enrichedOrderData.to("enriched-order-data", Produced.with(Serdes.String(), Serdes.String()));

        // create and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "kafka-streams-shutdown"));
        streams.start();
    }

}
