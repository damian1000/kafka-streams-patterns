package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class EnrichedOrderData {

    static final String ORDERS_TOPIC = "customer-orders";
    static final String DETAILS_TOPIC = "customer-details";
    static final String ENRICHED_TOPIC = "enriched-order-data";

    @Generated
    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(buildTopology(), runtimeProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "kafka-streams-shutdown"));
        streams.start();
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> customerOrders = builder.stream(ORDERS_TOPIC);
        KTable<String, String> customerDetails = builder.table(DETAILS_TOPIC);

        // Re-key the orders by customer-id (the first CSV field) so they line up
        // with customer-details. Returning null from the ValueJoiner drops the
        // record from the output, so any malformed row is skipped rather than
        // crashing the stream thread.
        customerOrders
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
                }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                // null values from the joiner mark malformed rows; filter them out
                // here so we don't emit tombstones for what was really a parse failure.
                .filter((key, value) -> value != null)
                .to(ENRICHED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    static Properties runtimeProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enriched-order-data");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
