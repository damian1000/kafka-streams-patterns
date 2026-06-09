package io.github.damian1000.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnrichedOrderDataTest {

    @Test
    void enrichesOrderWithCustomerDetailsAndEmitsJson() {
        try (TopologyTestDriver driver = new TopologyTestDriver(EnrichedOrderData.buildTopology(), testProps())) {
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    EnrichedOrderData.ORDERS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> details = driver.createInputTopic(
                    EnrichedOrderData.DETAILS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> enriched = driver.createOutputTopic(
                    EnrichedOrderData.ENRICHED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            details.pipeInput("cust1", "cust1,Alice,alice@example.com,+44-555-0100");
            // Order CSV: customer_id, order_id, product, quantity, timestamp
            orders.pipeInput("ignored-key", "cust1,ord1,Widget,3,1700000000000");

            KeyValue<String, String> result = enriched.readKeyValue();
            assertEquals("cust1", result.key);
            assertTrue(result.value.contains("\"customer_id\":\"cust1\""));
            assertTrue(result.value.contains("\"order_id\":\"ord1\""));
            assertTrue(result.value.contains("\"product\":\"Widget\""));
            assertTrue(result.value.contains("\"quantity\":3"));
            assertTrue(result.value.contains("\"customer_name\":\"Alice\""));
            assertTrue(result.value.contains("\"customer_email\":\"alice@example.com\""));
            assertTrue(result.value.contains("\"timestamp\":1700000000000"));
        }
    }

    @Test
    void orderWithoutMatchingCustomerDetailsIsDropped() {
        try (TopologyTestDriver driver = new TopologyTestDriver(EnrichedOrderData.buildTopology(), testProps())) {
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    EnrichedOrderData.ORDERS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> enriched = driver.createOutputTopic(
                    EnrichedOrderData.ENRICHED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            orders.pipeInput("ignored", "cust_unknown,ord1,Widget,3,1700000000000");
            assertTrue(enriched.isEmpty(), "no customer details -> nothing emitted");
        }
    }

    @Test
    void malformedOrderRowIsDroppedNotCrashed() {
        try (TopologyTestDriver driver = new TopologyTestDriver(EnrichedOrderData.buildTopology(), testProps())) {
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    EnrichedOrderData.ORDERS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> details = driver.createInputTopic(
                    EnrichedOrderData.DETAILS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> enriched = driver.createOutputTopic(
                    EnrichedOrderData.ENRICHED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            details.pipeInput("cust1", "cust1,Alice,alice@example.com,+44-555-0100");
            // Quantity is non-numeric — would throw NumberFormatException without the guard.
            orders.pipeInput("ignored", "cust1,ord1,Widget,NOT_A_NUMBER,1700000000000");
            assertTrue(enriched.isEmpty(), "malformed row dropped, stream not killed");

            // And a well-formed row right after still works.
            orders.pipeInput("ignored", "cust1,ord2,Gadget,1,1700000001000");
            assertEquals("cust1", enriched.readKeyValue().key);
        }
    }

    @Test
    void productNameContainingCommaIsHandledWhenQuoted() {
        try (TopologyTestDriver driver = new TopologyTestDriver(EnrichedOrderData.buildTopology(), testProps())) {
            TestInputTopic<String, String> orders = driver.createInputTopic(
                    EnrichedOrderData.ORDERS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> details = driver.createInputTopic(
                    EnrichedOrderData.DETAILS_TOPIC,
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> enriched = driver.createOutputTopic(
                    EnrichedOrderData.ENRICHED_TOPIC,
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            details.pipeInput("cust1", "cust1,Alice,alice@example.com,+44-555-0100");
            // Product name contains a comma; it would be split incorrectly without RFC-4180 parsing.
            orders.pipeInput("ignored", "cust1,ord1,\"Widget, deluxe\",3,1700000000000");

            String value = enriched.readKeyValue().value;
            assertTrue(value.contains("\"product\":\"Widget, deluxe\""), value);
            assertTrue(value.contains("\"quantity\":3"), value);
        }
    }

    @Test
    void parseCsvHandlesEscapedQuotes() {
        String[] fields = EnrichedOrderData.parseCsv("a,\"he said \"\"hi\"\"\",c");
        assertEquals(3, fields.length);
        assertEquals("a", fields[0]);
        assertEquals("he said \"hi\"", fields[1]);
        assertEquals("c", fields[2]);
    }

    private static Properties testProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "enriched-order-data-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return p;
    }
}
