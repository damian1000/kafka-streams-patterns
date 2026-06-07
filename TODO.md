# TODO

## Next (Highest Leverage)

- **Add `TopologyTestDriver` tests for each pattern.** Current Codecov badge shows ~0% because the only test (`KafkaStreamsLiveTest`) is `@Disabled` and requires a real broker. `TopologyTestDriver` lets us pipe synthetic input through each topology and assert the output without Docker — fast, deterministic, runs in CI. One test per pattern would bring coverage up meaningfully and demonstrate the modern Kafka Streams testing approach.

## Cleanup

- Convert the anonymous `ValueJoiner<String, String, String>` in `JoinedStreams` to a lambda.
- `EnrichedOrderData` CSV parsing via `value.split(",")` doesn't handle commas inside quoted fields. Demo-acceptable, but a real implementation would use a CSV library or proper Serde.
- The four topologies share boilerplate (props setup, shutdown hook, builder construction). A small helper in the test or main package could DRY this up if more patterns are added.

## Possible additions

- A schema-registry pattern (Avro or Protobuf) showing typed Serdes instead of `Serdes.String()` everywhere.
- A processor-API example (low-level Processor + state store) for cases the DSL can't express ergonomically.
- A real broker-backed integration test using Testcontainers' Kafka container, kept separate from the unit test set.
