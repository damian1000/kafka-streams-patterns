# TODO

## Cleanup

- `EnrichedOrderData` CSV parsing via `value.split(",")` doesn't handle commas inside quoted fields. Demo-acceptable, but a real implementation would use a CSV library or proper Serde.
- The four topologies share boilerplate (props setup, shutdown hook, builder construction). A small helper in the test or main package could DRY this up if more patterns are added.

## Possible additions

- A schema-registry pattern (Avro or Protobuf) showing typed Serdes instead of `Serdes.String()` everywhere.
- A processor-API example (low-level Processor + state store) for cases the DSL can't express ergonomically.
- A real broker-backed integration test using Testcontainers' Kafka container, kept separate from the unit test set.
