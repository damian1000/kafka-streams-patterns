# TODO

- Add a schema-registry pattern (Avro or Protobuf) with backward/forward compatibility tests in place of `Serdes.String()` everywhere.
- Add a Processor API example (custom state store with per-record side effects).
- Add a Testcontainers-backed integration test (real broker, real consumer group rebalance, real exactly-once-v2).
- DRY up the props/shutdown-hook/builder boilerplate across the four topologies once more patterns are added.
- Decide whether to fold the interesting topologies into `kafka-microservices-demo` and archive this repo.
