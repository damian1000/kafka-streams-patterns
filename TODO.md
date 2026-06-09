# TODO

## Roadmap (prioritized)

### P1 — done

- README's topics table now shows `JoinedStreams` writing to `enriched-clicks`
  (was incorrectly "logs joined records to stdout"). The other three rows
  (`sentences` → `word-count`, `input` → `output`,
  `customer-orders` + `customer-details` → `enriched-order-data`) were
  verified against the `.to(...)` targets in each topology.

### P2 — pick one investment if you want to keep this repo

The review's framing is "tutorial-like; merge or expand". Honest read: with `TopologyTestDriver` tests in for every pattern, the repo already does the job a tutorial would. To make it worth pinning standalone, pick one:

- **Schema-registry pattern.** Add an Avro or Protobuf example with backward/forward compatibility tests, replacing the `Serdes.String()`-everywhere approach. The single most common production gap in Streams code.
- **Processor API example.** A pattern the DSL can't express well (e.g. custom state store with side-effects on every record). Shows you can drop down a level when the DSL isn't enough.
- **Testcontainers-backed integration test.** Real broker, real consumer group rebalance, real exactly-once-v2. Complements the existing `TopologyTestDriver` tests without replacing them.

### P3 — alternative: fold into another repo

If you're not going to invest in P2, the review's "merge or expand" suggestion is reasonable — consider moving the most interesting one or two topologies into `kafka-microservices-demo` as additional patterns and archiving this repo. Less to maintain, no separate README to drift.

## Cleanup

- `EnrichedOrderData` CSV parsing via `value.split(",")` doesn't handle commas inside quoted fields. Demo-acceptable, but a real implementation would use a CSV library or proper Serde.
- The four topologies share boilerplate (props setup, shutdown hook, builder construction). A small helper in the test or main package could DRY this up if more patterns are added.

## Possible additions

- A schema-registry pattern (Avro or Protobuf) showing typed Serdes instead of `Serdes.String()` everywhere.
- A processor-API example (low-level Processor + state store) for cases the DSL can't express ergonomically.
- A real broker-backed integration test using Testcontainers' Kafka container, kept separate from the unit test set.
