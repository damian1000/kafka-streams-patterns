# Kafka Streams Patterns

[![CI](https://github.com/damian1000/kafka-streams-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/damian1000/kafka-streams-patterns/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damian1000/kafka-streams-patterns/actions/workflows/codeql.yml/badge.svg)](https://github.com/damian1000/kafka-streams-patterns/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/damian1000/kafka-streams-patterns/graph/badge.svg)](https://codecov.io/gh/damian1000/kafka-streams-patterns)

Four self-contained Kafka Streams topologies covering the patterns that come up most often in real stream processing: word count, windowed aggregation, stream-stream join with a time window, and KStream–KTable enrichment join.

## Streaming techniques

- **Stateful aggregation** — `groupBy`/`groupByKey` into a materialized state store, not just stateless transforms.
- **Tumbling time windows** — bounding an otherwise-unbounded aggregation by wall-clock time, with the windowed key surfaced on the output rather than hidden.
- **Stream-stream joins** — correlating two independent streams (clicks, impressions) within a bounded `JoinWindows`, the pattern behind attribution/matching problems.
- **KTable enrichment** — joining a stream against table semantics (latest-value-per-key) rather than another stream, the pattern behind reference-data lookups.
- **One topology per pattern, each independently runnable** — each class stays a small, readable `main` rather than a shared test harness, so a reviewer can read (or run) exactly one pattern in isolation without tracing through the others.

## Patterns

| Class                   | Pattern               | What it shows                                                                                                                                     |
| ----------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WordAppCount`          | Stateful aggregation  | `flatMapValues` → `groupBy` → `count()` materialized into a state store, written back to a count topic.                                           |
| `WindowedStreamExample` | Tumbling time windows | `groupByKey` → `windowedBy(TimeWindows.ofSizeWithNoGrace(5min))` → `count()`, with the windowed key flattened into `key@startTime` on the output. |
| `JoinedStreams`         | KStream–KStream join  | Inner join of two streams (`clicks`, `impressions`) within a 5-second `JoinWindows` using `StreamJoined.with(...)`.                               |
| `EnrichedOrderData`     | KStream–KTable join   | Selects a key off the orders stream and joins against a `KTable` of customer details, producing enriched order events.                            |

Each class has a `main` method, so each pattern can be run independently against a broker.

## Run

Start a single-node KRaft broker:

```bash
docker compose up -d
```

Then run any of the patterns (in separate terminals so you can produce input and watch output):

```bash
./gradlew --no-daemon run -PmainClass=io.github.damian1000.kafkastreams.WordAppCount
# or directly via the jar:
./gradlew jar
java -cp build/libs/kafka-streams-patterns-1.0.0.jar:$(./gradlew -q printRuntimeClasspath) io.github.damian1000.kafkastreams.WordAppCount
```

Produce / consume against the broker:

```bash
# Producer
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic sentences

# Consumer (for WordAppCount output)
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic word-count \
  --from-beginning \
  --property print.key=true \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

## Topics each pattern uses

| Pattern                 | Input topic(s)                        | Output topic          |
| ----------------------- | ------------------------------------- | --------------------- |
| `WordAppCount`          | `sentences`                           | `word-count`          |
| `WindowedStreamExample` | `input`                               | `output`              |
| `JoinedStreams`         | `clicks`, `impressions`               | `enriched-clicks`     |
| `EnrichedOrderData`     | `customer-orders`, `customer-details` | `enriched-order-data` |

Topics are auto-created by the broker on first publish (default config in `docker-compose.yml`).

## Testing

`KafkaStreamsLiveTest` exercises a wordcount topology against a real broker. It is `@Disabled` by default because it requires `docker compose up` and blocks for ~30 seconds. Run it manually with:

```bash
./gradlew test --tests KafkaStreamsLiveTest -DargLine="-Djunit.jupiter.conditions.deactivate=*"
```

For purely topology-level testing without a broker, use `org.apache.kafka.streams.TopologyTestDriver` — these patterns are kept as small `main` methods rather than wrapped in test-driver harnesses so each stays readable in isolation.

## Stack

- Kafka Streams 4.3.0
- JDK 25 toolchain
- JUnit Jupiter 6.1
- Confluent CP 7.7.1 (KRaft mode, no Zookeeper)
- Gradle 9.5.1

## License

Apache 2.0 — see [LICENSE](LICENSE).
