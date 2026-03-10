# Architecture

## Overview

The Iggy Flink POC is an Apache Flink 2.0 application that subscribes to an Apache Iggy
topic, processes messages through a configurable pipeline, and publishes results to another
Iggy topic. **Only TCP transport is used** — HTTP/REST and QUIC are explicitly disabled.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Flink DataStream Pipeline                        │
│                                                                         │
│  ┌──────────┐   byte[]   ┌─────────────────┐    I    ┌──────────────┐  │
│  │          │ ─────────► │ Deserialization │ ──────► │    Domain    │  │
│  │  Iggy    │            │     Stage       │         │    Stage     │  │
│  │  Source  │            │  (abstract)     │         │  (abstract)  │  │
│  │  (TCP)   │            │                 │         │              │  │
│  └──────────┘            └─────────────────┘         └──────┬───────┘  │
│  fromSource()                                                │  O       │
│                                                              ▼          │
│  ┌──────────┐   byte[]   ┌─────────────────┐               ...         │
│  │  Iggy    │ ◄───────── │  Serialization  │                           │
│  │  Sink    │            │     Stage       │                           │
│  │  (TCP)   │            │  (abstract)     │                           │
│  └──────────┘            └─────────────────┘                           │
│  sinkTo()                                                               │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### Configuration Layer

| Class | Purpose |
|-------|---------|
| `IggyConnectionConfig` | TCP host, port, credentials, timeouts |
| `IggySourceConfig` | Source stream, topic, consumer ID, poll settings |
| `IggySinkConfig` | Sink stream, topic, partitioning strategy |

All configs are immutable, built via the Builder pattern, and implement `Serializable`
for Flink serialization.

### Pipeline Abstractions

| Class | Signature | Purpose |
|-------|-----------|---------|
| `DeserializationStage<T>` | `byte[] → T` | Decode raw bytes from Iggy |
| `DomainStage<I, O>` | `I → O` | Apply business logic (implements Flink `MapFunction`) |
| `SerializationStage<T>` | `T → byte[]` | Encode processed output for Iggy |

Users extend these abstract classes to provide their own logic.

### Iggy Source (Flink 2.0 Source API)

The source implements the Flink 2.0 `Source<byte[], IggySplit, Collection<IggySplit>>` interface:

- **`IggySplit`** — represents one topic/partition work unit
- **`IggySplitSerializer`** — checkpoint serialization for splits
- **`IggySplitEnumerator`** — assigns splits to reader subtasks
- **`IggySourceReader`** — polls Iggy via TCP, emits `byte[]` payloads
- **`IggySource`** — `CONTINUOUS_UNBOUNDED` (runs forever until cancelled)

### Iggy Sink (Flink 2.0 Sink API)

- **`IggySink`** — `Sink<byte[]>` factory
- **`IggySinkWriter`** — `SinkWriter<byte[]>`, uses `IggyTcpClient` to publish

### Format Serializers

Built-in implementations for common binary formats:

| Format | Deserializer | Serializer |
|--------|-------------|-----------|
| Avro   | `AvroDeserializationStage<T extends SpecificRecord>` | `AvroSerializationStage<T>` |
| Protobuf | `ProtobufDeserializationStage<T extends Message>` | `ProtobufSerializationStage<T>` |

## Data Flow

```
Iggy Source Topic
       │
       │  TCP poll (PollingStrategy.NEXT)
       ▼
IggySourceReader.pollNext()
       │  byte[]
       ▼
DeserializationStage.deserialize(byte[])  → user-defined
       │  <I>
       ▼
DomainStage.process(I)                    → user-defined
       │  <O>
       ▼
SerializationStage.serialize(O)           → user-defined
       │  byte[]
       ▼
IggySinkWriter.write(byte[])
       │
       │  TCP send
       ▼
Iggy Sink Topic
```

## TCP-Only Constraint

The Iggy Java SDK (`iggy 0.6.0`) supports TCP, HTTP, and QUIC transports. This project
exclusively uses `IggyTcpClient` from `org.apache.iggy.client.blocking.tcp`. The HTTP
client is never instantiated. All Docker Compose and Helm configurations disable HTTP
and QUIC on the server side.

## Database Connectivity (Future)

The `DomainStage<I, O>` is the designated integration point for database connectivity.
To add database access:

1. Extend `DomainStage` in your application
2. Inject a repository or connection pool via the constructor
3. Override `process()` to perform lookups or writes

Example:
```java
public class EnrichmentStage extends DomainStage<RawEvent, EnrichedEvent> {
    private final transient CustomerRepository repository;

    public EnrichmentStage(CustomerRepository repository) {
        this.repository = repository;
    }

    @Override
    public EnrichedEvent process(RawEvent input) throws Exception {
        Customer customer = repository.findById(input.getCustomerId());
        return new EnrichedEvent(input, customer);
    }
}
```

The architecture supports JDBC, Redis, Cassandra, or any other data store by
injecting the appropriate client into your `DomainStage` implementation.

## Testing Strategy

### Unit Tests
Requirements-driven tests using JUnit 5 + AssertJ + Mockito:
- Config validation (builder constraints, defaults)
- Pipeline stage behaviour (deserialization, domain logic, serialization)
- Split serialization (Flink checkpoint correctness)
- Avro/Protobuf round-trips (field preservation, binary compactness)

### End-to-End Tests
Gherkin scenarios using Cucumber + Testcontainers:
- Real Iggy server (`apache/iggy:latest`) started via Testcontainers
- TCP connectivity, message publish/consume
- Full pipeline end-to-end (identity transformation)
- Avro format pipeline round-trip
- Protobuf format pipeline round-trip

Run E2E tests with: `mvn verify`
