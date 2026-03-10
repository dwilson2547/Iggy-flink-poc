# Iggy Flink POC

An Apache Flink 2.0 application that subscribes to an Apache Iggy topic via TCP, processes
messages through a configurable pipeline, and publishes results to another Iggy topic.

## Features

- **Flink 2.0** — uses the modern `Source`/`Sink` V2 APIs (no deprecated `SourceFunction`/`SinkFunction`)
- **TCP-only** — all Iggy communication uses the TCP transport (HTTP/REST/QUIC disabled)
- **Configurable pipeline** — abstract `DeserializationStage`, `DomainStage`, and `SerializationStage` for custom logic
- **Built-in Avro & Protobuf** serializers/deserializers
- **Fully tested** — 49 unit tests + end-to-end Cucumber/Testcontainers scenarios
- **Docker Compose** for local Iggy server
- **Kubernetes + Helm** deployment examples

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker (for local Iggy server and E2E tests)

### 1. Start the local Iggy server

```bash
docker compose up -d
```

The Iggy TCP server will be available at `localhost:8090`.
Default credentials: `iggy` / `iggy`.

### 2. Build the project

```bash
mvn package -DskipTests
```

### 3. Run unit tests

```bash
mvn test
```

### 4. Run end-to-end tests (requires Docker)

```bash
mvn verify
```

E2E tests start their own Iggy container via Testcontainers — no manual server setup needed.

---

## Project Structure

```
iggy-flink-poc/
├── pom.xml                          # Maven project (Java 17, Flink 2.0.0, Iggy 0.6.0)
├── docker-compose.yml               # Local Iggy server (TCP-only)
├── docs/
│   ├── architecture.md              # System design and design decisions
│   └── implementation-plan.md       # Checklist-style implementation progress
├── deploy/
│   ├── flink/flink-deployment.yaml  # Kubernetes Flink cluster + job submission
│   └── helm/values.yaml             # Helm chart values
├── src/main/
│   ├── avro/SensorReading.avsc      # Example Avro schema
│   ├── proto/SensorReading.proto    # Example Protobuf schema
│   └── java/com/example/iggy/flink/
│       ├── IggyFlinkPipeline.java   # Pipeline orchestrator
│       ├── IggyFlinkPipelineMain.java
│       ├── config/                  # IggyConnectionConfig, IggySourceConfig, IggySinkConfig
│       ├── pipeline/                # DeserializationStage, DomainStage, SerializationStage
│       ├── source/                  # IggySource, IggySourceReader, IggySplit, ...
│       ├── sink/                    # IggySink, IggySinkWriter, IggyMessageFactory
│       ├── serialization/avro/      # AvroDeserializationStage, AvroSerializationStage
│       ├── serialization/protobuf/  # ProtobufDeserializationStage, ProtobufSerializationStage
│       └── client/                  # IggyTcpClientFactory
└── src/test/
    ├── avro/TestEvent.avsc
    ├── proto/TestEvent.proto
    ├── resources/features/          # Gherkin feature files
    └── java/com/example/iggy/flink/
        ├── unit/                    # JUnit 5 unit tests
        └── e2e/                     # Cucumber + Testcontainers E2E tests
```

---

## Writing Your Own Pipeline

### 1. Implement the three pipeline stages

```java
// Deserialize from Iggy bytes into your domain model
public class OrderDeserializer extends DeserializationStage<OrderEvent> {
    private final AvroDeserializationStage<OrderEvent> avro =
        new AvroDeserializationStage<>(OrderEvent.class);

    @Override
    public OrderEvent deserialize(byte[] bytes) throws Exception {
        return avro.deserialize(bytes);
    }
}

// Apply business logic (future: inject a database repository here)
public class OrderEnrichmentStage extends DomainStage<OrderEvent, EnrichedOrder> {
    @Override
    public EnrichedOrder process(OrderEvent input) throws Exception {
        return new EnrichedOrder(input, "enriched");
    }
}

// Serialize output back to bytes for the sink topic
public class EnrichedOrderSerializer extends SerializationStage<EnrichedOrder> {
    private final ProtobufSerializationStage<EnrichedOrder> proto =
        new ProtobufSerializationStage<>(EnrichedOrder.class);

    @Override
    public byte[] serialize(EnrichedOrder value) throws Exception {
        return proto.serialize(value);
    }
}
```

### 2. Build and run the pipeline

```java
IggyFlinkPipeline.<OrderEvent, EnrichedOrder>builder()
    .connectionConfig(IggyConnectionConfig.builder()
        .host("localhost").port(8090).username("iggy").password("iggy").build())
    .sourceConfig(IggySourceConfig.builder()
        .streamName("orders").topicName("raw").build())
    .sinkConfig(IggySinkConfig.builder()
        .streamName("orders").topicName("enriched").build())
    .deserializationStage(new OrderDeserializer())
    .domainStage(new OrderEnrichmentStage())
    .serializationStage(new EnrichedOrderSerializer())
    .build()
    .execute("Order Enrichment Pipeline");
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IGGY_HOST` | `localhost` | Iggy server hostname |
| `IGGY_PORT` | `8090` | Iggy TCP port |
| `IGGY_USERNAME` | `iggy` | Iggy username |
| `IGGY_PASSWORD` | `iggy` | Iggy password |
| `SOURCE_STREAM` | `events` | Source stream name |
| `SOURCE_TOPIC` | `input` | Source topic name |
| `SINK_STREAM` | `events` | Sink stream name |
| `SINK_TOPIC` | `output` | Sink topic name |

---

## Deployment

See [`deploy/flink/flink-deployment.yaml`](deploy/flink/flink-deployment.yaml) for
Kubernetes and [`deploy/helm/values.yaml`](deploy/helm/values.yaml) for Helm.

---

## Documentation

- [Architecture](docs/architecture.md)
- [Implementation Plan](docs/implementation-plan.md)
