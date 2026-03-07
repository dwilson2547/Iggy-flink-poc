# Iggy Flink POC — Implementation Plan

## Legend
- [x] Completed
- [ ] Pending

---

## Phase 1: Foundation

- [x] Create Maven project (Java 17, Flink 2.0.0, Iggy SDK 0.6.0, Avro, Protobuf)
- [x] Configure Maven Shade plugin for fat-JAR packaging
- [x] Configure Avro Maven plugin for schema generation (`src/main/avro`, `src/test/avro`)
- [x] Configure Protobuf Maven plugin for proto compilation (`src/main/proto`, `src/test/proto`)
- [x] Add `.gitignore` to exclude build artifacts

---

## Phase 2: Core Configuration

- [x] `IggyConnectionConfig` — TCP host, port, credentials, timeouts
- [x] `IggySourceConfig` — stream name, topic, consumer ID, batch size, poll interval
- [x] `IggySinkConfig` — stream name, topic, partitioning strategy

---

## Phase 3: Pipeline Abstractions

- [x] `DeserializationStage<T>` — abstract: `byte[] → T`
- [x] `DomainStage<I, O>` — abstract: `I → O` (implements Flink `MapFunction`)
- [x] `SerializationStage<T>` — abstract: `T → byte[]`

---

## Phase 4: Iggy Flink Source (Flink 2.0 Source API)

- [x] `IggySplit` — `SourceSplit` representing one stream/topic/partition
- [x] `IggySplitSerializer` — `SimpleVersionedSerializer<IggySplit>` for checkpointing
- [x] `IggySplitEnumerator` — discovers and assigns splits to readers
- [x] `IggySourceReader` — polls Iggy via TCP, emits `byte[]`
- [x] `IggySource` — `Source<byte[], IggySplit, Collection<IggySplit>>`, `CONTINUOUS_UNBOUNDED`

---

## Phase 5: Iggy Flink Sink (Flink 2.0 Sink API)

- [x] `IggyMessageFactory` — creates `Message` from raw `byte[]` (handles binary formats)
- [x] `IggySinkWriter` — `SinkWriter<byte[]>`, publishes to Iggy via TCP
- [x] `IggySink` — `Sink<byte[]>`, factory for `IggySinkWriter`

---

## Phase 6: Pipeline Orchestrator

- [x] `IggyFlinkPipeline<I, O>` — wires all stages using `env.fromSource()` and `stream.sinkTo()`
- [x] `IggyFlinkPipelineMain` — example entry point with pass-through identity stages
- [x] `IggyTcpClientFactory` — factory enforcing TCP-only connections

---

## Phase 7: Format Serializers

- [x] `AvroDeserializationStage<T>` — Avro binary → `SpecificRecord`
- [x] `AvroSerializationStage<T>` — `SpecificRecord` → Avro binary
- [x] `ProtobufDeserializationStage<T>` — Protobuf binary → `Message`
- [x] `ProtobufSerializationStage<T>` — `Message` → Protobuf binary
- [x] Example Avro schema: `SensorReading.avsc`
- [x] Example Protobuf schema: `SensorReading.proto`

---

## Phase 8: Unit Tests

- [x] `ConfigurationTest` — validates all config builder rules and defaults
- [x] `PipelineStageTest` — validates stage abstractions and composition
- [x] `IggyMessageFactoryTest` — validates binary message creation
- [x] `IggySplitSerializerTest` — validates split serialization for checkpointing
- [x] `AvroSerializationTest` — validates Avro round-trip with edge cases
- [x] `ProtobufSerializationTest` — validates Protobuf round-trip with edge cases

---

## Phase 9: End-to-End Tests (Cucumber + Testcontainers)

- [x] `IggyTestContext` — shared Testcontainers Iggy server for all E2E scenarios
- [x] `CucumberE2ERunner` — JUnit Platform Suite runner
- [x] Feature: `pipeline_connectivity.feature` — TCP connect, sink write, source read, full pipeline
- [x] Feature: `avro_pipeline.feature` — Avro encode/decode, full round-trip, batch
- [x] Feature: `protobuf_pipeline.feature` — Protobuf encode/decode, full round-trip, compactness
- [x] Step definitions: `PipelineConnectivitySteps`, `AvroPipelineSteps`, `ProtobufPipelineSteps`

---

## Phase 10: Local Development

- [x] `docker-compose.yml` — Apache Iggy server with TCP-only transport, health check

---

## Phase 11: Deployment

- [x] `deploy/flink/flink-deployment.yaml` — Kubernetes Flink cluster + job submission
- [x] `deploy/helm/values.yaml` — Helm chart values with Flink 2.0, Iggy, pipeline config

---

## Phase 12: Documentation

- [x] `docs/implementation-plan.md` (this file)
- [x] `docs/architecture.md` — system design and design decisions
- [x] `README.md` — quick-start guide

---

## Future Work

- [ ] Database connectivity in `DomainStage` (architecture already prepared)
- [ ] Flink checkpointing / exactly-once semantics for the Iggy source
- [ ] Schema Registry integration for Avro (Confluent / Apicurio)
- [ ] Multi-partition source fan-out (one split per Iggy partition)
- [ ] Metrics integration (Flink metrics → Prometheus)
- [ ] TLS support for Iggy TCP connections
