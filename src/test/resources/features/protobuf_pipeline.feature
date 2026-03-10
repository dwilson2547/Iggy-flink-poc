Feature: Iggy Flink Pipeline - Protobuf Format
  As a developer using the Iggy Flink connector with Protobuf messages
  I want Protobuf-encoded messages to flow through the pipeline without data loss
  So that efficient binary-encoded data is reliably transmitted between Iggy topics

  Background:
    Given an Apache Iggy server is running

  Scenario: Protobuf-encoded messages are deserialized correctly from Iggy
    Given a stream "proto-stream" and topic "proto-input" exist in Iggy
    And a Protobuf-encoded TestEvent with id "proto-001", name "Humidity", value 65.0, timestamp 3000 is published to topic "proto-input"
    When the ProtobufDeserializationStage processes the raw bytes
    Then the deserialized Protobuf TestEvent should have id "proto-001", name "Humidity", value 65.0

  Scenario: Protobuf-encoded messages survive a full pipeline round-trip
    Given a stream "proto-stream" with source topic "proto-source" and sink topic "proto-sink" exist in Iggy
    And a Protobuf-encoded TestEvent with id "proto-rtrip", name "CO2 Level", value 412.5, timestamp 4000 is published to topic "proto-source"
    When the Iggy Flink pipeline runs with a Protobuf identity transformation
    Then a Protobuf-encoded TestEvent with id "proto-rtrip" should appear in sink topic "proto-sink"

  Scenario: Protobuf binary encoding is more compact than JSON
    Given a stream "proto-stream" and topic "proto-compact" exist in Iggy
    And a Protobuf-encoded TestEvent with id "size-test", name "CompactTest", value 1.0, timestamp 1 is published to topic "proto-compact"
    When the raw bytes are read from the topic
    Then the payload size should be smaller than the equivalent JSON representation
