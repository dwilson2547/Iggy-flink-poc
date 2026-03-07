Feature: Iggy Flink Pipeline - Avro Format
  As a developer using the Iggy Flink connector with Avro messages
  I want Avro-encoded messages to flow through the pipeline without data loss
  So that structured data is reliably transmitted between Iggy topics

  Background:
    Given an Apache Iggy server is running

  Scenario: Avro-encoded messages are deserialized correctly from Iggy
    Given a stream "avro-stream" and topic "avro-input" exist in Iggy
    And an Avro-encoded TestEvent with id "avro-001", name "Temperature", value 22.5, timestamp 1000 is published to topic "avro-input"
    When the AvroDeserializationStage processes the raw bytes
    Then the deserialized TestEvent should have id "avro-001", name "Temperature", value 22.5

  Scenario: Avro-encoded messages survive a full pipeline round-trip
    Given a stream "avro-stream" with source topic "avro-source" and sink topic "avro-sink" exist in Iggy
    And an Avro-encoded TestEvent with id "avro-rtrip", name "Pressure", value 101.3, timestamp 2000 is published to topic "avro-source"
    When the Iggy Flink pipeline runs with an Avro identity transformation
    Then an Avro-encoded TestEvent with id "avro-rtrip" should appear in sink topic "avro-sink"

  Scenario: Multiple Avro messages are processed in sequence
    Given a stream "avro-stream" and topic "avro-batch" exist in Iggy
    And 3 Avro-encoded TestEvents are published to topic "avro-batch"
    When the AvroDeserializationStage processes each message
    Then all 3 TestEvents should be deserialized with correct field values
