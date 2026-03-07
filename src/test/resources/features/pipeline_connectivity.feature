Feature: Iggy Flink Pipeline - Core Connectivity
  As a developer using the Iggy Flink connector
  I want to publish messages to and consume messages from Apache Iggy via TCP
  So that data flows reliably through the pipeline

  Background:
    Given an Apache Iggy server is running

  Scenario: Sink writer publishes raw bytes to Iggy via TCP
    Given a stream "test-stream" and topic "sink-test" exist in Iggy
    When I publish the message "hello-from-sink" to the topic via the IggySinkWriter
    Then the message "hello-from-sink" should be stored in topic "sink-test"

  Scenario: Source reader receives messages from Iggy via TCP
    Given a stream "test-stream" and topic "source-test" exist in Iggy
    And a message "hello-from-source" has been published to topic "source-test"
    When the IggySourceReader polls for messages
    Then the reader should emit the message "hello-from-source"

  Scenario: Full pipeline processes messages end-to-end
    Given a stream "e2e-stream" with source topic "pipeline-in" and sink topic "pipeline-out" exist in Iggy
    And the message "pipeline-test-message" is published to source topic "pipeline-in"
    When the Iggy Flink pipeline runs with an identity transformation
    Then the message "pipeline-test-message" should appear in sink topic "pipeline-out"
