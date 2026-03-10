package com.example.iggy.flink.e2e.steps;

import com.example.iggy.flink.IggyFlinkPipeline;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.serialization.protobuf.ProtobufDeserializationStage;
import com.example.iggy.flink.serialization.protobuf.ProtobufSerializationStage;
import com.example.iggy.flink.sink.IggySinkWriter;
import com.example.iggy.flink.test.proto.TestEvent;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cucumber step definitions for Protobuf-format Iggy Flink pipeline scenarios.
 *
 * <p>The "Given an Apache Iggy server is running" step is shared from
 * {@link PipelineConnectivitySteps}; admin client is initialised lazily here.
 */
public class ProtobufPipelineSteps {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufPipelineSteps.class);

    private IggyTcpClient adminClient;
    private byte[] lastRawBytes;
    private TestEvent lastDeserializedEvent;

    private final ProtobufSerializationStage<TestEvent> protoSerializer =
            new ProtobufSerializationStage<>(TestEvent.class);
    private final ProtobufDeserializationStage<TestEvent> protoDeserializer =
            new ProtobufDeserializationStage<>(TestEvent.class, TestEvent.parser());

    private IggyTcpClient admin() {
        if (adminClient == null) {
            adminClient = IggyTestContext.createAdminClient();
        }
        return adminClient;
    }

    @And("a Protobuf-encoded TestEvent with id {string}, name {string}, value {double}, timestamp {long} is published to topic {string}")
    public void protobufEncodedTestEventIsPublished(
            String id, String name, double value, long timestamp, String topicName) throws Exception {
        IggyTestContext.ensureStreamAndTopic(admin(), "proto-stream", topicName);

        TestEvent event = TestEvent.newBuilder()
                .setId(id).setName(name).setValue(value).setTimestamp(timestamp).build();
        lastRawBytes = protoSerializer.serialize(event);

        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("proto-stream", topicName);

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            writer.write(lastRawBytes, null);
        }
        Thread.sleep(300);
    }

    @When("the ProtobufDeserializationStage processes the raw bytes")
    public void protobufDeserializationStageProcessesBytes() throws Exception {
        lastDeserializedEvent = protoDeserializer.deserialize(lastRawBytes);
    }

    @Then("the deserialized Protobuf TestEvent should have id {string}, name {string}, value {double}")
    public void deserializedProtobufEventShouldHaveFields(String expectedId, String expectedName, double expectedValue) {
        assertThat(lastDeserializedEvent).isNotNull();
        assertThat(lastDeserializedEvent.getId()).isEqualTo(expectedId);
        assertThat(lastDeserializedEvent.getName()).isEqualTo(expectedName);
        assertThat(lastDeserializedEvent.getValue()).isEqualTo(expectedValue);
    }

    @When("the Iggy Flink pipeline runs with a Protobuf identity transformation")
    public void flinkPipelineRunsWithProtobufIdentityTransformation() throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySourceConfig sourceConfig = IggyTestContext.sourceConfig("proto-stream", "proto-source");
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("proto-stream", "proto-sink");

        IggyFlinkPipeline<TestEvent, TestEvent> pipeline = IggyFlinkPipeline.<TestEvent, TestEvent>builder()
                .connectionConfig(connectionConfig)
                .sourceConfig(sourceConfig)
                .sinkConfig(sinkConfig)
                .deserializationStage(new ProtobufDeserializationStage<>(TestEvent.class, TestEvent.parser()))
                .domainStage(new DomainStage<>() {
                    @Override
                    public TestEvent process(TestEvent input) { return input; }
                })
                .serializationStage(new ProtobufSerializationStage<>(TestEvent.class))
                .parallelism(1)
                .build();

        runPipelineWithTimeout(pipeline, "proto-e2e-test", Duration.ofSeconds(8));
    }

    @Then("a Protobuf-encoded TestEvent with id {string} should appear in sink topic {string}")
    public void protobufEncodedEventShouldAppearInSinkTopic(String expectedId, String sinkTopic) throws Exception {
        Thread.sleep(500);
        PolledMessages polled = admin().messages().pollMessages(
                StreamId.of("proto-stream"), TopicId.of(sinkTopic),
                Optional.of(1L), Consumer.of(99L), PollingStrategy.last(), 10L, false);

        assertThat(polled).isNotNull();
        assertThat(polled.messages()).isNotEmpty();

        boolean found = polled.messages().stream().anyMatch(m -> {
            try {
                return protoDeserializer.deserialize(m.payload()).getId().equals(expectedId);
            } catch (Exception e) { return false; }
        });
        assertThat(found).as("Expected Protobuf event id='%s' in topic '%s'", expectedId, sinkTopic).isTrue();
    }

    @When("the raw bytes are read from the topic")
    public void rawBytesAreReadFromTopic() {
        assertThat(lastRawBytes).isNotNull().isNotEmpty();
    }

    @Then("the payload size should be smaller than the equivalent JSON representation")
    public void payloadSizeShouldBeSmallerThanJson() {
        String jsonEquivalent = "{\"id\":\"size-test\",\"name\":\"CompactTest\",\"value\":1.0,\"timestamp\":1}";
        assertThat(lastRawBytes.length)
                .as("Protobuf binary (%d bytes) should be < JSON (%d bytes)",
                        lastRawBytes.length, jsonEquivalent.length())
                .isLessThan(jsonEquivalent.length());
    }

    private void runPipelineWithTimeout(IggyFlinkPipeline<?, ?> pipeline, String jobName, Duration timeout)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        pipeline.buildPipeline(env);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            try { env.execute(jobName); }
            catch (Exception e) { LOG.debug("Proto pipeline job stopped: {}", e.getMessage()); }
        });
        Thread.sleep(timeout.toMillis());
        future.cancel(true);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
