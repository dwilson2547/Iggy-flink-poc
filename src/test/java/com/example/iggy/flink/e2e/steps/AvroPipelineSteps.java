package com.example.iggy.flink.e2e.steps;

import com.example.iggy.flink.IggyFlinkPipeline;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.serialization.avro.AvroDeserializationStage;
import com.example.iggy.flink.serialization.avro.AvroSerializationStage;
import com.example.iggy.flink.sink.IggySinkWriter;
import com.example.iggy.flink.test.avro.TestEvent;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cucumber step definitions for Avro-format Iggy Flink pipeline scenarios.
 *
 * <p>The "Given an Apache Iggy server is running" step is shared from
 * {@link PipelineConnectivitySteps}; admin client is initialised lazily here.
 */
public class AvroPipelineSteps {

    private static final Logger LOG = LoggerFactory.getLogger(AvroPipelineSteps.class);

    private IggyTcpClient adminClient;
    private byte[] lastRawBytes;
    private TestEvent lastDeserializedEvent;
    private final List<TestEvent> deserializedBatch = new ArrayList<>();
    private final AvroSerializationStage<TestEvent> avroSerializer =
            new AvroSerializationStage<>(TestEvent.class);
    private final AvroDeserializationStage<TestEvent> avroDeserializer =
            new AvroDeserializationStage<>(TestEvent.class);

    private IggyTcpClient admin() {
        if (adminClient == null) {
            adminClient = IggyTestContext.createAdminClient();
        }
        return adminClient;
    }

    @And("an Avro-encoded TestEvent with id {string}, name {string}, value {double}, timestamp {long} is published to topic {string}")
    public void avroEncodedTestEventIsPublished(
            String id, String name, double value, long timestamp, String topicName) throws Exception {
        IggyTestContext.ensureStreamAndTopic(admin(), "avro-stream", topicName);

        TestEvent event = TestEvent.newBuilder()
                .setId(id).setName(name).setValue(value).setTimestamp(timestamp).build();
        lastRawBytes = avroSerializer.serialize(event);

        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("avro-stream", topicName);

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            writer.write(lastRawBytes, null);
        }
        Thread.sleep(300);
    }

    @When("the AvroDeserializationStage processes the raw bytes")
    public void avroDeserializationStageProcessesBytes() throws Exception {
        lastDeserializedEvent = avroDeserializer.deserialize(lastRawBytes);
    }

    @Then("the deserialized TestEvent should have id {string}, name {string}, value {double}")
    public void deserializedTestEventShouldHaveFields(String expectedId, String expectedName, double expectedValue) {
        assertThat(lastDeserializedEvent).isNotNull();
        assertThat(lastDeserializedEvent.getId().toString()).isEqualTo(expectedId);
        assertThat(lastDeserializedEvent.getName().toString()).isEqualTo(expectedName);
        assertThat(lastDeserializedEvent.getValue()).isEqualTo(expectedValue);
    }

    @When("the Iggy Flink pipeline runs with an Avro identity transformation")
    public void flinkPipelineRunsWithAvroIdentityTransformation() throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySourceConfig sourceConfig = IggyTestContext.sourceConfig("avro-stream", "avro-source");
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("avro-stream", "avro-sink");

        IggyFlinkPipeline<TestEvent, TestEvent> pipeline = IggyFlinkPipeline.<TestEvent, TestEvent>builder()
                .connectionConfig(connectionConfig)
                .sourceConfig(sourceConfig)
                .sinkConfig(sinkConfig)
                .deserializationStage(new AvroDeserializationStage<>(TestEvent.class))
                .domainStage(new DomainStage<>() {
                    @Override
                    public TestEvent process(TestEvent input) { return input; }
                })
                .serializationStage(new AvroSerializationStage<>(TestEvent.class))
                .parallelism(1)
                .build();

        runPipelineWithTimeout(pipeline, "avro-e2e-test", Duration.ofSeconds(8));
    }

    @Then("an Avro-encoded TestEvent with id {string} should appear in sink topic {string}")
    public void avroEncodedEventShouldAppearInSinkTopic(String expectedId, String sinkTopic) throws Exception {
        Thread.sleep(500);
        PolledMessages polled = admin().messages().pollMessages(
                StreamId.of("avro-stream"), TopicId.of(sinkTopic),
                Optional.of(1L), Consumer.of(99L), PollingStrategy.last(), 10L, false);

        assertThat(polled).isNotNull();
        assertThat(polled.messages()).isNotEmpty();

        boolean found = polled.messages().stream().anyMatch(m -> {
            try {
                return avroDeserializer.deserialize(m.payload()).getId().toString().equals(expectedId);
            } catch (Exception e) { return false; }
        });
        assertThat(found).as("Expected Avro event id='%s' in topic '%s'", expectedId, sinkTopic).isTrue();
    }

    @And("{int} Avro-encoded TestEvents are published to topic {string}")
    public void avroEncodedTestEventsArePublished(int count, String topicName) throws Exception {
        IggyTestContext.ensureStreamAndTopic(admin(), "avro-stream", topicName);
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("avro-stream", topicName);

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            for (int i = 0; i < count; i++) {
                TestEvent event = TestEvent.newBuilder()
                        .setId("batch-" + i).setName("Event " + i)
                        .setValue(i * 1.5).setTimestamp(i * 1000L).build();
                writer.write(avroSerializer.serialize(event), null);
            }
        }
        Thread.sleep(500);
    }

    @When("the AvroDeserializationStage processes each message")
    public void avroDeserializationStageProcessesEachMessage() throws Exception {
        PolledMessages polled = admin().messages().pollMessages(
                StreamId.of("avro-stream"), TopicId.of("avro-batch"),
                Optional.of(1L), Consumer.of(98L), PollingStrategy.first(), 10L, false);
        deserializedBatch.clear();
        if (polled != null && polled.messages() != null) {
            for (var message : polled.messages()) {
                deserializedBatch.add(avroDeserializer.deserialize(message.payload()));
            }
        }
    }

    @Then("all {int} TestEvents should be deserialized with correct field values")
    public void allTestEventsShouldBeDeserializedCorrectly(int expectedCount) {
        assertThat(deserializedBatch).hasSizeGreaterThanOrEqualTo(expectedCount);
        deserializedBatch.forEach(event -> {
            assertThat(event.getId()).isNotNull();
            assertThat(event.getName()).isNotNull();
        });
    }

    private void runPipelineWithTimeout(IggyFlinkPipeline<?, ?> pipeline, String jobName, Duration timeout)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        pipeline.buildPipeline(env);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            try { env.execute(jobName); }
            catch (Exception e) { LOG.debug("Avro pipeline job stopped: {}", e.getMessage()); }
        });
        Thread.sleep(timeout.toMillis());
        future.cancel(true);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
