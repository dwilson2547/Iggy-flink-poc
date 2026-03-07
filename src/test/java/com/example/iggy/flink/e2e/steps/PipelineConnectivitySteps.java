package com.example.iggy.flink.e2e.steps;

import com.example.iggy.flink.IggyFlinkPipeline;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import com.example.iggy.flink.pipeline.DeserializationStage;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.pipeline.SerializationStage;
import com.example.iggy.flink.sink.IggyMessageFactory;
import com.example.iggy.flink.sink.IggySinkWriter;
import com.example.iggy.flink.source.IggySplit;
import com.example.iggy.flink.source.IggySourceReader;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Cucumber step definitions for core Iggy Flink pipeline connectivity scenarios.
 */
public class PipelineConnectivitySteps {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineConnectivitySteps.class);

    private IggyTcpClient adminClient;
    private final List<byte[]> capturedOutput = new ArrayList<>();

    @Given("an Apache Iggy server is running")
    public void anApacheIggyServerIsRunning() {
        adminClient = IggyTestContext.createAdminClient();
        assertThat(adminClient).isNotNull();
        LOG.info("Iggy server confirmed running at {}:{}", IggyTestContext.getHost(), IggyTestContext.getPort());
    }

    @Given("a stream {string} and topic {string} exist in Iggy")
    public void aStreamAndTopicExistInIggy(String streamName, String topicName) {
        IggyTestContext.ensureStreamAndTopic(adminClient, streamName, topicName);
    }

    @Given("a stream {string} with source topic {string} and sink topic {string} exist in Iggy")
    public void aStreamWithSourceAndSinkTopicsExist(String streamName, String sourceTopic, String sinkTopic) {
        IggyTestContext.ensureStreamAndTopic(adminClient, streamName, sourceTopic);
        IggyTestContext.ensureStreamAndTopic(adminClient, streamName, sinkTopic);
    }

    @When("I publish the message {string} to the topic via the IggySinkWriter")
    public void iPublishTheMessageViaIggySinkWriter(String messageText) throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("test-stream", "sink-test");

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            writer.write(messageText.getBytes(StandardCharsets.UTF_8), null);
        }
    }

    @Then("the message {string} should be stored in topic {string}")
    public void theMessageShouldBeStoredInTopic(String expectedMessage, String topicName) throws Exception {
        Thread.sleep(500); // allow message propagation
        PolledMessages polled = adminClient.messages().pollMessages(
                StreamId.of("test-stream"),
                TopicId.of(topicName),
                Optional.of(1L),
                Consumer.of(99L),
                PollingStrategy.last(),
                10L,
                false
        );

        assertThat(polled).isNotNull();
        assertThat(polled.messages()).isNotEmpty();

        boolean found = polled.messages().stream()
                .anyMatch(m -> new String(m.payload(), StandardCharsets.UTF_8).contains(expectedMessage));
        assertThat(found).as("Expected message '%s' in topic '%s'", expectedMessage, topicName).isTrue();
    }

    @And("a message {string} has been published to topic {string}")
    public void aMessageHasBeenPublishedToTopic(String messageText, String topicName) throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("test-stream", topicName);

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            writer.write(messageText.getBytes(StandardCharsets.UTF_8), null);
        }
        Thread.sleep(300);
    }

    @When("the IggySourceReader polls for messages")
    public void theIggySourceReaderPollsForMessages() throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySourceConfig sourceConfig = IggyTestContext.sourceConfig("test-stream", "source-test");
        SourceReaderContext mockContext = mock(SourceReaderContext.class);

        IggySourceReader reader = new IggySourceReader(connectionConfig, sourceConfig, mockContext);
        reader.start();
        reader.addSplits(List.of(new IggySplit("test-stream", "source-test", 1, 1L)));

        // Poll a few times to collect messages
        ReaderOutput<byte[]> output = new ReaderOutput<>() {
            @Override
            public void collect(byte[] element) {
                capturedOutput.add(element);
            }
            @Override public void collect(byte[] element, long timestamp) { capturedOutput.add(element); }
            @Override public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}
            @Override public void markIdle() {}
            @Override public void markActive() {}
            @Override public org.apache.flink.api.connector.source.SourceOutput<byte[]> createOutputForSplit(String splitId) { return this; }
            @Override public void releaseOutputForSplit(String splitId) {}
        };
        for (int i = 0; i < 5; i++) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.MORE_AVAILABLE) {
                break;
            }
            Thread.sleep(200);
        }
        reader.close();
    }

    @Then("the reader should emit the message {string}")
    public void theReaderShouldEmitTheMessage(String expectedMessage) {
        boolean found = capturedOutput.stream()
                .anyMatch(b -> new String(b, StandardCharsets.UTF_8).contains(expectedMessage));
        assertThat(found)
                .as("Expected reader to emit '%s', but captured: %d messages", expectedMessage, capturedOutput.size())
                .isTrue();
    }

    @And("the message {string} is published to source topic {string}")
    public void theMessageIsPublishedToSourceTopic(String messageText, String sourceTopic) throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("e2e-stream", sourceTopic);

        try (IggySinkWriter writer = new IggySinkWriter(connectionConfig, sinkConfig)) {
            writer.write(messageText.getBytes(StandardCharsets.UTF_8), null);
        }
        Thread.sleep(300);
    }

    @When("the Iggy Flink pipeline runs with an identity transformation")
    public void theFlinkPipelineRunsWithIdentityTransformation() throws Exception {
        IggyConnectionConfig connectionConfig = IggyTestContext.getConnectionConfig();
        IggySourceConfig sourceConfig = IggyTestContext.sourceConfig("e2e-stream", "pipeline-in");
        IggySinkConfig sinkConfig = IggyTestContext.sinkConfig("e2e-stream", "pipeline-out");

        IggyFlinkPipeline<String, String> pipeline = IggyFlinkPipeline.<String, String>builder()
                .connectionConfig(connectionConfig)
                .sourceConfig(sourceConfig)
                .sinkConfig(sinkConfig)
                .deserializationStage(new PassThroughDeserializer())
                .domainStage(new PassThroughDomain())
                .serializationStage(new PassThroughSerializer())
                .parallelism(1)
                .build();

        runPipelineWithTimeout(pipeline, "e2e-identity-test", Duration.ofSeconds(8));
    }

    @Then("the message {string} should appear in sink topic {string}")
    public void theMessageShouldAppearInSinkTopic(String expectedMessage, String sinkTopic) throws Exception {
        Thread.sleep(500);
        PolledMessages polled = adminClient.messages().pollMessages(
                StreamId.of("e2e-stream"),
                TopicId.of(sinkTopic),
                Optional.of(1L),
                Consumer.of(99L),
                PollingStrategy.last(),
                10L,
                false
        );

        assertThat(polled).isNotNull();
        assertThat(polled.messages()).isNotEmpty();
        boolean found = polled.messages().stream()
                .anyMatch(m -> new String(m.payload(), StandardCharsets.UTF_8).contains(expectedMessage));
        assertThat(found).as("Expected '%s' in sink topic '%s'", expectedMessage, sinkTopic).isTrue();
    }

    // --- Helpers ---

    private void runPipelineWithTimeout(IggyFlinkPipeline<?, ?> pipeline, String jobName, Duration timeout)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        pipeline.buildPipeline(env);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            try {
                env.execute(jobName);
            } catch (Exception e) {
                // Expected when we cancel
                LOG.debug("Pipeline job stopped: {}", e.getMessage());
            }
        });

        Thread.sleep(timeout.toMillis());
        future.cancel(true);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    // Identity pipeline stages for E2E testing
    private static class PassThroughDeserializer extends DeserializationStage<String> {
        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static class PassThroughDomain extends DomainStage<String, String> {
        @Override
        public String process(String input) {
            return input;
        }
    }

    private static class PassThroughSerializer extends SerializationStage<String> {
        @Override
        public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }
}
