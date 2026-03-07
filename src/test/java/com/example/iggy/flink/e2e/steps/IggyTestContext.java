package com.example.iggy.flink.e2e.steps;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Optional;

/**
 * Shared Testcontainers setup for all Cucumber E2E scenarios.
 *
 * <p>Starts a single Apache Iggy server container shared across the test suite.
 * The container is started once and reused across all scenarios for performance.
 *
 * <p>Only TCP transport is used, matching the application's requirement.
 */
public class IggyTestContext {

    private static final Logger LOG = LoggerFactory.getLogger(IggyTestContext.class);

    private static final String IGGY_IMAGE = "apache/iggy:latest";
    private static final int IGGY_TCP_PORT = 8090;
    private static final String IGGY_USERNAME = "iggy";
    private static final String IGGY_PASSWORD = "iggy";

    @SuppressWarnings("resource")
    private static final GenericContainer<?> IGGY_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(IGGY_IMAGE))
                    .withExposedPorts(IGGY_TCP_PORT)
                    .withEnv("IGGY_ROOT_USERNAME", IGGY_USERNAME)
                    .withEnv("IGGY_ROOT_PASSWORD", IGGY_PASSWORD)
                    .withEnv("IGGY_TCP_ENABLED", "true")
                    .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:" + IGGY_TCP_PORT)
                    .withEnv("IGGY_HTTP_ENABLED", "false")
                    .withEnv("IGGY_QUIC_ENABLED", "false")
                    .waitingFor(
                            Wait.forListeningPort()
                                    .withStartupTimeout(Duration.ofSeconds(60))
                    );

    static {
        IGGY_CONTAINER.start();
        LOG.info("Iggy test container started: {}:{}",
                IGGY_CONTAINER.getHost(),
                IGGY_CONTAINER.getMappedPort(IGGY_TCP_PORT));
    }

    private IggyTestContext() {}

    public static String getHost() {
        return IGGY_CONTAINER.getHost();
    }

    public static int getPort() {
        return IGGY_CONTAINER.getMappedPort(IGGY_TCP_PORT);
    }

    public static IggyConnectionConfig getConnectionConfig() {
        return IggyConnectionConfig.builder()
                .host(getHost())
                .port(getPort())
                .username(IGGY_USERNAME)
                .password(IGGY_PASSWORD)
                .build();
    }

    public static IggyTcpClient createAdminClient() {
        IggyTcpClient client = IggyTcpClient.builder()
                .host(getHost())
                .port(getPort())
                .build();
        client.users().login(IGGY_USERNAME, IGGY_PASSWORD);
        return client;
    }

    /**
     * Creates a stream and topic if they don't already exist, then returns
     * the corresponding configs for source and sink.
     */
    public static void ensureStreamAndTopic(IggyTcpClient client, String streamName, String topicName) {
        try {
            if (client.streams().getStream(StreamId.of(streamName)).isEmpty()) {
                client.streams().createStream(streamName);
                LOG.info("Created stream: {}", streamName);
            }
        } catch (Exception e) {
            try {
                client.streams().createStream(streamName);
            } catch (Exception ignored) {}
        }

        try {
            if (client.topics().getTopic(StreamId.of(streamName), TopicId.of(topicName)).isEmpty()) {
                client.topics().createTopic(
                        StreamId.of(streamName),
                        1L,                        // partitions
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,           // message expiry (none)
                        BigInteger.ZERO,           // max topic size (unlimited)
                        Optional.empty(),          // replication factor
                        topicName
                );
                LOG.info("Created topic: {}/{}", streamName, topicName);
            }
        } catch (Exception e) {
            try {
                client.topics().createTopic(
                        StreamId.of(streamName),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        topicName
                );
            } catch (Exception ignored) {}
        }
    }

    public static IggySourceConfig sourceConfig(String streamName, String topicName) {
        return IggySourceConfig.builder()
                .streamName(streamName)
                .topicName(topicName)
                .consumerId(1L)
                .batchSize(10)
                .pollInterval(Duration.ofMillis(200))
                .build();
    }

    public static IggySinkConfig sinkConfig(String streamName, String topicName) {
        return IggySinkConfig.builder()
                .streamName(streamName)
                .topicName(topicName)
                .build();
    }
}
