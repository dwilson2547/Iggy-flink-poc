package com.example.iggy.flink.unit.config;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for configuration classes.
 *
 * <p>Validates that configuration builders correctly enforce required fields,
 * apply defaults, and produce well-formed configurations.
 */
class ConfigurationTest {

    @Test
    void connectionConfig_buildsWithDefaults() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config.getHost()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(8090);
        assertThat(config.getUsername()).isEqualTo("iggy");
        assertThat(config.getPassword()).isEqualTo("iggy");
        assertThat(config.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void connectionConfig_buildsWithCustomValues() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .host("iggy-server.example.com")
                .port(9090)
                .username("admin")
                .password("secret")
                .connectionTimeout(Duration.ofSeconds(5))
                .requestTimeout(Duration.ofSeconds(15))
                .build();

        assertThat(config.getHost()).isEqualTo("iggy-server.example.com");
        assertThat(config.getPort()).isEqualTo(9090);
        assertThat(config.getUsername()).isEqualTo("admin");
        assertThat(config.getPassword()).isEqualTo("secret");
        assertThat(config.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(15));
    }

    @Test
    void connectionConfig_rejectsNullHost() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                .host(null)
                .username("iggy")
                .password("iggy")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void connectionConfig_rejectsNullUsername() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                .username(null)
                .password("iggy")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void connectionConfig_rejectsNullPassword() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                .username("iggy")
                .password(null)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sourceConfig_buildsWithDefaults() {
        IggySourceConfig config = IggySourceConfig.builder()
                .streamName("my-stream")
                .topicName("my-topic")
                .build();

        assertThat(config.getStreamName()).isEqualTo("my-stream");
        assertThat(config.getTopicName()).isEqualTo("my-topic");
        assertThat(config.getConsumerId()).isEqualTo(1L);
        assertThat(config.getBatchSize()).isEqualTo(100);
        assertThat(config.getPollInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.isAutoCommitOffset()).isTrue();
        assertThat(config.getPartitionId()).isEqualTo(1);
    }

    @Test
    void sourceConfig_buildsWithCustomValues() {
        IggySourceConfig config = IggySourceConfig.builder()
                .streamName("events")
                .topicName("orders")
                .consumerId(42L)
                .batchSize(50)
                .pollInterval(Duration.ofMillis(500))
                .autoCommitOffset(false)
                .partitionId(2)
                .build();

        assertThat(config.getStreamName()).isEqualTo("events");
        assertThat(config.getTopicName()).isEqualTo("orders");
        assertThat(config.getConsumerId()).isEqualTo(42L);
        assertThat(config.getBatchSize()).isEqualTo(50);
        assertThat(config.getPollInterval()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.isAutoCommitOffset()).isFalse();
        assertThat(config.getPartitionId()).isEqualTo(2);
    }

    @Test
    void sourceConfig_requiresStreamName() {
        assertThatThrownBy(() -> IggySourceConfig.builder()
                .topicName("my-topic")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sourceConfig_requiresTopicName() {
        assertThatThrownBy(() -> IggySourceConfig.builder()
                .streamName("my-stream")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sinkConfig_buildsWithDefaults() {
        IggySinkConfig config = IggySinkConfig.builder()
                .streamName("output-stream")
                .topicName("output-topic")
                .build();

        assertThat(config.getStreamName()).isEqualTo("output-stream");
        assertThat(config.getTopicName()).isEqualTo("output-topic");
        assertThat(config.getPartitioningStrategy()).isEqualTo(IggySinkConfig.PartitioningStrategy.BALANCED);
    }

    @Test
    void sinkConfig_requiresStreamName() {
        assertThatThrownBy(() -> IggySinkConfig.builder()
                .topicName("my-topic")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sinkConfig_requiresTopicName() {
        assertThatThrownBy(() -> IggySinkConfig.builder()
                .streamName("my-stream")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sinkConfig_supportsAllPartitioningStrategies() {
        for (IggySinkConfig.PartitioningStrategy strategy : IggySinkConfig.PartitioningStrategy.values()) {
            IggySinkConfig config = IggySinkConfig.builder()
                    .streamName("stream")
                    .topicName("topic")
                    .partitioningStrategy(strategy)
                    .build();
            assertThat(config.getPartitioningStrategy()).isEqualTo(strategy);
        }
    }
}
