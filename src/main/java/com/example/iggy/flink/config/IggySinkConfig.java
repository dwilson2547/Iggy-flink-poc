package com.example.iggy.flink.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration for the Iggy Flink sink connector.
 *
 * <p>Specifies the stream and topic to publish to, along with partitioning strategy.
 */
public class IggySinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamName;
    private final String topicName;
    private final PartitioningStrategy partitioningStrategy;

    private IggySinkConfig(Builder builder) {
        this.streamName = Objects.requireNonNull(builder.streamName, "streamName must not be null");
        this.topicName = Objects.requireNonNull(builder.topicName, "topicName must not be null");
        this.partitioningStrategy = builder.partitioningStrategy;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getTopicName() {
        return topicName;
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    /**
     * Strategy for partitioning messages when publishing to Iggy.
     */
    public enum PartitioningStrategy {
        /** Distribute messages evenly across partitions. */
        BALANCED,
        /** Route messages based on a message key. */
        MESSAGE_KEY,
        /** Send all messages to a specific partition. */
        PARTITION_ID
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String streamName;
        private String topicName;
        private PartitioningStrategy partitioningStrategy = PartitioningStrategy.BALANCED;

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder partitioningStrategy(PartitioningStrategy partitioningStrategy) {
            this.partitioningStrategy = partitioningStrategy;
            return this;
        }

        public IggySinkConfig build() {
            Objects.requireNonNull(streamName, "streamName must not be null");
            Objects.requireNonNull(topicName, "topicName must not be null");
            return new IggySinkConfig(this);
        }
    }

    @Override
    public String toString() {
        return "IggySinkConfig{" +
                "streamName='" + streamName + '\'' +
                ", topicName='" + topicName + '\'' +
                ", partitioningStrategy=" + partitioningStrategy +
                '}';
    }
}
