package com.example.iggy.flink.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the Iggy Flink source connector.
 *
 * <p>Specifies the stream and topic to consume from, polling strategy,
 * and consumer identity for offset tracking.
 */
public class IggySourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamName;
    private final String topicName;
    private final long consumerId;
    private final int batchSize;
    private final Duration pollInterval;
    private final boolean autoCommitOffset;
    private final int partitionId;

    private IggySourceConfig(Builder builder) {
        this.streamName = Objects.requireNonNull(builder.streamName, "streamName must not be null");
        this.topicName = Objects.requireNonNull(builder.topicName, "topicName must not be null");
        this.consumerId = builder.consumerId;
        this.batchSize = builder.batchSize;
        this.pollInterval = builder.pollInterval;
        this.autoCommitOffset = builder.autoCommitOffset;
        this.partitionId = builder.partitionId;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getTopicName() {
        return topicName;
    }

    public long getConsumerId() {
        return consumerId;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public boolean isAutoCommitOffset() {
        return autoCommitOffset;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String streamName;
        private String topicName;
        private long consumerId = 1L;
        private int batchSize = 100;
        private Duration pollInterval = Duration.ofMillis(100);
        private boolean autoCommitOffset = true;
        private int partitionId = 1;

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder consumerId(long consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder autoCommitOffset(boolean autoCommitOffset) {
            this.autoCommitOffset = autoCommitOffset;
            return this;
        }

        public Builder partitionId(int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public IggySourceConfig build() {
            Objects.requireNonNull(streamName, "streamName must not be null");
            Objects.requireNonNull(topicName, "topicName must not be null");
            return new IggySourceConfig(this);
        }
    }

    @Override
    public String toString() {
        return "IggySourceConfig{" +
                "streamName='" + streamName + '\'' +
                ", topicName='" + topicName + '\'' +
                ", consumerId=" + consumerId +
                ", batchSize=" + batchSize +
                ", pollInterval=" + pollInterval +
                ", autoCommitOffset=" + autoCommitOffset +
                ", partitionId=" + partitionId +
                '}';
    }
}
