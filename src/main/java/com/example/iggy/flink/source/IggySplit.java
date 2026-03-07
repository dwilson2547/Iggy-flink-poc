package com.example.iggy.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single unit of work for the Iggy source: one topic partition.
 *
 * <p>For the Iggy Flink connector, a split maps to a specific stream/topic/partition
 * combination that a reader will poll from continuously.
 */
public class IggySplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String streamName;
    private final String topicName;
    private final int partitionId;
    private final long consumerId;

    public IggySplit(String streamName, String topicName, int partitionId, long consumerId) {
        this.splitId = streamName + "/" + topicName + "/" + partitionId + "/" + consumerId;
        this.streamName = streamName;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.consumerId = consumerId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getConsumerId() {
        return consumerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IggySplit)) return false;
        IggySplit that = (IggySplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    @Override
    public String toString() {
        return "IggySplit{splitId='" + splitId + "'}";
    }
}
