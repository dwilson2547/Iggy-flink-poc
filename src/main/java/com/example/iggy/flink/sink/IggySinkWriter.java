package com.example.iggy.flink.sink;

import com.example.iggy.flink.client.IggyTcpClientFactory;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Partitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Flink 2.0 SinkWriter that publishes raw byte messages to an Apache Iggy topic via TCP.
 *
 * <p>Each writer maintains its own TCP connection to the Iggy server and publishes
 * messages using only the TCP protocol (no HTTP/REST).
 *
 * <p>This writer is created by {@link IggySink#createWriter(InitContext)}.
 */
public class IggySinkWriter implements SinkWriter<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySinkWriter.class);

    private final IggyConnectionConfig connectionConfig;
    private final IggySinkConfig sinkConfig;
    private final IggyTcpClient iggyClient;

    public IggySinkWriter(IggyConnectionConfig connectionConfig, IggySinkConfig sinkConfig) {
        this.connectionConfig = connectionConfig;
        this.sinkConfig = sinkConfig;
        this.iggyClient = IggyTcpClientFactory.createAndLogin(connectionConfig);
        LOG.info("IggySinkWriter opened: stream={}, topic={}",
                sinkConfig.getStreamName(), sinkConfig.getTopicName());
    }

    @Override
    public void write(byte[] element, Context context) throws IOException {
        StreamId streamId = StreamId.of(sinkConfig.getStreamName());
        TopicId topicId = TopicId.of(sinkConfig.getTopicName());
        Partitioning partitioning = resolvePartitioning(sinkConfig.getPartitioningStrategy());

        try {
            org.apache.iggy.message.Message message = IggyMessageFactory.fromBytes(element);
            iggyClient.messages().sendMessages(streamId, topicId, partitioning, List.of(message));
            LOG.debug("Published message to Iggy: stream={}, topic={}",
                    sinkConfig.getStreamName(), sinkConfig.getTopicName());
        } catch (Exception e) {
            throw new IOException("Failed to send message to Iggy: stream=" +
                    sinkConfig.getStreamName() + ", topic=" + sinkConfig.getTopicName(), e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // Iggy TCP sends are synchronous; no explicit flush needed
    }

    @Override
    public void close() {
        LOG.info("IggySinkWriter closed: stream={}, topic={}",
                sinkConfig.getStreamName(), sinkConfig.getTopicName());
        // IggyTcpClient 0.6.0 does not expose a disconnect method
    }

    private Partitioning resolvePartitioning(IggySinkConfig.PartitioningStrategy strategy) {
        return switch (strategy) {
            case BALANCED -> Partitioning.balanced();
            case PARTITION_ID -> Partitioning.partitionId(1L);
            case MESSAGE_KEY -> Partitioning.balanced();
        };
    }
}
