package com.example.iggy.flink.sink;

import com.example.iggy.flink.sink.IggyMessageFactory;
import com.example.iggy.flink.client.IggyTcpClientFactory;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Partitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import java.util.List;

/**
 * Flink sink function that publishes raw byte messages to an Apache Iggy topic via TCP.
 *
 * <p>This sink function receives serialized byte arrays from the Flink pipeline and
 * publishes them to the configured Iggy stream/topic. It uses only the TCP protocol
 * for communication with the Iggy server.
 *
 * <p>The sink manages its own TCP connection lifecycle, opening it in {@code open()}
 * and closing it in {@code close()}.
 *
 * <p>Raw bytes are received from upstream; serialization is handled by a
 * {@link com.example.iggy.flink.pipeline.SerializationStage}.
 */
@SuppressWarnings("deprecation")
public class IggySink extends RichSinkFunction<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySink.class);

    private final IggyConnectionConfig connectionConfig;
    private final IggySinkConfig sinkConfig;

    private transient IggyTcpClient iggyClient;

    /**
     * Creates a new Iggy sink.
     *
     * @param connectionConfig TCP connection configuration
     * @param sinkConfig       sink stream/topic configuration
     */
    public IggySink(IggyConnectionConfig connectionConfig, IggySinkConfig sinkConfig) {
        this.connectionConfig = connectionConfig;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        iggyClient = IggyTcpClientFactory.createAndLogin(connectionConfig);
        LOG.info("Iggy sink opened: stream={}, topic={}",
                sinkConfig.getStreamName(), sinkConfig.getTopicName());
    }

    @Override
    public void invoke(byte[] value, Context context) throws Exception {
        StreamId streamId = StreamId.of(sinkConfig.getStreamName());
        TopicId topicId = TopicId.of(sinkConfig.getTopicName());
        Partitioning partitioning = resolvePartitioning(sinkConfig.getPartitioningStrategy());

        org.apache.iggy.message.Message message = IggyMessageFactory.fromBytes(value);
        iggyClient.messages().sendMessages(streamId, topicId, partitioning, List.of(message));

        LOG.debug("Published message to Iggy: stream={}, topic={}",
                sinkConfig.getStreamName(), sinkConfig.getTopicName());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Iggy sink closed: stream={}, topic={}",
                sinkConfig.getStreamName(), sinkConfig.getTopicName());
    }

    private Partitioning resolvePartitioning(IggySinkConfig.PartitioningStrategy strategy) {
        return switch (strategy) {
            case BALANCED -> Partitioning.balanced();
            case PARTITION_ID -> Partitioning.partitionId(1L);
            case MESSAGE_KEY -> Partitioning.balanced();
        };
    }
}
