package com.example.iggy.flink.source;

import com.example.iggy.flink.client.IggyTcpClientFactory;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Flink source function that reads raw byte messages from an Apache Iggy topic via TCP.
 *
 * <p>This source function continuously polls an Iggy topic and emits raw {@code byte[]} payloads
 * into the Flink pipeline. It uses only the TCP protocol for communication with the Iggy server.
 *
 * <p>The source automatically handles reconnection on failure and respects Flink's
 * cancellation lifecycle.
 *
 * <p>Raw bytes are emitted downstream; deserialization is handled by a
 * {@link com.example.iggy.flink.pipeline.DeserializationStage}.
 */
@SuppressWarnings("deprecation")
public class IggySource extends RichSourceFunction<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySource.class);

    private final IggyConnectionConfig connectionConfig;
    private final IggySourceConfig sourceConfig;

    private transient volatile boolean running = true;
    private transient IggyTcpClient iggyClient;

    /**
     * Creates a new Iggy source.
     *
     * @param connectionConfig TCP connection configuration
     * @param sourceConfig     source stream/topic configuration
     */
    public IggySource(IggyConnectionConfig connectionConfig, IggySourceConfig sourceConfig) {
        this.connectionConfig = connectionConfig;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        iggyClient = IggyTcpClientFactory.createAndLogin(connectionConfig);

        StreamId streamId = StreamId.of(sourceConfig.getStreamName());
        TopicId topicId = TopicId.of(sourceConfig.getTopicName());
        Consumer consumer = Consumer.of(sourceConfig.getConsumerId());
        Optional<Long> partitionId = Optional.of((long) sourceConfig.getPartitionId());

        LOG.info("Starting Iggy source: stream={}, topic={}, consumerId={}",
                sourceConfig.getStreamName(), sourceConfig.getTopicName(), sourceConfig.getConsumerId());

        while (running) {
            try {
                PolledMessages polled = iggyClient.messages().pollMessages(
                        streamId,
                        topicId,
                        partitionId,
                        consumer,
                        PollingStrategy.next(),
                        (long) sourceConfig.getBatchSize(),
                        sourceConfig.isAutoCommitOffset()
                );

                if (polled != null && polled.messages() != null && !polled.messages().isEmpty()) {
                    synchronized (ctx.getCheckpointLock()) {
                        for (Message message : polled.messages()) {
                            ctx.collect(message.payload());
                        }
                    }
                    LOG.debug("Polled {} messages from Iggy", polled.messages().size());
                } else {
                    // No messages available; wait before polling again
                    Thread.sleep(sourceConfig.getPollInterval().toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) {
                    LOG.error("Error polling messages from Iggy: stream={}, topic={}",
                            sourceConfig.getStreamName(), sourceConfig.getTopicName(), e);
                    Thread.sleep(sourceConfig.getPollInterval().toMillis());
                }
            }
        }

        LOG.info("Iggy source stopped: stream={}, topic={}",
                sourceConfig.getStreamName(), sourceConfig.getTopicName());
    }

    @Override
    public void cancel() {
        running = false;
        LOG.info("Iggy source cancelled");
    }
}
