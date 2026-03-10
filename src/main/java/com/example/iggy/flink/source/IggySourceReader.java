package com.example.iggy.flink.source;

import com.example.iggy.flink.client.IggyTcpClientFactory;
import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Flink 2.0 SourceReader that polls messages from an Apache Iggy topic via TCP.
 *
 * <p>Each reader is assigned an {@link IggySplit} and continuously polls the
 * corresponding Iggy stream/topic/partition for new messages, emitting raw
 * {@code byte[]} payloads downstream.
 */
public class IggySourceReader implements SourceReader<byte[], IggySplit> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySourceReader.class);

    private final IggyConnectionConfig connectionConfig;
    private final IggySourceConfig sourceConfig;
    private final SourceReaderContext readerContext;

    private final Queue<IggySplit> assignedSplits = new ArrayDeque<>();
    private transient IggyTcpClient iggyClient;
    private volatile boolean noMoreSplits = false;

    // Buffer holding messages fetched in the last poll batch
    private final Queue<byte[]> messageBuffer = new ArrayDeque<>();

    // CompletableFuture used to signal Flink that data may be available
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    public IggySourceReader(
            IggyConnectionConfig connectionConfig,
            IggySourceConfig sourceConfig,
            SourceReaderContext readerContext) {
        this.connectionConfig = connectionConfig;
        this.sourceConfig = sourceConfig;
        this.readerContext = readerContext;
    }

    @Override
    public void start() {
        iggyClient = IggyTcpClientFactory.createAndLogin(connectionConfig);
        readerContext.sendSplitRequest();
        LOG.info("IggySourceReader started, sent split request");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<byte[]> output) throws Exception {
        // Drain any buffered messages first
        if (!messageBuffer.isEmpty()) {
            output.collect(messageBuffer.poll());
            return messageBuffer.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        IggySplit split = assignedSplits.peek();
        if (split == null) {
            if (noMoreSplits) {
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        try {
            PolledMessages polled = iggyClient.messages().pollMessages(
                    StreamId.of(split.getStreamName()),
                    TopicId.of(split.getTopicName()),
                    Optional.of((long) split.getPartitionId()),
                    Consumer.of(split.getConsumerId()),
                    PollingStrategy.next(),
                    (long) sourceConfig.getBatchSize(),
                    sourceConfig.isAutoCommitOffset()
            );

            if (polled != null && polled.messages() != null && !polled.messages().isEmpty()) {
                for (Message message : polled.messages()) {
                    messageBuffer.add(message.payload());
                }
                LOG.debug("Fetched {} messages from Iggy split {}", polled.messages().size(), split.splitId());

                if (!messageBuffer.isEmpty()) {
                    output.collect(messageBuffer.poll());
                    if (!messageBuffer.isEmpty()) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                }
            } else {
                // No messages; sleep briefly then signal nothing available
                Thread.sleep(sourceConfig.getPollInterval().toMillis());
                // Reset availability future so Flink will wait until we signal again
                if (availability.isDone()) {
                    availability = new CompletableFuture<>();
                }
                // Schedule wake-up after poll interval
                CompletableFuture.delayedExecutor(
                        sourceConfig.getPollInterval().toMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS
                ).execute(() -> availability.complete(null));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Error polling Iggy: {}", e.getMessage(), e);
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<IggySplit> snapshotState(long checkpointId) {
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (!messageBuffer.isEmpty() || !assignedSplits.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (availability.isDone()) {
            availability = new CompletableFuture<>();
        }
        return availability;
    }

    @Override
    public void addSplits(List<IggySplit> splits) {
        LOG.info("IggySourceReader received {} split(s)", splits.size());
        assignedSplits.addAll(splits);
        // Signal that we now have work to do
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("IggySourceReader notified: no more splits");
        noMoreSplits = true;
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {
        LOG.info("IggySourceReader closed");
        // IggyTcpClient does not expose a close/disconnect method in 0.6.0
    }
}
