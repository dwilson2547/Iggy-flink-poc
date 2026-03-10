package com.example.iggy.flink.source;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Flink 2.0 Source implementation that reads raw byte messages from an Apache Iggy topic via TCP.
 *
 * <p>This source is {@link Boundedness#CONTINUOUS_UNBOUNDED} — it continuously polls Iggy
 * and emits raw {@code byte[]} payloads, which are then passed to the pipeline's
 * {@link com.example.iggy.flink.pipeline.DeserializationStage}.
 *
 * <p>The source creates one {@link IggySplit} per configured partition and assigns them
 * to available reader subtasks.
 *
 * <p>Only TCP communication is used (no HTTP/REST).
 */
public class IggySource implements Source<byte[], IggySplit, Collection<IggySplit>> {

    private static final long serialVersionUID = 1L;

    private final IggyConnectionConfig connectionConfig;
    private final IggySourceConfig sourceConfig;

    /**
     * Creates a new Iggy source.
     *
     * @param connectionConfig TCP connection configuration for the Iggy server
     * @param sourceConfig     configuration for which stream/topic/partition to read from
     */
    public IggySource(IggyConnectionConfig connectionConfig, IggySourceConfig sourceConfig) {
        this.connectionConfig = connectionConfig;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<byte[], IggySplit> createReader(SourceReaderContext readerContext) {
        return new IggySourceReader(connectionConfig, sourceConfig, readerContext);
    }

    @Override
    public SplitEnumerator<IggySplit, Collection<IggySplit>> createEnumerator(
            SplitEnumeratorContext<IggySplit> enumContext) {
        List<IggySplit> initialSplits = List.of(new IggySplit(
                sourceConfig.getStreamName(),
                sourceConfig.getTopicName(),
                sourceConfig.getPartitionId(),
                sourceConfig.getConsumerId()
        ));
        return new IggySplitEnumerator(enumContext, initialSplits);
    }

    @Override
    public SplitEnumerator<IggySplit, Collection<IggySplit>> restoreEnumerator(
            SplitEnumeratorContext<IggySplit> enumContext,
            Collection<IggySplit> checkpoint) {
        return new IggySplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<IggySplit> getSplitSerializer() {
        return new IggySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<IggySplit>> getEnumeratorCheckpointSerializer() {
        return new IggyEnumeratorStateSerializer();
    }

    /**
     * Serializer for the enumerator checkpoint state (a collection of unassigned splits).
     */
    private static class IggyEnumeratorStateSerializer
            implements SimpleVersionedSerializer<Collection<IggySplit>> {

        private static final int VERSION = 1;
        private final IggySplitSerializer splitSerializer = new IggySplitSerializer();

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(Collection<IggySplit> splits) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(baos)) {
                out.writeInt(splits.size());
                for (IggySplit split : splits) {
                    byte[] splitBytes = splitSerializer.serialize(split);
                    out.writeInt(splitBytes.length);
                    out.write(splitBytes);
                }
            }
            return baos.toByteArray();
        }

        @Override
        public Collection<IggySplit> deserialize(int version, byte[] serialized) throws IOException {
            List<IggySplit> splits = new ArrayList<>();
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    int len = in.readInt();
                    byte[] splitBytes = new byte[len];
                    in.readFully(splitBytes);
                    splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
                }
            }
            return splits;
        }
    }
}
