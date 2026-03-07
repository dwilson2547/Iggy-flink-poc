package com.example.iggy.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer for {@link IggySplit}, required by Flink's checkpoint mechanism.
 */
public class IggySplitSerializer implements SimpleVersionedSerializer<IggySplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IggySplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getStreamName());
            out.writeUTF(split.getTopicName());
            out.writeInt(split.getPartitionId());
            out.writeLong(split.getConsumerId());
        }
        return baos.toByteArray();
    }

    @Override
    public IggySplit deserialize(int version, byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String streamName = in.readUTF();
            String topicName = in.readUTF();
            int partitionId = in.readInt();
            long consumerId = in.readLong();
            return new IggySplit(streamName, topicName, partitionId, consumerId);
        }
    }
}
