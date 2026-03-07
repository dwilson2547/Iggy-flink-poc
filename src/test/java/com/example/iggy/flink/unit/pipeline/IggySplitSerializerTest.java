package com.example.iggy.flink.unit.pipeline;

import com.example.iggy.flink.source.IggySplit;
import com.example.iggy.flink.source.IggySplitSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link IggySplit} and {@link IggySplitSerializer}.
 *
 * <p>Validates that splits can be correctly serialized and deserialized for
 * Flink's checkpoint mechanism.
 */
class IggySplitSerializerTest {

    private final IggySplitSerializer serializer = new IggySplitSerializer();

    @Test
    void split_hasCorrectSplitId() {
        IggySplit split = new IggySplit("my-stream", "my-topic", 1, 42L);
        assertThat(split.splitId()).isEqualTo("my-stream/my-topic/1/42");
    }

    @Test
    void split_storesAllFields() {
        IggySplit split = new IggySplit("events", "orders", 3, 99L);
        assertThat(split.getStreamName()).isEqualTo("events");
        assertThat(split.getTopicName()).isEqualTo("orders");
        assertThat(split.getPartitionId()).isEqualTo(3);
        assertThat(split.getConsumerId()).isEqualTo(99L);
    }

    @Test
    void serializer_roundTrip_preservesAllFields() throws IOException {
        IggySplit original = new IggySplit("stream-1", "topic-2", 2, 7L);

        byte[] bytes = serializer.serialize(original);
        IggySplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.getStreamName()).isEqualTo("stream-1");
        assertThat(restored.getTopicName()).isEqualTo("topic-2");
        assertThat(restored.getPartitionId()).isEqualTo(2);
        assertThat(restored.getConsumerId()).isEqualTo(7L);
        assertThat(restored.splitId()).isEqualTo(original.splitId());
    }

    @Test
    void serializer_hasVersion1() {
        assertThat(serializer.getVersion()).isEqualTo(1);
    }

    @Test
    void split_equality_basedOnSplitId() {
        IggySplit s1 = new IggySplit("s", "t", 1, 1L);
        IggySplit s2 = new IggySplit("s", "t", 1, 1L);
        IggySplit s3 = new IggySplit("s", "t", 2, 1L);

        assertThat(s1).isEqualTo(s2);
        assertThat(s1).isNotEqualTo(s3);
    }
}
