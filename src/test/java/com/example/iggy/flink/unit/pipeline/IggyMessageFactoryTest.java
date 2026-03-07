package com.example.iggy.flink.unit.pipeline;

import com.example.iggy.flink.sink.IggyMessageFactory;
import org.apache.iggy.message.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link IggyMessageFactory}.
 *
 * <p>Validates that the factory correctly creates Iggy messages from raw byte arrays,
 * which is required for publishing binary formats (Avro, Protobuf) to Iggy.
 */
class IggyMessageFactoryTest {

    @Test
    void fromBytes_createsMessageWithCorrectPayload() {
        byte[] payload = {0x01, 0x02, 0x03};

        Message message = IggyMessageFactory.fromBytes(payload);

        assertThat(message.payload()).isEqualTo(payload);
    }

    @Test
    void fromBytes_createsMessageWithEmptyUserHeaders() {
        byte[] payload = "test".getBytes();

        Message message = IggyMessageFactory.fromBytes(payload);

        assertThat(message.userHeaders()).isEmpty();
    }

    @Test
    void fromBytes_handlesEmptyPayload() {
        byte[] emptyPayload = new byte[0];

        Message message = IggyMessageFactory.fromBytes(emptyPayload);

        assertThat(message.payload()).isEmpty();
    }

    @Test
    void fromBytes_handlesBinaryData() {
        byte[] binaryData = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x00, 0x01};

        Message message = IggyMessageFactory.fromBytes(binaryData);

        assertThat(message.payload()).isEqualTo(binaryData);
    }

    @Test
    void fromBytes_setsPayloadLength() {
        byte[] payload = new byte[42];

        Message message = IggyMessageFactory.fromBytes(payload);

        assertThat(message.header().payloadLength()).isEqualTo(42L);
    }
}
