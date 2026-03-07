package com.example.iggy.flink.sink;

import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;

import java.math.BigInteger;
import java.util.Optional;

/**
 * Utility class for creating Iggy {@link Message} objects from raw byte arrays.
 *
 * <p>The built-in {@link Message#of(String)} factory converts a String to bytes using the
 * platform's default charset, which is not suitable for binary formats (Avro, Protobuf).
 * This utility provides a safe way to create messages directly from byte arrays.
 */
public final class IggyMessageFactory {

    private IggyMessageFactory() {
        // Utility class
    }

    /**
     * Creates an Iggy {@link Message} from a raw byte array.
     *
     * <p>Uses server-generated message IDs and zeroed header fields, matching the
     * behavior of {@link Message#of(String)} but working with raw binary payloads.
     *
     * @param payload the raw byte payload
     * @return a new Message ready to be sent to Iggy
     */
    public static Message fromBytes(byte[] payload) {
        MessageHeader header = new MessageHeader(
                BigInteger.ZERO,              // checksum (computed by server/SDK)
                MessageId.serverGenerated(),  // server generates the ID
                BigInteger.ZERO,              // offset (assigned by server)
                BigInteger.ZERO,              // timestamp (assigned by server)
                BigInteger.ZERO,              // originTimestamp
                0L,                           // userHeadersLength
                (long) payload.length         // payloadLength
        );
        return new Message(header, payload, Optional.empty());
    }
}
