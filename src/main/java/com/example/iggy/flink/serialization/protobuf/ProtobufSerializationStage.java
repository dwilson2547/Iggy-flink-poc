package com.example.iggy.flink.serialization.protobuf;

import com.example.iggy.flink.pipeline.SerializationStage;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protobuf-based serialization stage that converts Protobuf {@link Message} objects into raw bytes.
 *
 * <p>This stage uses Protobuf's binary wire format for efficient, compact serialization.
 *
 * <p>Usage:
 * <pre>{@code
 * SerializationStage<MyProtoMessage> stage = new ProtobufSerializationStage<>(MyProtoMessage.class);
 * }</pre>
 *
 * @param <T> the Protobuf Message type to serialize
 */
public class ProtobufSerializationStage<T extends Message> extends SerializationStage<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufSerializationStage.class);
    private static final long serialVersionUID = 1L;

    private final Class<T> sourceClass;

    /**
     * Creates a Protobuf serialization stage for the given Message type.
     *
     * @param sourceClass the Protobuf message class to serialize
     */
    public ProtobufSerializationStage(Class<T> sourceClass) {
        this.sourceClass = sourceClass;
    }

    @Override
    public byte[] serialize(T value) throws Exception {
        return value.toByteArray();
    }

    @Override
    public String getStageName() {
        return "ProtobufSerialization[" + sourceClass.getSimpleName() + "]";
    }
}
