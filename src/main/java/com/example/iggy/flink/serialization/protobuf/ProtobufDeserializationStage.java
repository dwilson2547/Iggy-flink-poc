package com.example.iggy.flink.serialization.protobuf;

import com.example.iggy.flink.pipeline.DeserializationStage;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protobuf-based deserialization stage that converts raw bytes into Protobuf {@link Message} objects.
 *
 * <p>This stage uses Protobuf's binary wire format for deserialization.
 *
 * <p>Usage:
 * <pre>{@code
 * DeserializationStage<MyProtoMessage> stage =
 *     new ProtobufDeserializationStage<>(MyProtoMessage.parser());
 * }</pre>
 *
 * @param <T> the Protobuf Message type to deserialize into
 */
public class ProtobufDeserializationStage<T extends Message> extends DeserializationStage<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufDeserializationStage.class);
    private static final long serialVersionUID = 1L;

    private final Class<T> targetClass;
    private final transient Parser<T> parser;

    /**
     * Creates a Protobuf deserialization stage using the given parser.
     *
     * <p>The parser is typically obtained via {@code MyProtoMessage.parser()}.
     *
     * @param targetClass the Protobuf message class (used for stage naming and serialization)
     * @param parser      the Protobuf parser for the target type
     */
    public ProtobufDeserializationStage(Class<T> targetClass, Parser<T> parser) {
        this.targetClass = targetClass;
        this.parser = parser;
    }

    @Override
    public T deserialize(byte[] bytes) throws Exception {
        return parser.parseFrom(bytes);
    }

    @Override
    public String getStageName() {
        return "ProtobufDeserialization[" + targetClass.getSimpleName() + "]";
    }
}
