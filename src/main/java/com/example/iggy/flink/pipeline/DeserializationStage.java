package com.example.iggy.flink.pipeline;

import java.io.Serializable;

/**
 * Abstract stage for deserializing raw bytes from Iggy into a domain-ready input type.
 *
 * <p>Implementations should provide the logic to convert raw byte arrays received from
 * the Iggy topic into the target type {@code T}. Built-in implementations are provided
 * for common formats (Avro, Protobuf). Users can extend this class to support custom
 * formats.
 *
 * <p>Example usage:
 * <pre>{@code
 * public class MyJsonDeserializationStage extends DeserializationStage<MyEvent> {
 *     private final ObjectMapper mapper = new ObjectMapper();
 *
 *     @Override
 *     public MyEvent deserialize(byte[] bytes) throws Exception {
 *         return mapper.readValue(bytes, MyEvent.class);
 *     }
 * }
 * }</pre>
 *
 * @param <T> the type produced after deserialization
 */
public abstract class DeserializationStage<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Deserializes raw bytes into the target type.
     *
     * @param bytes the raw byte payload from an Iggy message
     * @return the deserialized object
     * @throws Exception if deserialization fails
     */
    public abstract T deserialize(byte[] bytes) throws Exception;

    /**
     * Returns a human-readable name for this deserialization stage, used in logging.
     *
     * @return stage name
     */
    public String getStageName() {
        return getClass().getSimpleName();
    }
}
