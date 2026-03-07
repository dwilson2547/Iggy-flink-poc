package com.example.iggy.flink.pipeline;

import java.io.Serializable;

/**
 * Abstract stage for serializing domain output objects back into raw bytes for Iggy.
 *
 * <p>Implementations should provide the logic to convert a processed output type {@code T}
 * into a byte array to be published to the Iggy sink topic. Built-in implementations are
 * provided for common formats (Avro, Protobuf). Users can extend this class to support
 * custom formats.
 *
 * <p>Example usage:
 * <pre>{@code
 * public class MyJsonSerializationStage extends SerializationStage<EnrichedOrder> {
 *     private final ObjectMapper mapper = new ObjectMapper();
 *
 *     @Override
 *     public byte[] serialize(EnrichedOrder value) throws Exception {
 *         return mapper.writeValueAsBytes(value);
 *     }
 * }
 * }</pre>
 *
 * @param <T> the type to be serialized into bytes
 */
public abstract class SerializationStage<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Serializes the given value into a byte array for publishing to Iggy.
     *
     * @param value the domain object to serialize
     * @return the serialized byte representation
     * @throws Exception if serialization fails
     */
    public abstract byte[] serialize(T value) throws Exception;

    /**
     * Returns a human-readable name for this serialization stage, used in logging.
     *
     * @return stage name
     */
    public String getStageName() {
        return getClass().getSimpleName();
    }
}
