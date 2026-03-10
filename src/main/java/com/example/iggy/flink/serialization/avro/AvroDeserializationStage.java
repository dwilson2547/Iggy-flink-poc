package com.example.iggy.flink.serialization.avro;

import com.example.iggy.flink.pipeline.DeserializationStage;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

/**
 * Avro-based deserialization stage that converts raw bytes into Avro {@link SpecificRecord} objects.
 *
 * <p>This stage uses Avro's binary encoding format. The writer and reader schemas can be
 * configured separately to support schema evolution.
 *
 * <p>Usage:
 * <pre>{@code
 * DeserializationStage<MyAvroRecord> stage = new AvroDeserializationStage<>(MyAvroRecord.class);
 * }</pre>
 *
 * @param <T> the Avro SpecificRecord type to deserialize into
 */
public class AvroDeserializationStage<T extends SpecificRecord> extends DeserializationStage<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationStage.class);
    private static final long serialVersionUID = 1L;

    private final Class<T> targetClass;
    private final Schema writerSchema;

    private transient SpecificDatumReader<T> reader;

    /**
     * Creates an Avro deserialization stage using the target class schema for both
     * reading and writing (no schema evolution).
     *
     * @param targetClass the Avro SpecificRecord class to deserialize into
     */
    public AvroDeserializationStage(Class<T> targetClass) {
        this.targetClass = targetClass;
        this.writerSchema = null;
    }

    /**
     * Creates an Avro deserialization stage with explicit writer schema for schema evolution.
     *
     * @param targetClass  the Avro SpecificRecord class to deserialize into
     * @param writerSchema the schema used when the data was written (for schema evolution)
     */
    public AvroDeserializationStage(Class<T> targetClass, Schema writerSchema) {
        this.targetClass = targetClass;
        this.writerSchema = writerSchema;
    }

    @Override
    public T deserialize(byte[] bytes) throws Exception {
        if (reader == null) {
            reader = createReader();
        }
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
                new ByteArrayInputStream(bytes), null);
        return reader.read(null, decoder);
    }

    private SpecificDatumReader<T> createReader() {
        try {
            T instance = targetClass.getDeclaredConstructor().newInstance();
            Schema readerSchema = instance.getSchema();
            if (writerSchema != null) {
                return new SpecificDatumReader<>(writerSchema, readerSchema);
            }
            return new SpecificDatumReader<>(readerSchema);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Avro reader for " + targetClass.getName(), e);
        }
    }

    @Override
    public String getStageName() {
        return "AvroDeserialization[" + targetClass.getSimpleName() + "]";
    }
}
