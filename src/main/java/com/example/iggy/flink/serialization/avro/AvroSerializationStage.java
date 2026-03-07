package com.example.iggy.flink.serialization.avro;

import com.example.iggy.flink.pipeline.SerializationStage;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * Avro-based serialization stage that converts Avro {@link SpecificRecord} objects into raw bytes.
 *
 * <p>This stage uses Avro's binary encoding format for efficient, compact serialization.
 *
 * <p>Usage:
 * <pre>{@code
 * SerializationStage<MyAvroRecord> stage = new AvroSerializationStage<>(MyAvroRecord.class);
 * }</pre>
 *
 * @param <T> the Avro SpecificRecord type to serialize
 */
public class AvroSerializationStage<T extends SpecificRecord> extends SerializationStage<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializationStage.class);
    private static final long serialVersionUID = 1L;

    private final Class<T> sourceClass;

    private transient SpecificDatumWriter<T> writer;

    /**
     * Creates an Avro serialization stage for the given SpecificRecord type.
     *
     * @param sourceClass the Avro SpecificRecord class to serialize
     */
    public AvroSerializationStage(Class<T> sourceClass) {
        this.sourceClass = sourceClass;
    }

    @Override
    public byte[] serialize(T value) throws Exception {
        if (writer == null) {
            writer = new SpecificDatumWriter<>(value.getSchema());
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(value, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }

    @Override
    public String getStageName() {
        return "AvroSerialization[" + sourceClass.getSimpleName() + "]";
    }
}
