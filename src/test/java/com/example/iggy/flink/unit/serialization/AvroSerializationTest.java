package com.example.iggy.flink.unit.serialization;

import com.example.iggy.flink.serialization.avro.AvroDeserializationStage;
import com.example.iggy.flink.serialization.avro.AvroSerializationStage;
import com.example.iggy.flink.test.avro.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for Avro serialization/deserialization stages.
 *
 * <p>Validates that:
 * <ul>
 *   <li>Avro objects can be serialized to binary format</li>
 *   <li>Avro binary data can be deserialized back to the original object</li>
 *   <li>A round-trip (serialize then deserialize) preserves all field values</li>
 *   <li>Stage names are correctly reported</li>
 * </ul>
 */
class AvroSerializationTest {

    private AvroSerializationStage<TestEvent> serializer;
    private AvroDeserializationStage<TestEvent> deserializer;

    @BeforeEach
    void setUp() {
        serializer = new AvroSerializationStage<>(TestEvent.class);
        deserializer = new AvroDeserializationStage<>(TestEvent.class);
    }

    @Test
    void serialize_producesNonEmptyBytes() throws Exception {
        TestEvent event = buildTestEvent("evt-001", "Temperature Reading", 22.5, 1000L);

        byte[] bytes = serializer.serialize(event);

        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void deserialize_restoresAllFields() throws Exception {
        TestEvent original = buildTestEvent("evt-001", "Temperature Reading", 22.5, 1000L);

        byte[] bytes = serializer.serialize(original);
        TestEvent restored = deserializer.deserialize(bytes);

        assertThat(restored.getId().toString()).isEqualTo("evt-001");
        assertThat(restored.getName().toString()).isEqualTo("Temperature Reading");
        assertThat(restored.getValue()).isEqualTo(22.5);
        assertThat(restored.getTimestamp()).isEqualTo(1000L);
    }

    @Test
    void roundTrip_preservesData() throws Exception {
        TestEvent original = buildTestEvent("evt-roundtrip", "Pressure Reading", 101.325, 999999L);

        byte[] bytes = serializer.serialize(original);
        TestEvent roundTripped = deserializer.deserialize(bytes);

        assertThat(roundTripped.getId().toString()).isEqualTo(original.getId().toString());
        assertThat(roundTripped.getName().toString()).isEqualTo(original.getName().toString());
        assertThat(roundTripped.getValue()).isEqualTo(original.getValue());
        assertThat(roundTripped.getTimestamp()).isEqualTo(original.getTimestamp());
    }

    @Test
    void serializer_reportsStageName() {
        assertThat(serializer.getStageName())
                .isEqualTo("AvroSerialization[TestEvent]");
    }

    @Test
    void deserializer_reportsStageName() {
        assertThat(deserializer.getStageName())
                .isEqualTo("AvroDeserialization[TestEvent]");
    }

    @Test
    void serialize_multipleDifferentObjects_produceDifferentBytes() throws Exception {
        TestEvent event1 = buildTestEvent("evt-001", "Reading 1", 10.0, 1L);
        TestEvent event2 = buildTestEvent("evt-002", "Reading 2", 20.0, 2L);

        byte[] bytes1 = serializer.serialize(event1);
        byte[] bytes2 = serializer.serialize(event2);

        assertThat(bytes1).isNotEqualTo(bytes2);
    }

    @Test
    void deserialize_handlesSpecialValues() throws Exception {
        TestEvent event = buildTestEvent("evt-special", "Special Reading", Double.MAX_VALUE, Long.MAX_VALUE);

        byte[] bytes = serializer.serialize(event);
        TestEvent restored = deserializer.deserialize(bytes);

        assertThat(restored.getValue()).isEqualTo(Double.MAX_VALUE);
        assertThat(restored.getTimestamp()).isEqualTo(Long.MAX_VALUE);
    }

    private static TestEvent buildTestEvent(String id, String name, double value, long timestamp) {
        return TestEvent.newBuilder()
                .setId(id)
                .setName(name)
                .setValue(value)
                .setTimestamp(timestamp)
                .build();
    }
}
