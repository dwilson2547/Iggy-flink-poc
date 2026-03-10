package com.example.iggy.flink.unit.serialization;

import com.example.iggy.flink.serialization.protobuf.ProtobufDeserializationStage;
import com.example.iggy.flink.serialization.protobuf.ProtobufSerializationStage;
import com.example.iggy.flink.test.proto.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for Protobuf serialization/deserialization stages.
 *
 * <p>Validates that:
 * <ul>
 *   <li>Protobuf objects can be serialized to binary format</li>
 *   <li>Protobuf binary data can be deserialized back to the original object</li>
 *   <li>A round-trip (serialize then deserialize) preserves all field values</li>
 *   <li>Stage names are correctly reported</li>
 * </ul>
 */
class ProtobufSerializationTest {

    private ProtobufSerializationStage<TestEvent> serializer;
    private ProtobufDeserializationStage<TestEvent> deserializer;

    @BeforeEach
    void setUp() {
        serializer = new ProtobufSerializationStage<>(TestEvent.class);
        deserializer = new ProtobufDeserializationStage<>(TestEvent.class, TestEvent.parser());
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

        assertThat(restored.getId()).isEqualTo("evt-001");
        assertThat(restored.getName()).isEqualTo("Temperature Reading");
        assertThat(restored.getValue()).isEqualTo(22.5);
        assertThat(restored.getTimestamp()).isEqualTo(1000L);
    }

    @Test
    void roundTrip_preservesData() throws Exception {
        TestEvent original = buildTestEvent("evt-roundtrip", "Pressure Reading", 101.325, 999999L);

        byte[] bytes = serializer.serialize(original);
        TestEvent roundTripped = deserializer.deserialize(bytes);

        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void serializer_reportsStageName() {
        assertThat(serializer.getStageName())
                .isEqualTo("ProtobufSerialization[TestEvent]");
    }

    @Test
    void deserializer_reportsStageName() {
        assertThat(deserializer.getStageName())
                .isEqualTo("ProtobufDeserialization[TestEvent]");
    }

    @Test
    void serialize_producesCompactBinaryFormat() throws Exception {
        TestEvent event = buildTestEvent("id", "name", 1.0, 1L);
        byte[] bytes = serializer.serialize(event);

        // Protobuf binary should be much smaller than JSON equivalent
        String jsonEquivalent = "{\"id\":\"id\",\"name\":\"name\",\"value\":1.0,\"timestamp\":1}";
        assertThat(bytes.length).isLessThan(jsonEquivalent.length());
    }

    @Test
    void emptyMessage_serializesToEmptyBytes() throws Exception {
        TestEvent emptyEvent = TestEvent.newBuilder().build();

        byte[] bytes = serializer.serialize(emptyEvent);
        TestEvent restored = deserializer.deserialize(bytes);

        assertThat(restored).isEqualTo(emptyEvent);
    }

    @Test
    void deserialize_handlesSpecialValues() throws Exception {
        TestEvent event = buildTestEvent("evt-special", "Special", Double.MAX_VALUE, Long.MAX_VALUE);

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
