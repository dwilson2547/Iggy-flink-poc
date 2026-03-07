package com.example.iggy.flink.unit.pipeline;

import com.example.iggy.flink.pipeline.DeserializationStage;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.pipeline.SerializationStage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for pipeline stage abstractions.
 *
 * <p>Validates that:
 * <ul>
 *   <li>DeserializationStage correctly converts bytes to domain objects</li>
 *   <li>DomainStage correctly applies business logic transformations</li>
 *   <li>SerializationStage correctly converts domain objects back to bytes</li>
 *   <li>Stages integrate correctly as a pipeline</li>
 * </ul>
 */
class PipelineStageTest {

    // --- DeserializationStage tests ---

    @Test
    void deserializationStage_convertsBytes() throws Exception {
        DeserializationStage<String> stage = new StringDeserializationStage();
        byte[] input = "hello".getBytes(StandardCharsets.UTF_8);

        String result = stage.deserialize(input);

        assertThat(result).isEqualTo("hello");
    }

    @Test
    void deserializationStage_hasDefaultStageName() {
        DeserializationStage<String> stage = new StringDeserializationStage();
        assertThat(stage.getStageName()).isEqualTo("StringDeserializationStage");
    }

    @Test
    void deserializationStage_handlesBinaryData() throws Exception {
        DeserializationStage<byte[]> stage = new IdentityDeserializationStage();
        byte[] input = new byte[]{0x01, 0x02, 0x03, (byte) 0xFF};

        byte[] result = stage.deserialize(input);

        assertThat(result).isEqualTo(input);
    }

    // --- DomainStage tests ---

    @Test
    void domainStage_transformsInput() throws Exception {
        DomainStage<String, String> stage = new UpperCaseDomainStage();

        String result = stage.process("hello world");

        assertThat(result).isEqualTo("HELLO WORLD");
    }

    @Test
    void domainStage_mapDelegatestoProcess() throws Exception {
        DomainStage<String, String> stage = new UpperCaseDomainStage();

        // map() is the Flink interface method, should delegate to process()
        String result = stage.map("test");

        assertThat(result).isEqualTo("TEST");
    }

    @Test
    void domainStage_hasDefaultStageName() {
        DomainStage<String, String> stage = new UpperCaseDomainStage();
        assertThat(stage.getStageName()).isEqualTo("UpperCaseDomainStage");
    }

    @Test
    void domainStage_canTransformTypes() throws Exception {
        DomainStage<String, Integer> stage = new StringLengthDomainStage();

        Integer result = stage.process("hello");

        assertThat(result).isEqualTo(5);
    }

    // --- SerializationStage tests ---

    @Test
    void serializationStage_convertsToBytes() throws Exception {
        SerializationStage<String> stage = new StringSerializationStage();
        byte[] result = stage.serialize("hello");

        assertThat(result).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void serializationStage_hasDefaultStageName() {
        SerializationStage<String> stage = new StringSerializationStage();
        assertThat(stage.getStageName()).isEqualTo("StringSerializationStage");
    }

    // --- Pipeline integration tests ---

    @Test
    void pipeline_stagesCompose() throws Exception {
        DeserializationStage<String> deserializer = new StringDeserializationStage();
        DomainStage<String, String> domain = new UpperCaseDomainStage();
        SerializationStage<String> serializer = new StringSerializationStage();

        byte[] rawInput = "hello world".getBytes(StandardCharsets.UTF_8);

        String deserialized = deserializer.deserialize(rawInput);
        String processed = domain.process(deserialized);
        byte[] serialized = serializer.serialize(processed);

        assertThat(new String(serialized, StandardCharsets.UTF_8)).isEqualTo("HELLO WORLD");
    }

    @Test
    void pipeline_handlesEmptyBytes() throws Exception {
        DeserializationStage<String> deserializer = new StringDeserializationStage();

        String result = deserializer.deserialize(new byte[0]);

        assertThat(result).isEmpty();
    }

    // --- Test stage implementations ---

    private static class StringDeserializationStage extends DeserializationStage<String> {
        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static class IdentityDeserializationStage extends DeserializationStage<byte[]> {
        @Override
        public byte[] deserialize(byte[] bytes) {
            return bytes;
        }
    }

    private static class UpperCaseDomainStage extends DomainStage<String, String> {
        @Override
        public String process(String input) {
            return input.toUpperCase();
        }
    }

    private static class StringLengthDomainStage extends DomainStage<String, Integer> {
        @Override
        public Integer process(String input) {
            return input.length();
        }
    }

    private static class StringSerializationStage extends SerializationStage<String> {
        @Override
        public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }
}
