package com.example.iggy.flink;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import com.example.iggy.flink.pipeline.DeserializationStage;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.pipeline.SerializationStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Example main entry point for the Iggy Flink pipeline.
 *
 * <p>This demonstrates a simple pass-through pipeline using byte-array identity operations.
 * Replace the pipeline stages with your own implementations for real use cases.
 *
 * <p>Configuration is read from environment variables or falls back to defaults:
 * <ul>
 *   <li>{@code IGGY_HOST} - Iggy server host (default: localhost)</li>
 *   <li>{@code IGGY_PORT} - Iggy server TCP port (default: 8090)</li>
 *   <li>{@code IGGY_USERNAME} - Iggy username (default: iggy)</li>
 *   <li>{@code IGGY_PASSWORD} - Iggy password (default: iggy)</li>
 *   <li>{@code SOURCE_STREAM} - Source stream name (default: events)</li>
 *   <li>{@code SOURCE_TOPIC} - Source topic name (default: input)</li>
 *   <li>{@code SINK_STREAM} - Sink stream name (default: events)</li>
 *   <li>{@code SINK_TOPIC} - Sink topic name (default: output)</li>
 * </ul>
 */
public class IggyFlinkPipelineMain {

    private static final Logger LOG = LoggerFactory.getLogger(IggyFlinkPipelineMain.class);

    public static void main(String[] args) throws Exception {
        String iggyHost = getEnv("IGGY_HOST", "localhost");
        int iggyPort = Integer.parseInt(getEnv("IGGY_PORT", "8090"));
        String iggyUsername = getEnv("IGGY_USERNAME", "iggy");
        String iggyPassword = getEnv("IGGY_PASSWORD", "iggy");
        String sourceStream = getEnv("SOURCE_STREAM", "events");
        String sourceTopic = getEnv("SOURCE_TOPIC", "input");
        String sinkStream = getEnv("SINK_STREAM", "events");
        String sinkTopic = getEnv("SINK_TOPIC", "output");

        LOG.info("Starting Iggy Flink Pipeline: {}:{} | {}/{} -> {}/{}",
                iggyHost, iggyPort, sourceStream, sourceTopic, sinkStream, sinkTopic);

        IggyConnectionConfig connectionConfig = IggyConnectionConfig.builder()
                .host(iggyHost)
                .port(iggyPort)
                .username(iggyUsername)
                .password(iggyPassword)
                .build();

        IggySourceConfig sourceConfig = IggySourceConfig.builder()
                .streamName(sourceStream)
                .topicName(sourceTopic)
                .build();

        IggySinkConfig sinkConfig = IggySinkConfig.builder()
                .streamName(sinkStream)
                .topicName(sinkTopic)
                .build();

        // Example: pass-through pipeline with identity stages
        // Replace these with your actual Avro/Protobuf stages for production use
        IggyFlinkPipeline.<String, String>builder()
                .connectionConfig(connectionConfig)
                .sourceConfig(sourceConfig)
                .sinkConfig(sinkConfig)
                .deserializationStage(new PassThroughDeserializationStage())
                .domainStage(new PassThroughDomainStage())
                .serializationStage(new PassThroughSerializationStage())
                .build()
                .execute("Iggy Flink Pipeline");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null && !value.isBlank() ? value : defaultValue;
    }

    /** Identity deserialization stage: converts bytes to UTF-8 String. */
    private static class PassThroughDeserializationStage extends DeserializationStage<String> {
        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /** Identity domain stage: passes String through unchanged. */
    private static class PassThroughDomainStage extends DomainStage<String, String> {
        @Override
        public String process(String input) {
            return input;
        }
    }

    /** Identity serialization stage: converts UTF-8 String back to bytes. */
    private static class PassThroughSerializationStage extends SerializationStage<String> {
        @Override
        public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }
}
