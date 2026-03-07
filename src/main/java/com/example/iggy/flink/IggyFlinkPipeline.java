package com.example.iggy.flink;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import com.example.iggy.flink.config.IggySourceConfig;
import com.example.iggy.flink.pipeline.DeserializationStage;
import com.example.iggy.flink.pipeline.DomainStage;
import com.example.iggy.flink.pipeline.SerializationStage;
import com.example.iggy.flink.sink.IggySink;
import com.example.iggy.flink.source.IggySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main orchestrator for the Iggy Flink pipeline.
 *
 * <p>This class wires together all pipeline stages into a complete Flink job:
 * <ol>
 *   <li>Source: reads raw byte messages from an Iggy topic via TCP</li>
 *   <li>Deserialization: converts raw bytes into the domain input type</li>
 *   <li>Domain processing: applies business logic to transform the input</li>
 *   <li>Serialization: converts the output into raw bytes</li>
 *   <li>Sink: publishes raw bytes to another Iggy topic via TCP</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>{@code
 * IggyConnectionConfig connectionConfig = IggyConnectionConfig.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .username("iggy")
 *     .password("iggy")
 *     .build();
 *
 * IggyFlinkPipeline.<MyEvent, ProcessedEvent>builder()
 *     .connectionConfig(connectionConfig)
 *     .sourceConfig(IggySourceConfig.builder().streamName("events").topicName("raw").build())
 *     .sinkConfig(IggySinkConfig.builder().streamName("events").topicName("processed").build())
 *     .deserializationStage(new AvroDeserializationStage<>(MyEvent.class))
 *     .domainStage(new MyDomainStage())
 *     .serializationStage(new AvroSerializationStage<>(ProcessedEvent.class))
 *     .build()
 *     .execute("My Iggy Pipeline");
 * }</pre>
 *
 * <p>Architecture note: Future database connectivity can be added by injecting
 * repositories into the {@link DomainStage} implementations.
 *
 * @param <I> the deserialized input type
 * @param <O> the domain-processed output type
 */
public class IggyFlinkPipeline<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(IggyFlinkPipeline.class);

    private final IggyConnectionConfig sourceConnectionConfig;
    private final IggyConnectionConfig sinkConnectionConfig;
    private final IggySourceConfig sourceConfig;
    private final IggySinkConfig sinkConfig;
    private final DeserializationStage<I> deserializationStage;
    private final DomainStage<I, O> domainStage;
    private final SerializationStage<O> serializationStage;
    private final int parallelism;

    private IggyFlinkPipeline(Builder<I, O> builder) {
        this.sourceConnectionConfig = builder.sourceConnectionConfig;
        this.sinkConnectionConfig = builder.sinkConnectionConfig != null
                ? builder.sinkConnectionConfig
                : builder.sourceConnectionConfig;
        this.sourceConfig = builder.sourceConfig;
        this.sinkConfig = builder.sinkConfig;
        this.deserializationStage = builder.deserializationStage;
        this.domainStage = builder.domainStage;
        this.serializationStage = builder.serializationStage;
        this.parallelism = builder.parallelism;
    }

    /**
     * Builds and executes the Flink job.
     *
     * @param jobName the name for the Flink job
     * @throws Exception if the Flink job fails
     */
    public void execute(String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        buildPipeline(env);

        LOG.info("Executing Iggy Flink pipeline: {}", jobName);
        env.execute(jobName);
    }

    /**
     * Builds the Flink pipeline on the given execution environment without executing it.
     * Useful for testing or when sharing an environment across multiple jobs.
     *
     * @param env the Flink execution environment
     */
    public void buildPipeline(StreamExecutionEnvironment env) {
        LOG.info("Building Iggy Flink pipeline: source={}/{}, sink={}/{}",
                sourceConfig.getStreamName(), sourceConfig.getTopicName(),
                sinkConfig.getStreamName(), sinkConfig.getTopicName());

        // Stage 1: Read raw bytes from Iggy source
        DataStream<byte[]> rawSource = env.addSource(
                new IggySource(sourceConnectionConfig, sourceConfig))
                .name("Iggy Source [" + sourceConfig.getStreamName() + "/" + sourceConfig.getTopicName() + "]");

        // Stage 2: Deserialize raw bytes into domain input type
        DataStream<I> deserialized = rawSource.map(
                new DeserializeFunction<>(deserializationStage))
                .name("Deserialize [" + deserializationStage.getStageName() + "]");

        // Stage 3: Apply domain logic
        DataStream<O> processed = deserialized.map(domainStage)
                .name("Domain [" + domainStage.getStageName() + "]");

        // Stage 4: Serialize output back to bytes
        DataStream<byte[]> serialized = processed.map(
                new SerializeFunction<>(serializationStage))
                .name("Serialize [" + serializationStage.getStageName() + "]");

        // Stage 5: Write serialized bytes to Iggy sink
        serialized.addSink(new IggySink(sinkConnectionConfig, sinkConfig))
                .name("Iggy Sink [" + sinkConfig.getStreamName() + "/" + sinkConfig.getTopicName() + "]");
    }

    /**
     * Creates a new pipeline builder.
     */
    public static <I, O> Builder<I, O> builder() {
        return new Builder<>();
    }

    /**
     * Flink MapFunction wrapper for DeserializationStage.
     */
    private static class DeserializeFunction<T> implements MapFunction<byte[], T> {
        private final DeserializationStage<T> stage;

        DeserializeFunction(DeserializationStage<T> stage) {
            this.stage = stage;
        }

        @Override
        public T map(byte[] value) throws Exception {
            return stage.deserialize(value);
        }
    }

    /**
     * Flink MapFunction wrapper for SerializationStage.
     */
    private static class SerializeFunction<T> implements MapFunction<T, byte[]> {
        private final SerializationStage<T> stage;

        SerializeFunction(SerializationStage<T> stage) {
            this.stage = stage;
        }

        @Override
        public byte[] map(T value) throws Exception {
            return stage.serialize(value);
        }
    }

    /**
     * Builder for {@link IggyFlinkPipeline}.
     */
    public static class Builder<I, O> {
        private IggyConnectionConfig sourceConnectionConfig;
        private IggyConnectionConfig sinkConnectionConfig;
        private IggySourceConfig sourceConfig;
        private IggySinkConfig sinkConfig;
        private DeserializationStage<I> deserializationStage;
        private DomainStage<I, O> domainStage;
        private SerializationStage<O> serializationStage;
        private int parallelism = 1;

        /** Sets the connection config for both source and sink (same server). */
        public Builder<I, O> connectionConfig(IggyConnectionConfig connectionConfig) {
            this.sourceConnectionConfig = connectionConfig;
            this.sinkConnectionConfig = connectionConfig;
            return this;
        }

        /** Sets a separate connection config for the source. */
        public Builder<I, O> sourceConnectionConfig(IggyConnectionConfig sourceConnectionConfig) {
            this.sourceConnectionConfig = sourceConnectionConfig;
            return this;
        }

        /** Sets a separate connection config for the sink. */
        public Builder<I, O> sinkConnectionConfig(IggyConnectionConfig sinkConnectionConfig) {
            this.sinkConnectionConfig = sinkConnectionConfig;
            return this;
        }

        public Builder<I, O> sourceConfig(IggySourceConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public Builder<I, O> sinkConfig(IggySinkConfig sinkConfig) {
            this.sinkConfig = sinkConfig;
            return this;
        }

        public Builder<I, O> deserializationStage(DeserializationStage<I> deserializationStage) {
            this.deserializationStage = deserializationStage;
            return this;
        }

        public Builder<I, O> domainStage(DomainStage<I, O> domainStage) {
            this.domainStage = domainStage;
            return this;
        }

        public Builder<I, O> serializationStage(SerializationStage<O> serializationStage) {
            this.serializationStage = serializationStage;
            return this;
        }

        public Builder<I, O> parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public IggyFlinkPipeline<I, O> build() {
            if (sourceConnectionConfig == null) {
                throw new IllegalStateException("sourceConnectionConfig must be set");
            }
            if (sourceConfig == null) {
                throw new IllegalStateException("sourceConfig must be set");
            }
            if (sinkConfig == null) {
                throw new IllegalStateException("sinkConfig must be set");
            }
            if (deserializationStage == null) {
                throw new IllegalStateException("deserializationStage must be set");
            }
            if (domainStage == null) {
                throw new IllegalStateException("domainStage must be set");
            }
            if (serializationStage == null) {
                throw new IllegalStateException("serializationStage must be set");
            }
            return new IggyFlinkPipeline<>(this);
        }
    }
}
