package com.example.iggy.flink.sink;

import com.example.iggy.flink.config.IggyConnectionConfig;
import com.example.iggy.flink.config.IggySinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * Flink 2.0 Sink implementation that publishes raw byte messages to an Apache Iggy topic via TCP.
 *
 * <p>This sink receives serialized byte arrays from the Flink pipeline and publishes them
 * to the configured Iggy stream/topic using only the TCP protocol (no HTTP/REST).
 *
 * <p>Serialization is handled upstream by a
 * {@link com.example.iggy.flink.pipeline.SerializationStage}.
 *
 * <p>Usage with Flink 2.0:
 * <pre>{@code
 * dataStream.sinkTo(new IggySink(connectionConfig, sinkConfig));
 * }</pre>
 */
public class IggySink implements Sink<byte[]>, Serializable {

    private static final long serialVersionUID = 1L;

    private final IggyConnectionConfig connectionConfig;
    private final IggySinkConfig sinkConfig;

    /**
     * Creates a new Iggy sink.
     *
     * @param connectionConfig TCP connection configuration for the Iggy server
     * @param sinkConfig       configuration for which stream/topic to publish to
     */
    public IggySink(IggyConnectionConfig connectionConfig, IggySinkConfig sinkConfig) {
        this.connectionConfig = connectionConfig;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public SinkWriter<byte[]> createWriter(WriterInitContext context) throws IOException {
        return new IggySinkWriter(connectionConfig, sinkConfig);
    }
}
