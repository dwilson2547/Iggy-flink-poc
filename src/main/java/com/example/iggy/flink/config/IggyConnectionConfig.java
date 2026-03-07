package com.example.iggy.flink.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for connecting to an Apache Iggy server via TCP.
 *
 * <p>This configuration is used by both the source and sink components to establish
 * TCP connections to the Iggy server. Only TCP protocol is supported.
 *
 * <p>Designed with future extensibility in mind (e.g., TLS, database connectivity).
 */
public class IggyConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;

    private IggyConnectionConfig(Builder builder) {
        this.host = Objects.requireNonNull(builder.host, "host must not be null");
        this.port = builder.port;
        this.username = Objects.requireNonNull(builder.username, "username must not be null");
        this.password = Objects.requireNonNull(builder.password, "password must not be null");
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 8090;
        private String username = "iggy";
        private String password = "iggy";
        private Duration connectionTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public IggyConnectionConfig build() {
            return new IggyConnectionConfig(this);
        }
    }

    @Override
    public String toString() {
        return "IggyConnectionConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", requestTimeout=" + requestTimeout +
                '}';
    }
}
