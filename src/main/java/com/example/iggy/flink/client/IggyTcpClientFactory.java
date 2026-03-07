package com.example.iggy.flink.client;

import com.example.iggy.flink.config.IggyConnectionConfig;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating and configuring Apache Iggy TCP clients.
 *
 * <p>This factory ensures that only TCP connections are used, as per the project requirement.
 * HTTP and other transport protocols are explicitly not supported.
 */
public class IggyTcpClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IggyTcpClientFactory.class);

    private IggyTcpClientFactory() {
        // Utility class - no instantiation
    }

    /**
     * Creates a new Iggy TCP client from the given connection configuration and logs in.
     *
     * @param config the connection configuration
     * @return an authenticated Iggy TCP client
     * @throws IggyClientException if connection or login fails
     */
    public static IggyTcpClient createAndLogin(IggyConnectionConfig config) {
        LOG.info("Creating Iggy TCP client: host={}, port={}", config.getHost(), config.getPort());
        try {
            IggyTcpClient.Builder builder = IggyTcpClient.builder()
                    .host(config.getHost())
                    .port(config.getPort());

            if (config.getConnectionTimeout() != null) {
                builder.connectionTimeout(config.getConnectionTimeout());
            }
            if (config.getRequestTimeout() != null) {
                builder.requestTimeout(config.getRequestTimeout());
            }

            IggyTcpClient client = builder.build();
            client.users().login(config.getUsername(), config.getPassword());
            LOG.info("Successfully connected and logged in to Iggy server at {}:{}",
                    config.getHost(), config.getPort());
            return client;
        } catch (Exception e) {
            throw new IggyClientException(
                    "Failed to create Iggy TCP client for " + config.getHost() + ":" + config.getPort(), e);
        }
    }

    /**
     * Creates a new Iggy TCP client without logging in.
     *
     * @param config the connection configuration
     * @return an unauthenticated Iggy TCP client
     */
    public static IggyTcpClient create(IggyConnectionConfig config) {
        LOG.info("Creating Iggy TCP client (no login): host={}, port={}", config.getHost(), config.getPort());
        return IggyTcpClient.builder()
                .host(config.getHost())
                .port(config.getPort())
                .build();
    }
}
