package com.kubling.provider.oracle;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import oracle.ucp.jdbc.PoolDataSource;

final class ConnectionSettings {
    private static final String CONNECT_TIMEOUT_PROPERTY = "oracle.net.CONNECT_TIMEOUT";

    private final String jdbcUrl;
    private final Optional<Credentials> credentials;
    private final Properties properties;
    private final Duration connectTimeout;

    ConnectionSettings(
            String jdbcUrl,
            Optional<Credentials> credentials,
            Properties properties,
            Duration connectTimeout) {
        this.jdbcUrl = jdbcUrl;
        this.credentials = credentials;
        this.properties = copyOf(properties);
        this.connectTimeout = connectTimeout;
    }

    String jdbcUrl() {
        return jdbcUrl;
    }

    Optional<Credentials> credentials() {
        return credentials;
    }

    Properties properties() {
        return copyOf(properties);
    }

    Duration connectTimeout() {
        return connectTimeout;
    }

    void configure(PoolDataSource dataSource) throws SQLException {
        Properties configured = properties();
        configured.setProperty(
                CONNECT_TIMEOUT_PROPERTY, Long.toString(connectTimeout.toMillis()));
        dataSource.setConnectionProperties(configured);
        dataSource.setURL(jdbcUrl);
        dataSource.setLoginTimeout(Math.toIntExact(connectTimeout.toSeconds()));
        if (credentials.isPresent()) {
            Credentials configuredCredentials = credentials.orElseThrow();
            dataSource.setUser(configuredCredentials.username());
            dataSource.setPassword(configuredCredentials.password());
        }
    }

    private static Properties copyOf(Properties source) {
        Properties copy = new Properties();
        copy.putAll(source);
        return copy;
    }
}
