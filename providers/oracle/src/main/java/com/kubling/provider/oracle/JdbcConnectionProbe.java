package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.SQLException;

final class JdbcConnectionProbe implements ConnectionProbe {
    private final ConnectionFactory connectionFactory;
    private final int timeoutSeconds;

    JdbcConnectionProbe(
            ConnectionFactory connectionFactory,
            int timeoutSeconds) {
        this.connectionFactory = connectionFactory;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public boolean isHealthy() throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            return connection.isValid(timeoutSeconds);
        }
    }
}
