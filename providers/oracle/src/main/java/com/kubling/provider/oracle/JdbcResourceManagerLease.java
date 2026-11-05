package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

final class JdbcResourceManagerLease implements AutoCloseable {
    static final String TABLE_NAME = "KUBLING_PROVIDER_TX_LOCKS";

    private final Connection connection;

    private JdbcResourceManagerLease(Connection connection) {
        this.connection = connection;
    }

    static JdbcResourceManagerLease acquire(
            ConnectionFactory connectionFactory, String resourceManagerId)
            throws SQLException {
        ensureRowExists(connectionFactory, resourceManagerId);

        Connection connection = connectionFactory.openConnection();
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(
                            "SELECT RESOURCE_MANAGER_ID FROM " + TABLE_NAME
                                    + " WHERE RESOURCE_MANAGER_ID = ? FOR UPDATE NOWAIT")) {
                statement.setString(1, resourceManagerId);
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        throw new SQLException("Oracle resource manager lease row was not found");
                    }
                }
            }
            return new JdbcResourceManagerLease(connection);
        } catch (SQLException | RuntimeException exception) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    private static void ensureRowExists(
            ConnectionFactory connectionFactory, String resourceManagerId)
            throws SQLException {
        String sql = "MERGE INTO " + TABLE_NAME + " target"
                + " USING (SELECT ? RESOURCE_MANAGER_ID FROM DUAL) source"
                + " ON (target.RESOURCE_MANAGER_ID = source.RESOURCE_MANAGER_ID)"
                + " WHEN NOT MATCHED THEN INSERT (RESOURCE_MANAGER_ID)"
                + " VALUES (source.RESOURCE_MANAGER_ID)";
        try (Connection connection = connectionFactory.openConnection()) {
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, resourceManagerId);
                statement.executeUpdate();
            } catch (SQLException exception) {
                if (exception.getErrorCode() != 1) {
                    throw exception;
                }
            }
        }
    }

    @Override
    public void close() throws SQLException {
        SQLException failure = null;
        try {
            connection.rollback();
        } catch (SQLException exception) {
            failure = exception;
        }
        try {
            connection.close();
        } catch (SQLException exception) {
            if (failure == null) {
                failure = exception;
            } else {
                failure.addSuppressed(exception);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
