package com.kubling.provider.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class ConnectionRegistry implements AutoCloseable {
    private final LogicalConnectionFactory connectionFactory;
    private final ConcurrentMap<String, LogicalConnection> connections =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    ConnectionRegistry(ConnectionFactory connectionFactory) {
        this(connectionId -> new LogicalConnection(connectionFactory.openConnection()));
    }

    ConnectionRegistry(LogicalConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    String open() throws SQLException {
        if (closed.get()) {
            throw new SQLException("Oracle provider connection registry is closed");
        }
        String connectionId;
        do {
            connectionId = UUID.randomUUID().toString();
        } while (connections.containsKey(connectionId));
        LogicalConnection connection = connectionFactory.open(connectionId);
        if (closed.get()) {
            connection.close();
            throw new SQLException("Oracle provider connection registry is closed");
        }

        while (connections.putIfAbsent(connectionId, connection) != null) {
            connection.close();
            connectionId = UUID.randomUUID().toString();
            connection = connectionFactory.open(connectionId);
        }
        if (closed.get()) {
            if (connections.remove(connectionId, connection)) {
                connection.close();
            }
            throw new SQLException("Oracle provider connection registry is closed");
        }
        return connectionId;
    }

    LogicalConnection get(String connectionId) {
        return connections.get(connectionId);
    }

    boolean close(String connectionId) throws SQLException {
        LogicalConnection connection = connections.remove(connectionId);
        if (connection == null) {
            return false;
        }
        connection.close();
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        List<SQLException> failures = new ArrayList<>();
        for (String connectionId : List.copyOf(connections.keySet())) {
            try {
                close(connectionId);
            } catch (SQLException exception) {
                failures.add(exception);
            }
        }
        if (!failures.isEmpty()) {
            SQLException failure = new SQLException("Unable to close all Oracle connections");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }
}
