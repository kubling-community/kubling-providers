package com.kubling.provider.oracle;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.XAConnection;

final class XaJdbcConnection implements InvocationHandler {
    private final XAConnection xaConnection;
    private final Connection connection;
    private final AtomicBoolean closed = new AtomicBoolean();

    private XaJdbcConnection(XAConnection xaConnection, Connection connection) {
        this.xaConnection = xaConnection;
        this.connection = connection;
    }

    static Connection open(XAConnection xaConnection) throws SQLException {
        Connection connection = null;
        try {
            connection = xaConnection.getConnection();
            return Connection.class.cast(Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class},
                    new XaJdbcConnection(xaConnection, connection)));
        } catch (SQLException | RuntimeException | Error exception) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            try {
                xaConnection.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
        return switch (method.getName()) {
            case "close" -> {
                close();
                yield null;
            }
            case "isClosed" -> closed.get() || connection.isClosed();
            default -> invokeDelegate(method, arguments);
        };
    }

    private Object invokeDelegate(Method method, Object[] arguments) throws Throwable {
        if (closed.get()) {
            throw new SQLException("Oracle XA-backed JDBC connection is closed");
        }
        try {
            return method.invoke(connection, arguments);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private void close() throws SQLException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        SQLException failure = null;
        try {
            connection.close();
        } catch (SQLException exception) {
            failure = exception;
        }
        try {
            xaConnection.close();
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
