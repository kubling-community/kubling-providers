package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

final class XaSession implements AutoCloseable {
    private final XAConnection xaConnection;
    private final Connection connection;
    private final XAResource xaResource;
    private final Runnable release;
    private final AtomicBoolean closed = new AtomicBoolean();

    XaSession(XAConnection xaConnection) throws SQLException {
        this(xaConnection, () -> {});
    }

    XaSession(XAConnection xaConnection, Runnable release) throws SQLException {
        this.xaConnection = xaConnection;
        this.release = release;
        Connection openedConnection = null;
        try {
            openedConnection = xaConnection.getConnection();
            this.xaResource = xaConnection.getXAResource();
            this.connection = openedConnection;
        } catch (SQLException | RuntimeException | Error exception) {
            if (openedConnection != null) {
                try {
                    openedConnection.close();
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

    Connection connection() {
        return connection;
    }

    XAResource xaResource() {
        return xaResource;
    }

    @Override
    public void close() throws SQLException {
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
        try {
            if (failure != null) {
                throw failure;
            }
        } finally {
            release.run();
        }
    }
}
