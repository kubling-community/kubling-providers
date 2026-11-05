package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import oracle.ucp.jdbc.PoolXADataSource;

final class XaConnectionPool implements ConnectionFactory, XaSessionFactory, AutoCloseable {
    private final PoolXADataSource dataSource;
    private final String poolName;
    private final Semaphore sessionPermits;
    private final long sessionWaitSeconds;

    XaConnectionPool(
            PoolXADataSource dataSource,
            String poolName,
            int maximumSessions,
            long sessionWaitSeconds) {
        this.dataSource = dataSource;
        this.poolName = poolName;
        this.sessionPermits = new Semaphore(maximumSessions, true);
        this.sessionWaitSeconds = sessionWaitSeconds;
    }

    static XaConnectionPool create(ProviderConfig config) throws SQLException {
        String poolName = "kubling-oracle-xa-" + UUID.randomUUID();
        return new XaConnectionPool(
                UcpPools.xa(config, poolName),
                poolName,
                config.poolSettings().maximumSize() - 2,
                config.poolSettings().connectionWaitTimeout().toSeconds());
    }

    @Override
    public Connection openConnection() throws SQLException {
        return XaJdbcConnection.open(dataSource.getXAConnection());
    }

    @Override
    public XaSession openSession() throws SQLException {
        final boolean acquired;
        try {
            acquired = sessionPermits.tryAcquire(sessionWaitSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new SQLTransientConnectionException(
                    "Interrupted while waiting for an Oracle XA session", exception);
        }
        if (!acquired) {
            throw new SQLTransientConnectionException(
                    "Timed out waiting for an Oracle XA session");
        }
        try {
            return new XaSession(dataSource.getXAConnection(), sessionPermits::release);
        } catch (SQLException | RuntimeException | Error exception) {
            sessionPermits.release();
            throw exception;
        }
    }

    @Override
    public void close() throws SQLException {
        UcpPools.destroy(poolName);
    }
}
