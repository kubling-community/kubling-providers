package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import oracle.ucp.jdbc.PoolDataSource;

final class JdbcConnectionPool implements ConnectionFactory, AutoCloseable {
    private final PoolDataSource dataSource;
    private final String poolName;

    private JdbcConnectionPool(PoolDataSource dataSource, String poolName) {
        this.dataSource = dataSource;
        this.poolName = poolName;
    }

    static JdbcConnectionPool create(ProviderConfig config) throws SQLException {
        String poolName = "kubling-oracle-jdbc-" + UUID.randomUUID();
        return new JdbcConnectionPool(UcpPools.jdbc(config, poolName), poolName);
    }

    @Override
    public Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() throws SQLException {
        UcpPools.destroy(poolName);
    }
}
