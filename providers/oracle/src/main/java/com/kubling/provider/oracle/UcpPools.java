package com.kubling.provider.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.xa.client.OracleXADataSource;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.PoolXADataSource;

final class UcpPools {
    private static final String JDBC_CONNECTION_FACTORY = OracleDataSource.class.getName();
    private static final String XA_CONNECTION_FACTORY = OracleXADataSource.class.getName();

    private UcpPools() {}

    static PoolDataSource jdbc(
            ProviderConfig config, String poolName) throws SQLException {
        PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();
        configure(dataSource, config, poolName, JDBC_CONNECTION_FACTORY);
        return dataSource;
    }

    static PoolXADataSource xa(
            ProviderConfig config, String poolName) throws SQLException {
        PoolXADataSource dataSource = PoolDataSourceFactory.getPoolXADataSource();
        configure(dataSource, config, poolName, XA_CONNECTION_FACTORY);
        return dataSource;
    }

    static void destroy(String poolName) throws SQLException {
        try {
            UniversalConnectionPoolManager manager =
                    UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            if (Arrays.asList(manager.getConnectionPoolNames()).contains(poolName)) {
                manager.destroyConnectionPool(poolName);
            }
        } catch (UniversalConnectionPoolException exception) {
            throw new SQLException("Unable to close Oracle connection pool", exception);
        }
    }

    private static void configure(
            PoolDataSource dataSource,
            ProviderConfig config,
            String poolName,
            String connectionFactoryClassName)
            throws SQLException {
        dataSource.setConnectionPoolName(poolName);
        dataSource.setConnectionFactoryClassName(connectionFactoryClassName);
        config.connectionSettings().configure(dataSource);
        config.poolSettings().configure(dataSource);
        dataSource.setConnectionValidationTimeout(
                Math.toIntExact(config.healthTimeout().toSeconds()));
    }
}
