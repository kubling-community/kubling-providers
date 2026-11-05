package com.kubling.provider.oracle;

import java.sql.SQLException;
import java.time.Duration;
import oracle.ucp.jdbc.PoolDataSource;

record PoolSettings(
        int minimumSize,
        int maximumSize,
        Duration connectionWaitTimeout,
        Duration inactiveConnectionTimeout,
        Duration maximumConnectionReuseTime) {

    void configure(PoolDataSource dataSource) throws SQLException {
        dataSource.setInitialPoolSize(minimumSize);
        dataSource.setMinPoolSize(minimumSize);
        dataSource.setMaxPoolSize(maximumSize);
        dataSource.setConnectionWaitDuration(connectionWaitTimeout);
        dataSource.setInactiveConnectionTimeout(
                Math.toIntExact(inactiveConnectionTimeout.toSeconds()));
        dataSource.setMaxConnectionReuseTime(maximumConnectionReuseTime.toSeconds());
        dataSource.setValidateConnectionOnBorrow(true);
    }
}
