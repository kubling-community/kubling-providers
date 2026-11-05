package com.kubling.provider.oracle;

import java.sql.SQLException;

@FunctionalInterface
interface LogicalConnectionFactory {
    LogicalConnection open(String connectionId) throws SQLException;
}
