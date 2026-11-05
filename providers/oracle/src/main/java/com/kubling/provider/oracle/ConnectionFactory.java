package com.kubling.provider.oracle;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
interface ConnectionFactory {
    Connection openConnection() throws SQLException;
}
