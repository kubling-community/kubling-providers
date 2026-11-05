package com.kubling.provider.oracle;

import java.sql.SQLException;

@FunctionalInterface
interface XaSessionFactory {
    XaSession openSession() throws SQLException;
}
