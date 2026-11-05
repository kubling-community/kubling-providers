package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolXADataSource;
import org.junit.jupiter.api.Test;

final class UcpPoolsTest {
    private static final String JDBC_URL =
            "jdbc:oracle:thin:@//database:1521/FREEPDB1";

    @Test
    void configuresTheJdbcPoolAndDriverProperties() throws Exception {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret",
                ProviderConfig.TNS_ADMIN_KEY, "/oracle/network/admin",
                ProviderConfig.CONNECT_TIMEOUT_KEY, "8",
                ProviderConfig.POOL_MIN_SIZE_KEY, "2",
                ProviderConfig.POOL_MAX_SIZE_KEY, "12",
                ProviderConfig.POOL_CONNECTION_WAIT_KEY, "6",
                ProviderConfig.POOL_INACTIVE_TIMEOUT_KEY, "60",
                ProviderConfig.POOL_MAX_REUSE_KEY, "900"));

        PoolDataSource pool = UcpPools.jdbc(config, "oracle-jdbc-test");

        assertEquals("oracle-jdbc-test", pool.getConnectionPoolName());
        assertEquals("oracle.jdbc.pool.OracleDataSource", pool.getConnectionFactoryClassName());
        assertEquals(JDBC_URL, pool.getURL());
        assertEquals("kubling", pool.getUser());
        assertEquals(2, pool.getInitialPoolSize());
        assertEquals(2, pool.getMinPoolSize());
        assertEquals(12, pool.getMaxPoolSize());
        assertEquals(java.time.Duration.ofSeconds(6), pool.getConnectionWaitDuration());
        assertEquals(60, pool.getInactiveConnectionTimeout());
        assertEquals(900, pool.getMaxConnectionReuseTime());
        assertEquals(8, pool.getLoginTimeout());
        assertEquals(5, pool.getConnectionValidationTimeout());
        assertTrue(pool.getValidateConnectionOnBorrow());
        assertEquals(
                "/oracle/network/admin",
                pool.getConnectionProperty("oracle.net.tns_admin"));
        assertEquals("8000", pool.getConnectionProperty("oracle.net.CONNECT_TIMEOUT"));
    }

    @Test
    void configuresXaAndRegularBorrowsFromTheSamePool() throws Exception {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.RESOURCE_MANAGER_ID_KEY, "oracle-test-rm"));

        PoolXADataSource pool = UcpPools.xa(config, "oracle-xa-test");

        assertEquals("oracle-xa-test", pool.getConnectionPoolName());
        assertEquals(
                "oracle.jdbc.xa.client.OracleXADataSource",
                pool.getConnectionFactoryClassName());
        assertEquals(JDBC_URL, pool.getURL());
        assertEquals(20, pool.getMaxPoolSize());
        assertTrue(pool.getValidateConnectionOnBorrow());
    }

    @Test
    void reservesPoolCapacityForTheLeaseAndTransactionJournal() throws Exception {
        PoolXADataSource dataSource = proxy(
                PoolXADataSource.class,
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getXAConnection" -> xaConnection();
                    default -> defaultValue(method.getReturnType());
                });
        XaConnectionPool pool = new XaConnectionPool(dataSource, "unused-test-pool", 1, 0);

        XaSession first = pool.openSession();
        assertThrows(SQLTransientConnectionException.class, pool::openSession);

        first.close();
        first.close();
        XaSession second = pool.openSession();
        assertThrows(SQLTransientConnectionException.class, pool::openSession);
        second.close();
    }

    @Test
    void closesBothLayersOfAnAuxiliaryJdbcBorrow() throws Exception {
        AtomicBoolean connectionClosed = new AtomicBoolean();
        AtomicBoolean xaConnectionClosed = new AtomicBoolean();
        Connection connection = proxy(
                Connection.class,
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "close" -> {
                        connectionClosed.set(true);
                        yield null;
                    }
                    case "isClosed" -> connectionClosed.get();
                    default -> defaultValue(method.getReturnType());
                });
        XAConnection xaConnection = proxy(
                XAConnection.class,
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getConnection" -> connection;
                    case "close" -> {
                        xaConnectionClosed.set(true);
                        yield null;
                    }
                    default -> defaultValue(method.getReturnType());
                });
        PoolXADataSource dataSource = proxy(
                PoolXADataSource.class,
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getXAConnection" -> xaConnection;
                    default -> defaultValue(method.getReturnType());
                });
        XaConnectionPool pool = new XaConnectionPool(dataSource, "unused-test-pool", 1, 0);

        Connection borrowed = pool.openConnection();
        assertTrue(!borrowed.isClosed());
        borrowed.close();
        borrowed.close();

        assertTrue(connectionClosed.get());
        assertTrue(xaConnectionClosed.get());
        assertTrue(borrowed.isClosed());
    }

    private static XAConnection xaConnection() {
        Connection connection = proxy(
                Connection.class,
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        XAResource resource = proxy(
                XAResource.class,
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        return proxy(
                XAConnection.class,
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getConnection" -> connection;
                    case "getXAResource" -> resource;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
