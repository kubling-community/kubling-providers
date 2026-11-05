package com.kubling.provider.oracle;

import java.io.IOException;
import java.sql.SQLException;
import javax.transaction.xa.XAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts the Kubling Oracle provider process. */
public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private Main() {}

    /**
     * Starts the provider and blocks until the process is stopped.
     *
     * @param arguments command-line arguments; currently unused
     */
    public static void main(String[] arguments) {
        int exitCode = run();
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static int run() {
        final ProviderConfig config;
        try {
            config = ProviderConfig.load();
        } catch (ProviderConfigurationException exception) {
            LOGGER.error("Invalid Oracle provider configuration: {}", exception.getMessage());
            return 2;
        }

        try {
            if (config.resourceManagerId().isPresent()) {
                runWithXaPool(config);
            } else {
                runWithJdbcPool(config);
            }
            return 0;
        } catch (SQLException exception) {
            if (exception.getErrorCode() == 54) {
                LOGGER.error(
                        "Another provider instance already owns the configured Oracle resource manager");
            } else {
                LOGGER.error(
                        "Unable to initialize Oracle JDBC connectivity (SQL state {}, vendor code {})",
                        exception.getSQLState(),
                        exception.getErrorCode());
                LOGGER.debug("Oracle JDBC startup failure", exception);
            }
            return 1;
        } catch (XAException exception) {
            LOGGER.error(
                    "Unable to initialize Oracle XA recovery (XA code {})",
                    exception.errorCode);
            return 1;
        } catch (IOException exception) {
            LOGGER.error(
                    "Unable to start the Oracle provider listener ({})",
                    exception.getClass().getSimpleName());
            return 1;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Oracle provider failed during startup ({})",
                    exception.getClass().getSimpleName());
            return 1;
        }
    }

    private static void runWithJdbcPool(ProviderConfig config)
            throws SQLException, IOException, InterruptedException {
        try (JdbcConnectionPool pool = JdbcConnectionPool.create(config)) {
            ProviderService service = new ProviderService(
                    connectionProbe(config, pool),
                    schemaDiscovery(config, pool),
                    pool,
                    config.schemaPolicy());
            serve(config, service);
        }
    }

    private static void runWithXaPool(ProviderConfig config)
            throws SQLException, XAException, IOException, InterruptedException {
        String resourceManagerId = config.resourceManagerId().orElseThrow();
        try (XaConnectionPool pool = XaConnectionPool.create(config)) {
            TransactionJournal journal = new JdbcTransactionJournal(pool, resourceManagerId);
            JdbcResourceManagerLease lease =
                    JdbcResourceManagerLease.acquire(pool, resourceManagerId);
            TransactionRecoveryManager recovery = new TransactionRecoveryManager(
                    resourceManagerId, journal, pool, lease);
            recovery.validate();
            ProviderService service = new ProviderService(
                    connectionProbe(config, pool),
                    schemaDiscovery(config, pool),
                    new XaLogicalConnectionFactory(pool, resourceManagerId, journal),
                    config.schemaPolicy(),
                    recovery);
            serve(config, service);
        }
    }

    private static ConnectionProbe connectionProbe(
            ProviderConfig config, ConnectionFactory connectionFactory) {
        return new JdbcConnectionProbe(
                connectionFactory,
                Math.toIntExact(config.healthTimeout().toSeconds()));
    }

    private static SchemaDiscovery schemaDiscovery(
            ProviderConfig config, ConnectionFactory connectionFactory) {
        return new JdbcSchemaDiscovery(connectionFactory, config.schemaPolicy());
    }

    private static void serve(ProviderConfig config, ProviderService service)
            throws IOException, InterruptedException {
        try (ProviderServer server = new ProviderServer(
                config.listenAddress(), config.shutdownGracePeriod(), service)) {
            Thread shutdownHook = new Thread(server::close, "oracle-provider-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                server.start();
                LOGGER.info(
                        "Oracle provider listening on {}:{}",
                        config.listenAddress().getHostString(),
                        server.port());
                server.awaitTermination();
            } finally {
                removeShutdownHook(shutdownHook);
            }
        }
    }

    private static void removeShutdownHook(Thread shutdownHook) {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // The JVM is already shutting down and owns the hook lifecycle.
        }
    }
}
