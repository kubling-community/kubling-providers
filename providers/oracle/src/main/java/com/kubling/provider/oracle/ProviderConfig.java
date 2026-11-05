package com.kubling.provider.oracle;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

final class ProviderConfig {
    static final String LISTEN_KEY = "KUBLING_ORACLE_LISTEN";
    static final String JDBC_URL_KEY = "KUBLING_ORACLE_JDBC_URL";
    static final String USERNAME_KEY = "KUBLING_ORACLE_USERNAME";
    static final String USERNAME_FILE_KEY = "KUBLING_ORACLE_USERNAME_FILE";
    static final String PASSWORD_KEY = "KUBLING_ORACLE_PASSWORD";
    static final String PASSWORD_FILE_KEY = "KUBLING_ORACLE_PASSWORD_FILE";
    static final String JDBC_PROPERTIES_FILE_KEY = "KUBLING_ORACLE_JDBC_PROPERTIES_FILE";
    static final String TNS_ADMIN_KEY = "KUBLING_ORACLE_TNS_ADMIN";
    static final String SCHEMA_ALLOWLIST_KEY = "KUBLING_ORACLE_SCHEMA_ALLOWLIST";
    static final String SCHEMA_DENYLIST_KEY = "KUBLING_ORACLE_SCHEMA_DENYLIST";
    static final String CONNECT_TIMEOUT_KEY = "KUBLING_ORACLE_CONNECT_TIMEOUT_SECONDS";
    static final String HEALTH_TIMEOUT_KEY = "KUBLING_ORACLE_HEALTH_TIMEOUT_SECONDS";
    static final String SHUTDOWN_GRACE_KEY = "KUBLING_ORACLE_SHUTDOWN_GRACE_SECONDS";
    static final String RESOURCE_MANAGER_ID_KEY = "KUBLING_ORACLE_RESOURCE_MANAGER_ID";
    static final String POOL_MIN_SIZE_KEY = "KUBLING_ORACLE_POOL_MIN_SIZE";
    static final String POOL_MAX_SIZE_KEY = "KUBLING_ORACLE_POOL_MAX_SIZE";
    static final String POOL_CONNECTION_WAIT_KEY =
            "KUBLING_ORACLE_POOL_CONNECTION_WAIT_SECONDS";
    static final String POOL_INACTIVE_TIMEOUT_KEY =
            "KUBLING_ORACLE_POOL_INACTIVE_TIMEOUT_SECONDS";
    static final String POOL_MAX_REUSE_KEY =
            "KUBLING_ORACLE_POOL_MAX_REUSE_SECONDS";

    private static final String DEFAULT_LISTEN = "0.0.0.0:50051";
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 15;
    private static final int DEFAULT_HEALTH_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_SHUTDOWN_GRACE_SECONDS = 10;
    private static final int DEFAULT_POOL_MIN_SIZE = 0;
    private static final int DEFAULT_POOL_MAX_SIZE = 20;
    private static final int DEFAULT_POOL_CONNECTION_WAIT_SECONDS = 30;
    private static final int DEFAULT_POOL_INACTIVE_TIMEOUT_SECONDS = 300;
    private static final int DEFAULT_POOL_MAX_REUSE_SECONDS = 3600;
    private static final String TNS_ADMIN_PROPERTY = "oracle.net.tns_admin";

    private final InetSocketAddress listenAddress;
    private final ConnectionSettings connectionSettings;
    private final PoolSettings poolSettings;
    private final SchemaPolicy schemaPolicy;
    private final Duration healthTimeout;
    private final Duration shutdownGracePeriod;
    private final Optional<String> resourceManagerId;

    private ProviderConfig(
            InetSocketAddress listenAddress,
            ConnectionSettings connectionSettings,
            PoolSettings poolSettings,
            SchemaPolicy schemaPolicy,
            Duration healthTimeout,
            Duration shutdownGracePeriod,
            Optional<String> resourceManagerId) {
        this.listenAddress = listenAddress;
        this.connectionSettings = connectionSettings;
        this.poolSettings = poolSettings;
        this.schemaPolicy = schemaPolicy;
        this.healthTimeout = healthTimeout;
        this.shutdownGracePeriod = shutdownGracePeriod;
        this.resourceManagerId = resourceManagerId;
    }

    static ProviderConfig load() {
        return fromEnvironment(System.getenv());
    }

    static ProviderConfig fromEnvironment(Map<String, String> environment) {
        String listen = optionalNonBlank(environment, LISTEN_KEY, DEFAULT_LISTEN);
        String jdbcUrl = requiredNonBlank(environment, JDBC_URL_KEY);
        Optional<Credentials> credentials = credentials(environment);
        Properties jdbcProperties = jdbcProperties(environment);
        optionalNonBlank(environment, TNS_ADMIN_KEY)
                .ifPresent(value -> jdbcProperties.setProperty(TNS_ADMIN_PROPERTY, value));
        Set<String> schemaAllowlist = identifierSet(environment, SCHEMA_ALLOWLIST_KEY);
        Set<String> schemaDenylist = identifierSet(environment, SCHEMA_DENYLIST_KEY);
        int connectTimeoutSeconds = boundedInteger(
                environment,
                CONNECT_TIMEOUT_KEY,
                DEFAULT_CONNECT_TIMEOUT_SECONDS,
                1,
                300);
        int healthTimeoutSeconds = boundedInteger(
                environment,
                HEALTH_TIMEOUT_KEY,
                DEFAULT_HEALTH_TIMEOUT_SECONDS,
                1,
                60);
        int shutdownGraceSeconds = boundedInteger(
                environment,
                SHUTDOWN_GRACE_KEY,
                DEFAULT_SHUTDOWN_GRACE_SECONDS,
                1,
                120);
        Optional<String> resourceManagerId = resourceManagerId(environment);
        int minimumPoolSize = boundedInteger(
                environment, POOL_MIN_SIZE_KEY, DEFAULT_POOL_MIN_SIZE, 0, 1000);
        int maximumPoolSize = boundedInteger(
                environment, POOL_MAX_SIZE_KEY, DEFAULT_POOL_MAX_SIZE, 1, 1000);
        if (minimumPoolSize > maximumPoolSize) {
            throw new ProviderConfigurationException(
                    POOL_MIN_SIZE_KEY + " must not exceed " + POOL_MAX_SIZE_KEY);
        }
        if (resourceManagerId.isPresent() && maximumPoolSize < 3) {
            throw new ProviderConfigurationException(
                    POOL_MAX_SIZE_KEY + " must be at least 3 when "
                            + RESOURCE_MANAGER_ID_KEY + " is set");
        }
        int poolConnectionWaitSeconds = boundedInteger(
                environment,
                POOL_CONNECTION_WAIT_KEY,
                DEFAULT_POOL_CONNECTION_WAIT_SECONDS,
                1,
                300);
        int poolInactiveTimeoutSeconds = boundedInteger(
                environment,
                POOL_INACTIVE_TIMEOUT_KEY,
                DEFAULT_POOL_INACTIVE_TIMEOUT_SECONDS,
                0,
                86_400);
        int poolMaxReuseSeconds = boundedInteger(
                environment,
                POOL_MAX_REUSE_KEY,
                DEFAULT_POOL_MAX_REUSE_SECONDS,
                0,
                86_400);

        return new ProviderConfig(
                parseListenAddress(listen),
                new ConnectionSettings(
                        jdbcUrl,
                        credentials,
                        jdbcProperties,
                        Duration.ofSeconds(connectTimeoutSeconds)),
                new PoolSettings(
                        minimumPoolSize,
                        maximumPoolSize,
                        Duration.ofSeconds(poolConnectionWaitSeconds),
                        Duration.ofSeconds(poolInactiveTimeoutSeconds),
                        Duration.ofSeconds(poolMaxReuseSeconds)),
                new SchemaPolicy(schemaAllowlist, schemaDenylist),
                Duration.ofSeconds(healthTimeoutSeconds),
                Duration.ofSeconds(shutdownGraceSeconds),
                resourceManagerId);
    }

    InetSocketAddress listenAddress() {
        return listenAddress;
    }

    ConnectionSettings connectionSettings() {
        return connectionSettings;
    }

    PoolSettings poolSettings() {
        return poolSettings;
    }

    SchemaPolicy schemaPolicy() {
        return schemaPolicy;
    }

    Duration healthTimeout() {
        return healthTimeout;
    }

    Duration shutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    Optional<String> resourceManagerId() {
        return resourceManagerId;
    }

    private static Optional<String> resourceManagerId(Map<String, String> environment) {
        String value = environment.get(RESOURCE_MANAGER_ID_KEY);
        if (value == null) {
            return Optional.empty();
        }
        if (value.isEmpty() || !value.strip().equals(value)) {
            throw new ProviderConfigurationException(
                    RESOURCE_MANAGER_ID_KEY
                            + " must be a non-empty canonical identifier without surrounding whitespace");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw new ProviderConfigurationException(
                    RESOURCE_MANAGER_ID_KEY + " must not exceed 256 UTF-8 bytes");
        }
        return Optional.of(value);
    }

    private static String optionalNonBlank(
            Map<String, String> environment, String key, String fallback) {
        String value = environment.get(key);
        if (value == null) {
            return fallback;
        }
        if (value.isBlank()) {
            throw new ProviderConfigurationException(key + " must not be blank");
        }
        return value;
    }

    private static String requiredNonBlank(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new ProviderConfigurationException(key + " is required");
        }
        return value;
    }

    private static Optional<Credentials> credentials(Map<String, String> environment) {
        Optional<String> username = secretValue(
                environment, USERNAME_KEY, USERNAME_FILE_KEY);
        Optional<String> password = secretValue(
                environment, PASSWORD_KEY, PASSWORD_FILE_KEY);
        if (username.isPresent() != password.isPresent()) {
            throw new ProviderConfigurationException(
                    "configure both Oracle username and password, or neither for driver-managed authentication");
        }
        return username.map(value -> new Credentials(value, password.orElseThrow()));
    }

    private static Optional<String> secretValue(
            Map<String, String> environment, String valueKey, String fileKey) {
        String directValue = environment.get(valueKey);
        String fileValue = environment.get(fileKey);
        boolean hasDirectValue = directValue != null;
        boolean hasFileValue = fileValue != null;

        if (hasDirectValue && hasFileValue) {
            throw new ProviderConfigurationException(
                    "configure exactly one of " + valueKey + " and " + fileKey);
        }
        if (hasDirectValue) {
            if (directValue.isEmpty()) {
                throw new ProviderConfigurationException(valueKey + " must not be empty");
            }
            return Optional.of(directValue);
        }
        if (!hasFileValue) {
            return Optional.empty();
        }
        if (fileValue.isBlank()) {
            throw new ProviderConfigurationException(fileKey + " must not be blank");
        }

        try {
            String value = Files.readString(Path.of(fileValue), StandardCharsets.UTF_8);
            value = removeSingleTrailingLineEnding(value);
            if (value.isEmpty()) {
                throw new ProviderConfigurationException(
                        "the secret file named by " + fileKey + " is empty");
            }
            return Optional.of(value);
        } catch (IOException | InvalidPathException exception) {
            throw new ProviderConfigurationException(
                    "unable to read the secret file named by " + fileKey,
                    exception);
        }
    }

    private static Properties jdbcProperties(Map<String, String> environment) {
        String configuredPath = environment.get(JDBC_PROPERTIES_FILE_KEY);
        if (configuredPath == null) {
            return new Properties();
        }
        if (configuredPath.isBlank()) {
            throw new ProviderConfigurationException(
                    JDBC_PROPERTIES_FILE_KEY + " must not be blank");
        }

        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(
                Path.of(configuredPath), StandardCharsets.UTF_8)) {
            properties.load(reader);
            return properties;
        } catch (IOException | InvalidPathException exception) {
            throw new ProviderConfigurationException(
                    "unable to read the JDBC properties file named by "
                            + JDBC_PROPERTIES_FILE_KEY,
                    exception);
        }
    }

    private static Optional<String> optionalNonBlank(
            Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (value.isBlank()) {
            throw new ProviderConfigurationException(key + " must not be blank");
        }
        return Optional.of(value);
    }

    private static String removeSingleTrailingLineEnding(String value) {
        if (value.endsWith("\r\n")) {
            return value.substring(0, value.length() - 2);
        }
        if (value.endsWith("\n") || value.endsWith("\r")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static int boundedInteger(
            Map<String, String> environment,
            String key,
            int fallback,
            int minimum,
            int maximum) {
        String value = environment.get(key);
        if (value == null) {
            return fallback;
        }

        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new ProviderConfigurationException(key + " must be an integer", exception);
        }
        if (parsed < minimum || parsed > maximum) {
            throw new ProviderConfigurationException(
                    key + " must be between " + minimum + " and " + maximum);
        }
        return parsed;
    }

    private static Set<String> identifierSet(Map<String, String> environment, String key) {
        String configured = environment.get(key);
        if (configured == null || configured.isBlank()) {
            return Set.of();
        }

        Set<String> identifiers = new LinkedHashSet<>();
        for (String candidate : configured.split(",", -1)) {
            String identifier = candidate.strip();
            if (identifier.isEmpty()) {
                throw new ProviderConfigurationException(
                        key + " must contain comma-separated schema names");
            }
            identifiers.add(identifier);
        }
        return Set.copyOf(identifiers);
    }

    private static InetSocketAddress parseListenAddress(String value) {
        String host;
        String portText;

        if (value.startsWith("[")) {
            int closingBracket = value.indexOf(']');
            if (closingBracket < 0
                    || closingBracket + 1 >= value.length()
                    || value.charAt(closingBracket + 1) != ':') {
                throw invalidListenAddress();
            }
            host = value.substring(1, closingBracket);
            portText = value.substring(closingBracket + 2);
        } else {
            int separator = value.lastIndexOf(':');
            if (separator < 0 || value.indexOf(':') != separator) {
                throw invalidListenAddress();
            }
            host = value.substring(0, separator);
            portText = value.substring(separator + 1);
        }

        if (host.isEmpty()) {
            host = "0.0.0.0";
        }

        final int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException exception) {
            throw invalidListenAddress(exception);
        }
        if (port < 1 || port > 65_535) {
            throw invalidListenAddress();
        }

        return new InetSocketAddress(host, port);
    }

    private static ProviderConfigurationException invalidListenAddress() {
        return new ProviderConfigurationException(
                LISTEN_KEY + " must use host:port or [ipv6-address]:port");
    }

    private static ProviderConfigurationException invalidListenAddress(Throwable cause) {
        return new ProviderConfigurationException(
                LISTEN_KEY + " must use host:port or [ipv6-address]:port",
                cause);
    }
}
