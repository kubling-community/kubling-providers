package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ProviderConfigTest {
    private static final String JDBC_URL = "jdbc:oracle:thin:@//database:1521/FREEPDB1";

    @TempDir
    Path temporaryDirectory;

    @Test
    void loadsDirectCredentialsAndDefaults() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret"));

        assertEquals("0.0.0.0", config.listenAddress().getHostString());
        assertEquals(50_051, config.listenAddress().getPort());
        assertEquals(JDBC_URL, config.connectionSettings().jdbcUrl());
        Credentials credentials = config.connectionSettings().credentials().orElseThrow();
        assertEquals("kubling", credentials.username());
        assertEquals("secret", credentials.password());
        assertEquals(Duration.ofSeconds(15), config.connectionSettings().connectTimeout());
        assertEquals(0, config.poolSettings().minimumSize());
        assertEquals(20, config.poolSettings().maximumSize());
        assertEquals(Duration.ofSeconds(30), config.poolSettings().connectionWaitTimeout());
        assertEquals(Duration.ofSeconds(300), config.poolSettings().inactiveConnectionTimeout());
        assertEquals(Duration.ofSeconds(3600), config.poolSettings().maximumConnectionReuseTime());
        assertTrue(config.schemaPolicy().includes("APPLICATION"));
        assertFalse(config.schemaPolicy().includes("SYS"));
        assertFalse(config.schemaPolicy().includes("SYSTEM"));
        assertEquals(Duration.ofSeconds(5), config.healthTimeout());
        assertEquals(Duration.ofSeconds(10), config.shutdownGracePeriod());
        assertTrue(config.resourceManagerId().isEmpty());
    }

    @Test
    void readsCredentialsFromFilesWithoutRetainingTheLineEnding() throws IOException {
        Path usernameFile = temporaryDirectory.resolve("username");
        Path passwordFile = temporaryDirectory.resolve("password");
        Files.writeString(usernameFile, "service-user\n");
        Files.writeString(passwordFile, " leading and trailing spaces \r\n");

        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_FILE_KEY, usernameFile.toString(),
                ProviderConfig.PASSWORD_FILE_KEY, passwordFile.toString()));

        Credentials credentials = config.connectionSettings().credentials().orElseThrow();
        assertEquals("service-user", credentials.username());
        assertEquals(" leading and trailing spaces ", credentials.password());
    }

    @Test
    void rejectsAmbiguousSecretConfigurationWithoutEchoingTheSecret() {
        Map<String, String> environment = new HashMap<>();
        environment.put(ProviderConfig.JDBC_URL_KEY, JDBC_URL);
        environment.put(ProviderConfig.USERNAME_KEY, "direct-user");
        environment.put(ProviderConfig.USERNAME_FILE_KEY, "/not/read");
        environment.put(ProviderConfig.PASSWORD_KEY, "do-not-echo");

        ProviderConfigurationException exception = assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(environment));

        assertFalse(exception.getMessage().contains("direct-user"));
        assertFalse(exception.getMessage().contains("do-not-echo"));
        assertFalse(exception.getMessage().contains("/not/read"));
    }

    @Test
    void acceptsDriverManagedAuthenticationWithoutInlineCredentials() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL));

        assertTrue(config.connectionSettings().credentials().isEmpty());
    }

    @Test
    void rejectsPartialCredentials() {
        ProviderConfigurationException exception = assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(Map.of(
                        ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                        ProviderConfig.USERNAME_KEY, "kubling")));

        assertEquals(
                "configure both Oracle username and password, or neither for driver-managed authentication",
                exception.getMessage());
    }

    @Test
    void loadsDriverPropertiesAndLetsExplicitTnsAdminWin() throws IOException {
        Path propertiesFile = temporaryDirectory.resolve("ojdbc.properties");
        Files.writeString(
                propertiesFile,
                "oracle.jdbc.tokenAuthentication=OAUTH\n"
                        + "oracle.jdbc.tokenLocation=/run/secrets/database-token\n"
                        + "oracle.net.tns_admin=/from-properties\n");

        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, "jdbc:oracle:thin:@database_alias",
                ProviderConfig.JDBC_PROPERTIES_FILE_KEY, propertiesFile.toString(),
                ProviderConfig.TNS_ADMIN_KEY, "/run/secrets/oracle-network"));

        Properties properties = config.connectionSettings().properties();
        assertEquals("OAUTH", properties.getProperty("oracle.jdbc.tokenAuthentication"));
        assertEquals(
                "/run/secrets/database-token",
                properties.getProperty("oracle.jdbc.tokenLocation"));
        assertEquals(
                "/run/secrets/oracle-network",
                properties.getProperty("oracle.net.tns_admin"));
        assertTrue(config.connectionSettings().credentials().isEmpty());
    }

    @Test
    void loadsConfigurablePoolBoundsAndTimeouts() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.CONNECT_TIMEOUT_KEY, "12",
                ProviderConfig.POOL_MIN_SIZE_KEY, "2",
                ProviderConfig.POOL_MAX_SIZE_KEY, "40",
                ProviderConfig.POOL_CONNECTION_WAIT_KEY, "7",
                ProviderConfig.POOL_INACTIVE_TIMEOUT_KEY, "90",
                ProviderConfig.POOL_MAX_REUSE_KEY, "600"));

        assertEquals(Duration.ofSeconds(12), config.connectionSettings().connectTimeout());
        assertEquals(2, config.poolSettings().minimumSize());
        assertEquals(40, config.poolSettings().maximumSize());
        assertEquals(Duration.ofSeconds(7), config.poolSettings().connectionWaitTimeout());
        assertEquals(Duration.ofSeconds(90), config.poolSettings().inactiveConnectionTimeout());
        assertEquals(Duration.ofSeconds(600), config.poolSettings().maximumConnectionReuseTime());
    }

    @Test
    void rejectsInvalidPoolBoundsAndAnUndersizedRecoveryPool() {
        assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(Map.of(
                        ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                        ProviderConfig.POOL_MIN_SIZE_KEY, "4",
                        ProviderConfig.POOL_MAX_SIZE_KEY, "3")));

        assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(Map.of(
                        ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                        ProviderConfig.RESOURCE_MANAGER_ID_KEY, "oracle-production-eu",
                        ProviderConfig.POOL_MAX_SIZE_KEY, "2")));
    }

    @Test
    void acceptsBracketedIpv6AndExplicitTimeouts() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.LISTEN_KEY, "[::1]:50052",
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret",
                ProviderConfig.HEALTH_TIMEOUT_KEY, "9",
                ProviderConfig.SHUTDOWN_GRACE_KEY, "20"));

        assertEquals("0:0:0:0:0:0:0:1", config.listenAddress().getAddress().getHostAddress());
        assertEquals(50_052, config.listenAddress().getPort());
        assertEquals(Duration.ofSeconds(9), config.healthTimeout());
        assertEquals(Duration.ofSeconds(20), config.shutdownGracePeriod());
    }

    @Test
    void acceptsAStableResourceManagerIdentity() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret",
                ProviderConfig.RESOURCE_MANAGER_ID_KEY, "oracle-production-eu"));

        assertEquals("oracle-production-eu", config.resourceManagerId().orElseThrow());
    }

    @Test
    void rejectsNonCanonicalOrOversizedResourceManagerIdentities() {
        Map<String, String> base = new HashMap<>();
        base.put(ProviderConfig.JDBC_URL_KEY, JDBC_URL);
        base.put(ProviderConfig.USERNAME_KEY, "kubling");
        base.put(ProviderConfig.PASSWORD_KEY, "secret");

        base.put(ProviderConfig.RESOURCE_MANAGER_ID_KEY, " oracle-rm");
        assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(base));

        base.put(ProviderConfig.RESOURCE_MANAGER_ID_KEY, "x".repeat(257));
        assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(base));
    }

    @Test
    void rejectsUnbracketedIpv6() {
        Map<String, String> environment = Map.of(
                ProviderConfig.LISTEN_KEY, "::1:50051",
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret");

        ProviderConfigurationException exception = assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(environment));

        assertEquals(
                "KUBLING_ORACLE_LISTEN must use host:port or [ipv6-address]:port",
                exception.getMessage());
    }

    @Test
    void appliesExactSchemaAllowlistAndDenylist() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret",
                ProviderConfig.SCHEMA_ALLOWLIST_KEY, "Mixed Schema, SYS, app_%",
                ProviderConfig.SCHEMA_DENYLIST_KEY, "app_%"));

        assertTrue(config.schemaPolicy().includes("Mixed Schema"));
        assertTrue(config.schemaPolicy().includes("SYS"));
        assertFalse(config.schemaPolicy().includes("mixed schema"));
        assertFalse(config.schemaPolicy().includes("app_%"));
        assertFalse(config.schemaPolicy().includes("APPLICATION"));
    }

    @Test
    void configuredDenylistWinsOverAllowlist() {
        ProviderConfig config = ProviderConfig.fromEnvironment(Map.of(
                ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                ProviderConfig.USERNAME_KEY, "kubling",
                ProviderConfig.PASSWORD_KEY, "secret",
                ProviderConfig.SCHEMA_ALLOWLIST_KEY, "APP,REPORTING",
                ProviderConfig.SCHEMA_DENYLIST_KEY, "REPORTING"));

        assertTrue(config.schemaPolicy().includes("APP"));
        assertFalse(config.schemaPolicy().includes("REPORTING"));
    }

    @Test
    void rejectsEmptySchemaListEntries() {
        ProviderConfigurationException exception = assertThrows(
                ProviderConfigurationException.class,
                () -> ProviderConfig.fromEnvironment(Map.of(
                        ProviderConfig.JDBC_URL_KEY, JDBC_URL,
                        ProviderConfig.USERNAME_KEY, "kubling",
                        ProviderConfig.PASSWORD_KEY, "secret",
                        ProviderConfig.SCHEMA_ALLOWLIST_KEY, "APP,,REPORTING")));

        assertEquals(
                "KUBLING_ORACLE_SCHEMA_ALLOWLIST must contain comma-separated schema names",
                exception.getMessage());
    }
}
