# Oracle provider

This provider connects Kubling to Oracle Database through Oracle's JDBC driver.
It discovers accessible tables and views as structured Kubling metadata, with
Oracle owners represented as namespaces. Read queries support bound filters,
ordering, pagination, grouping, HAVING and standard aggregate pushdown.
Insert, update and delete operations use bound JDBC parameters and can
participate in a native Oracle transaction attached to the logical provider
connection. Durable XA prepare and recovery can be enabled for deployments that
need prepared branches to survive provider restarts.

## Configuration

Set the database connection before starting the provider:

| Variable | Purpose | Default |
| --- | --- | --- |
| `KUBLING_ORACLE_JDBC_URL` | Oracle JDBC URL | required |
| `KUBLING_ORACLE_USERNAME` | Database user | optional |
| `KUBLING_ORACLE_USERNAME_FILE` | File containing the database user | optional |
| `KUBLING_ORACLE_PASSWORD` | Database password | optional |
| `KUBLING_ORACLE_PASSWORD_FILE` | File containing the database password | optional |
| `KUBLING_ORACLE_JDBC_PROPERTIES_FILE` | Oracle JDBC properties file | optional |
| `KUBLING_ORACLE_TNS_ADMIN` | Directory containing Oracle network configuration | optional |
| `KUBLING_ORACLE_SCHEMA_ALLOWLIST` | Exact, comma-separated owners to expose | optional |
| `KUBLING_ORACLE_SCHEMA_DENYLIST` | Exact, comma-separated owners to exclude | optional |
| `KUBLING_ORACLE_LISTEN` | gRPC listen address | `0.0.0.0:50051` |
| `KUBLING_ORACLE_CONNECT_TIMEOUT_SECONDS` | New database connection timeout | `15` |
| `KUBLING_ORACLE_HEALTH_TIMEOUT_SECONDS` | Database health-check timeout | `5` |
| `KUBLING_ORACLE_SHUTDOWN_GRACE_SECONDS` | Graceful shutdown timeout | `10` |
| `KUBLING_ORACLE_LOG_LEVEL` | Log level | `INFO` |
| `KUBLING_ORACLE_RESOURCE_MANAGER_ID` | Stable identity that enables durable XA recovery | optional |
| `KUBLING_ORACLE_POOL_MIN_SIZE` | Minimum open database connections | `0` |
| `KUBLING_ORACLE_POOL_MAX_SIZE` | Maximum open database connections | `20` |
| `KUBLING_ORACLE_POOL_CONNECTION_WAIT_SECONDS` | Maximum wait for a pooled connection | `30` |
| `KUBLING_ORACLE_POOL_INACTIVE_TIMEOUT_SECONDS` | Idle connection retirement time; zero disables it | `300` |
| `KUBLING_ORACLE_POOL_MAX_REUSE_SECONDS` | Maximum physical connection reuse time; zero disables it | `3600` |

Configure either the direct value or the corresponding `_FILE` variable for
each credential, never both. Username and password must be supplied together.
If both are omitted, Oracle JDBC obtains authentication material from the URL,
the configured properties or Oracle network configuration. This supports
driver-managed authentication such as wallets, token files and Kerberos without
placing credentials in provider configuration. Secret files are preferable when
the runtime can mount them without exposing their contents in the environment.

The provider uses one Oracle Universal Connection Pool. When durable recovery
is enabled, the same pool supplies regular and XA connections; its resource
manager lease reserves one connection for the lifetime of the process. The
maximum pool size must therefore be at least three in that mode.

Common Oracle-maintained schemas are excluded by default. An explicit allowlist
may include one deliberately; the configured denylist takes precedence. Owner,
table, view, column and key names are preserved exactly, including quoted
mixed-case or special-character names. Mutation eligibility is discovered from
Oracle's data dictionary for the authenticated database user.
Columns using unsupported Oracle object, collection or time-zone-aware timestamp
types are omitted instead of being exposed through a lossy coercion.
Binary and character LOBs are currently returned inline and are limited to 16
MiB per value.

Insert operations do not return generated values. Update assignments accept
literal values or another field from the same row. An update or delete without
a filter follows normal SQL semantics and affects every row.

## Durable transaction recovery

Recovery is disabled unless `KUBLING_ORACLE_RESOURCE_MANAGER_ID` is set. The
value identifies one Oracle resource manager and must stay unchanged across
provider restarts. Failover instances use the same value, database and journal;
an Oracle row lock prevents more than one instance from owning it at a time.

Before enabling it, create the recovery tables as the provider database user with
[`schema/transaction-recovery.sql`](schema/transaction-recovery.sql). A database
administrator must also grant that user the Oracle privileges needed to scan
and resolve pending XA branches:

```sql
GRANT SELECT ON SYS.DBA_PENDING_TRANSACTIONS TO provider_user;
GRANT EXECUTE ON SYS.DBMS_XA TO provider_user;
```

The provider validates the journal and XA recovery access at startup. It does
not create or migrate database objects automatically.

## Container image

The provider is distributed as `docker.io/kubling/oracle-provider`. Expose its
gRPC port and supply the configuration above through the container runtime.
Mount wallets, credential files and Oracle network configuration read-only.

## Development

Build and run the Java distribution with:

```sh
mvn clean package
java -jar target/kubling-oracle-provider.jar
```

The local fixture in [`local`](local/) starts the provider with Oracle Database
Free.

The provider source is licensed under Apache License 2.0. The container also
contains Oracle's JDBC driver; see [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
