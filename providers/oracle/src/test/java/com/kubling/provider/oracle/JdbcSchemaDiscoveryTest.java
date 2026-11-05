package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.ColumnMetadata;
import com.kubling.provider.grpc.ColumnSearchability;
import com.kubling.provider.grpc.KeyKind;
import com.kubling.provider.grpc.SchemaMetadata;
import com.kubling.provider.grpc.TableKind;
import com.kubling.provider.grpc.TableMetadata;
import com.kubling.transport.grpc.ValueType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.junit.jupiter.api.Test;

final class JdbcSchemaDiscoveryTest {
    @Test
    void discoversExactOracleNamesTypesAndKeys() throws Exception {
        List<String> requestedPatterns = new ArrayList<>();
        List<String> requestedMutabilitySchemas = new ArrayList<>();
        DatabaseMetaData databaseMetadata = databaseMetadata(requestedPatterns);
        Connection connection = proxy(Connection.class, (ignored, method, arguments) ->
                switch (method.getName()) {
                    case "getMetaData" -> databaseMetadata;
                    case "prepareStatement" -> mutabilityStatement(
                            (String) arguments[0],
                            requestedMutabilitySchemas);
                    case "close" -> null;
                    case "isClosed" -> false;
                    default -> throw unsupported(method.getName());
                });

        JdbcSchemaDiscovery discovery = new JdbcSchemaDiscovery(
                () -> connection,
                new SchemaPolicy(Set.of(), Set.of()));

        SchemaMetadata schema = discovery.discover();

        assertEquals(2, schema.getNamespacesCount());
        assertEquals("APP", schema.getNamespaces(0).getName());
        assertEquals("Mixed_Schema%", schema.getNamespaces(1).getName());
        assertEquals(2, schema.getTablesCount());
        assertEquals(
                List.of("APP/Order", "Mixed_Schema%/Telemetry_%"),
                schema.getTablesList().stream()
                        .map(table -> table.getNamespace() + "/" + table.getName())
                        .toList());
        assertEquals(
                List.of("APP/Order", "Mixed\\_Schema\\%/Telemetry\\_\\%"),
                requestedPatterns);
        assertEquals(List.of("APP", "Mixed_Schema%"), requestedMutabilitySchemas);

        TableMetadata order = schema.getTables(0);
        assertEquals(TableKind.TABLE_KIND_TABLE, order.getKind());
        assertTrue(order.getUpdatable());
        assertEquals("Application orders", order.getAnnotation());
        assertEquals(4, order.getColumnsCount());
        assertEquals(
                List.of("select", "Amount", "Payload", "Created At"),
                order.getColumnsList().stream().map(ColumnMetadata::getName).toList());

        ColumnMetadata identifier = order.getColumns(0);
        assertEquals(ValueType.VALUE_TYPE_BIGINTEGER, identifier.getType());
        assertEquals("NUMBER", identifier.getNativeType());
        assertEquals(12, identifier.getPrecision());
        assertEquals(0, identifier.getScale());
        assertTrue(identifier.hasNullable());
        assertFalse(identifier.getNullable());
        assertTrue(identifier.hasAutoIncremented());
        assertFalse(identifier.getAutoIncremented());
        assertTrue(identifier.getUpdatable());
        assertEquals(12, identifier.getTypeDescriptor().getPrecision());
        assertFalse(identifier.getTypeDescriptor().hasScale());

        ColumnMetadata amount = order.getColumns(1);
        assertEquals(ValueType.VALUE_TYPE_BIGDECIMAL, amount.getType());
        assertEquals(18, amount.getPrecision());
        assertEquals(2, amount.getScale());
        assertTrue(amount.getNullable());
        assertEquals(18, amount.getTypeDescriptor().getPrecision());
        assertEquals(2, amount.getTypeDescriptor().getScale());
        assertTrue(amount.getUpdatable());

        ColumnMetadata payload = order.getColumns(2);
        assertEquals(ValueType.VALUE_TYPE_BLOB, payload.getType());
        assertFalse(payload.getUpdatable());
        assertEquals(
                ColumnSearchability.COLUMN_SEARCHABILITY_UNSEARCHABLE,
                payload.getSearchability());
        assertEquals(ValueType.VALUE_TYPE_TIMESTAMP, order.getColumns(3).getType());
        assertFalse(order.getColumns(3).getUpdatable());

        assertEquals(2, order.getKeysCount());
        assertEquals("PK_Order", order.getKeys(0).getName());
        assertEquals(KeyKind.KEY_KIND_PRIMARY, order.getKeys(0).getKind());
        assertEquals(List.of("select"), order.getKeys(0).getColumnsList());
        assertEquals("UQ_Order_Amount", order.getKeys(1).getName());
        assertEquals(KeyKind.KEY_KIND_UNIQUE, order.getKeys(1).getKind());
        assertEquals(List.of("Amount"), order.getKeys(1).getColumnsList());

        TableMetadata telemetry = schema.getTables(1);
        assertEquals(TableKind.TABLE_KIND_VIEW, telemetry.getKind());
        assertFalse(telemetry.getUpdatable());
        assertEquals("value_%", telemetry.getColumns(0).getName());
        assertEquals(64, telemetry.getColumns(0).getLength());
        assertFalse(telemetry.getColumns(0).getUpdatable());
    }

    @Test
    void escapesMetadataPatternsWithoutChangingLogicalNames() {
        assertEquals(
                "Mixed\\_Schema\\%",
                JdbcSchemaDiscovery.escapeMetadataPattern("Mixed_Schema%", "\\"));
        assertEquals(
                "A\\\\B\\_C\\%",
                JdbcSchemaDiscovery.escapeMetadataPattern("A\\B_C%", "\\"));
        assertEquals(
                "\"A\"\"B\"",
                JdbcSchemaDiscovery.quoteIndexMetadataIdentifier("A\"B"));
    }

    private static DatabaseMetaData databaseMetadata(List<String> requestedPatterns) {
        return proxy(DatabaseMetaData.class, (ignored, method, arguments) ->
                switch (method.getName()) {
                    case "getSearchStringEscape" -> "\\";
                    case "getTables" -> relations();
                    case "getColumns" -> columns(arguments, requestedPatterns);
                    case "getPrimaryKeys" -> primaryKeys((String) arguments[2]);
                    case "getIndexInfo" -> uniqueIndexes(arguments);
                    default -> throw unsupported(method.getName());
                });
    }

    private static ResultSet columns(Object[] arguments, List<String> requestedPatterns)
            throws SQLException {
        String schemaPattern = (String) arguments[1];
        String tablePattern = (String) arguments[2];
        requestedPatterns.add(schemaPattern + "/" + tablePattern);
        if (schemaPattern.equals("APP") && tablePattern.equals("Order")) {
            return orderColumns();
        }
        if (schemaPattern.equals("Mixed\\_Schema\\%")
                && tablePattern.equals("Telemetry\\_\\%")) {
            return telemetryColumns();
        }
        throw new AssertionError(
                "unexpected metadata pattern " + schemaPattern + "/" + tablePattern);
    }

    private static UnsupportedOperationException unsupported(String method) {
        return new UnsupportedOperationException("unexpected JDBC call: " + method);
    }

    private static PreparedStatement mutabilityStatement(
            String sql,
            List<String> requestedSchemas) {
        assertTrue(sql.contains("ALL_UPDATABLE_COLUMNS"));
        String[] schema = new String[1];
        return proxy(PreparedStatement.class, (ignored, method, arguments) ->
                switch (method.getName()) {
                    case "setString" -> {
                        assertEquals(1, arguments[0]);
                        schema[0] = (String) arguments[1];
                        yield null;
                    }
                    case "executeQuery" -> {
                        requestedSchemas.add(schema[0]);
                        yield mutabilityRows(schema[0]);
                    }
                    case "close" -> null;
                    case "isClosed" -> false;
                    default -> throw unsupported(method.getName());
                });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler invocationHandler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                invocationHandler));
    }

    private static ResultSet relations() throws SQLException {
        return rowSet(
                new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"},
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR},
                new Object[] {null, "SYS", "SECRET", "TABLE", null},
                new Object[] {
                    null,
                    "APP",
                    JdbcTransactionJournal.TABLE_NAME,
                    "TABLE",
                    null
                },
                new Object[] {null, "Mixed_Schema%", "Telemetry_%", "VIEW", null},
                new Object[] {null, "APP", "Order", "TABLE", "Application orders"});
    }

    private static ResultSet orderColumns() throws SQLException {
        return rowSet(
                columnLabels(),
                columnTypes(),
                column("APP", "Order", "Shape", Types.STRUCT, "MDSYS.SDO_GEOMETRY", null, null, 1, "NO", 5, null),
                column("APP", "Order", "Created At", Types.TIMESTAMP, "TIMESTAMP(6)", null, 6, 1, "NO", 4, null),
                column("APP", "Order", "Payload", Types.BLOB, "BLOB", null, null, 1, "NO", 3, null),
                column("APP", "Order", "Amount", Types.NUMERIC, "NUMBER", 18, 2, 1, "NO", 2, null),
                column("APP", "Order", "select", Types.NUMERIC, "NUMBER", 12, 0, 0, "NO", 1, "Reserved identifier"));
    }

    private static ResultSet telemetryColumns() throws SQLException {
        return rowSet(
                columnLabels(),
                columnTypes(),
                column(
                        "Mixed_Schema%",
                        "Telemetry_%",
                        "value_%",
                        Types.VARCHAR,
                        "VARCHAR2",
                        64,
                        null,
                        1,
                        "NO",
                        1,
                        null));
    }

    private static ResultSet mutabilityRows(String schema) throws SQLException {
        String[] labels = {
            "OWNER", "TABLE_NAME", "COLUMN_NAME", "UPDATABLE", "INSERTABLE", "DELETABLE"
        };
        int[] types = {
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR
        };
        if (schema.equals("APP")) {
            return rowSet(
                    labels,
                    types,
                    new Object[] {"APP", "Order", "select", "YES", "YES", "YES"},
                    new Object[] {"APP", "Order", "Amount", "NO", "YES", "YES"},
                    new Object[] {"APP", "Order", "Payload", "NO", "NO", "YES"},
                    new Object[] {"APP", "Order", "Created At", "NO", "NO", "NO"},
                    new Object[] {"APP", "NOT_EXPOSED", "value", "YES", "YES", "YES"});
        }
        if (schema.equals("Mixed_Schema%")) {
            return rowSet(
                    labels,
                    types,
                    new Object[] {
                        "Mixed_Schema%", "Telemetry_%", "value_%", "NO", "NO", "NO"
                    });
        }
        throw new AssertionError("unexpected mutability schema " + schema);
    }

    private static ResultSet primaryKeys(String table) throws SQLException {
        if (!table.equals("Order")) {
            return emptyPrimaryKeys();
        }
        return rowSet(
                new String[] {"PK_NAME", "COLUMN_NAME", "KEY_SEQ"},
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.SMALLINT},
                new Object[] {"PK_Order", "select", 1});
    }

    private static ResultSet uniqueIndexes(Object[] arguments) throws SQLException {
        assertTrue((Boolean) arguments[3]);
        assertTrue((Boolean) arguments[4]);
        String table = (String) arguments[2];
        if (!table.equals("\"Order\"")) {
            return emptyUniqueIndexes();
        }
        return rowSet(
                new String[] {"TYPE", "NON_UNIQUE", "INDEX_NAME", "COLUMN_NAME", "ORDINAL_POSITION"},
                new int[] {Types.SMALLINT, Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT},
                new Object[] {DatabaseMetaData.tableIndexOther, false, "FUNCTION_INDEX", null, 1},
                new Object[] {DatabaseMetaData.tableIndexOther, false, "PK_Order_INDEX", "select", 1},
                new Object[] {DatabaseMetaData.tableIndexOther, false, "UQ_Order_Amount", "Amount", 1});
    }

    private static ResultSet emptyPrimaryKeys() throws SQLException {
        return rowSet(
                new String[] {"PK_NAME", "COLUMN_NAME", "KEY_SEQ"},
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.SMALLINT});
    }

    private static ResultSet emptyUniqueIndexes() throws SQLException {
        return rowSet(
                new String[] {"TYPE", "NON_UNIQUE", "INDEX_NAME", "COLUMN_NAME", "ORDINAL_POSITION"},
                new int[] {Types.SMALLINT, Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT});
    }

    private static String[] columnLabels() {
        return new String[] {
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "TYPE_NAME",
            "COLUMN_SIZE",
            "DECIMAL_DIGITS",
            "NULLABLE",
            "IS_AUTOINCREMENT",
            "ORDINAL_POSITION",
            "REMARKS"
        };
    }

    private static int[] columnTypes() {
        return new int[] {
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.INTEGER,
            Types.VARCHAR,
            Types.INTEGER,
            Types.INTEGER,
            Types.INTEGER,
            Types.VARCHAR,
            Types.INTEGER,
            Types.VARCHAR
        };
    }

    private static Object[] column(
            String schema,
            String table,
            String name,
            int dataType,
            String nativeType,
            Integer size,
            Integer scale,
            int nullable,
            String autoIncremented,
            int ordinal,
            String remarks) {
        return new Object[] {
            schema,
            table,
            name,
            dataType,
            nativeType,
            size,
            scale,
            nullable,
            autoIncremented,
            ordinal,
            remarks
        };
    }

    private static ResultSet rowSet(
            String[] labels,
            int[] types,
            Object[]... rows) throws SQLException {
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(labels.length);
        for (int index = 0; index < labels.length; index++) {
            metadata.setColumnName(index + 1, labels[index]);
            metadata.setColumnLabel(index + 1, labels[index]);
            metadata.setColumnType(index + 1, types[index]);
        }

        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        rowSet.setMetaData(metadata);
        for (Object[] row : rows) {
            rowSet.moveToInsertRow();
            for (int index = 0; index < row.length; index++) {
                if (row[index] == null) {
                    rowSet.updateNull(index + 1);
                } else {
                    rowSet.updateObject(index + 1, row[index]);
                }
            }
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
        }
        rowSet.beforeFirst();
        return rowSet;
    }
}
