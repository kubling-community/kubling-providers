package com.kubling.provider.oracle;

import com.kubling.provider.grpc.ColumnMetadata;
import com.kubling.provider.grpc.KeyKind;
import com.kubling.provider.grpc.KeyMetadata;
import com.kubling.provider.grpc.NamespaceMetadata;
import com.kubling.provider.grpc.SchemaMetadata;
import com.kubling.provider.grpc.TableKind;
import com.kubling.provider.grpc.TableMetadata;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.ValueType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcSchemaDiscovery implements SchemaDiscovery {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JdbcSchemaDiscovery.class);
    private static final String[] RELATION_TYPES = {"TABLE", "VIEW"};
    private static final String MUTABILITY_QUERY = """
            SELECT OWNER, TABLE_NAME, COLUMN_NAME, UPDATABLE, INSERTABLE, DELETABLE
            FROM ALL_UPDATABLE_COLUMNS
            WHERE OWNER = ?
            """;
    private static final Comparator<Relation> RELATION_ORDER = Comparator
            .comparing(Relation::schema)
            .thenComparing(Relation::name)
            .thenComparing(relation -> relation.kind().getNumber());

    private final ConnectionFactory connectionFactory;
    private final SchemaPolicy schemaPolicy;

    JdbcSchemaDiscovery(
            ConnectionFactory connectionFactory,
            SchemaPolicy schemaPolicy) {
        this.connectionFactory = connectionFactory;
        this.schemaPolicy = schemaPolicy;
    }

    @Override
    public SchemaMetadata discover() throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            DatabaseMetaData databaseMetadata = connection.getMetaData();
            List<Relation> relations = readRelations(databaseMetadata);
            relations.sort(RELATION_ORDER);
            Map<RelationIdentity, RelationMutability> mutability =
                    readMutability(connection, relations);

            SchemaMetadata.Builder schema = SchemaMetadata.newBuilder();
            Set<String> exposedNamespaces = new TreeSet<>();
            for (Relation relation : relations) {
                TableMetadata table = readTable(
                        databaseMetadata,
                        relation,
                        mutability.getOrDefault(
                                new RelationIdentity(relation.schema(), relation.name()),
                                RelationMutability.NONE));
                if (table == null) {
                    continue;
                }
                schema.addTables(table);
                exposedNamespaces.add(relation.schema());
            }
            for (String namespace : exposedNamespaces) {
                schema.addNamespaces(NamespaceMetadata.newBuilder().setName(namespace));
            }
            return schema.build();
        }
    }

    private List<Relation> readRelations(DatabaseMetaData databaseMetadata) throws SQLException {
        Map<RelationIdentity, Relation> relations = new LinkedHashMap<>();
        try (ResultSet resultSet = databaseMetadata.getTables(
                null,
                null,
                "%",
                RELATION_TYPES.clone())) {
            while (resultSet.next()) {
                String schema = resultSet.getString("TABLE_SCHEM");
                String name = resultSet.getString("TABLE_NAME");
                TableKind kind = tableKind(resultSet.getString("TABLE_TYPE"));
                if (!schemaPolicy.includes(schema)
                        || !InternalObjects.isExposedRelation(name)
                        || name.isEmpty()
                        || kind == null) {
                    continue;
                }
                Relation relation = new Relation(
                        resultSet.getString("TABLE_CAT"),
                        schema,
                        name,
                        kind,
                        resultSet.getString("REMARKS"));
                relations.putIfAbsent(new RelationIdentity(schema, name), relation);
            }
        }
        return new ArrayList<>(relations.values());
    }

    private static Map<RelationIdentity, RelationMutability> readMutability(
            Connection connection,
            List<Relation> relations) throws SQLException {
        Set<RelationIdentity> exposedRelations = new HashSet<>();
        Set<String> schemas = new TreeSet<>();
        for (Relation relation : relations) {
            exposedRelations.add(new RelationIdentity(relation.schema(), relation.name()));
            schemas.add(relation.schema());
        }
        if (exposedRelations.isEmpty()) {
            return Map.of();
        }

        Map<RelationIdentity, RelationMutability.Builder> builders = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(MUTABILITY_QUERY)) {
            for (String schema : schemas) {
                statement.setString(1, schema);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        RelationIdentity identity = new RelationIdentity(
                                resultSet.getString("OWNER"),
                                resultSet.getString("TABLE_NAME"));
                        String column = resultSet.getString("COLUMN_NAME");
                        if (!exposedRelations.contains(identity)
                                || column == null
                                || column.isEmpty()) {
                            continue;
                        }
                        builders.computeIfAbsent(
                                        identity,
                                        ignored -> new RelationMutability.Builder())
                                .add(
                                        column,
                                        yes(resultSet.getString("UPDATABLE")),
                                        yes(resultSet.getString("INSERTABLE")),
                                        yes(resultSet.getString("DELETABLE")));
                    }
                }
            }
        }

        Map<RelationIdentity, RelationMutability> mutability = new LinkedHashMap<>();
        for (Map.Entry<RelationIdentity, RelationMutability.Builder> entry : builders.entrySet()) {
            mutability.put(entry.getKey(), entry.getValue().build());
        }
        return mutability;
    }

    private TableMetadata readTable(
            DatabaseMetaData databaseMetadata,
            Relation relation,
            RelationMutability mutability)
            throws SQLException {
        List<ColumnSource> sourceColumns = readColumns(databaseMetadata, relation);
        List<ColumnMetadata> columns = new ArrayList<>(sourceColumns.size());
        Set<String> exposedColumnNames = new LinkedHashSet<>();
        int unsupportedColumns = 0;
        for (ColumnSource sourceColumn : sourceColumns) {
            TypeMapper.Mapping mapping = TypeMapper.map(
                            sourceColumn.jdbcType(),
                            sourceColumn.nativeType(),
                            sourceColumn.columnSize(),
                            sourceColumn.decimalDigits())
                    .orElse(null);
            if (mapping == null) {
                unsupportedColumns++;
                continue;
            }
            columns.add(columnMetadata(
                    sourceColumn,
                    mapping,
                    mutability.column(sourceColumn.name()).input()));
            exposedColumnNames.add(sourceColumn.name());
        }

        if (unsupportedColumns > 0) {
            LOGGER.debug(
                    "Skipped {} unsupported columns while discovering an Oracle relation",
                    unsupportedColumns);
        }
        if (columns.isEmpty()) {
            return null;
        }

        TableMetadata.Builder table = TableMetadata.newBuilder()
                .setName(relation.name())
                .setNamespace(relation.schema())
                .setKind(relation.kind())
                .setUpdatable(mutability.writable())
                .addAllColumns(columns);
        if (relation.kind() == TableKind.TABLE_KIND_TABLE) {
            table.addAllKeys(readKeys(databaseMetadata, relation, exposedColumnNames));
        }
        if (relation.remarks() != null && !relation.remarks().isBlank()) {
            table.setAnnotation(relation.remarks());
        }
        return table.build();
    }

    private List<ColumnSource> readColumns(
            DatabaseMetaData databaseMetadata,
            Relation relation) throws SQLException {
        String escape = databaseMetadata.getSearchStringEscape();
        String schemaPattern = escapeMetadataPattern(relation.schema(), escape);
        String tablePattern = escapeMetadataPattern(relation.name(), escape);
        List<ColumnSource> columns = new ArrayList<>();
        try (ResultSet resultSet = databaseMetadata.getColumns(
                relation.catalog(),
                schemaPattern,
                tablePattern,
                "%")) {
            while (resultSet.next()) {
                if (!matchesIfPresent(resultSet, "TABLE_SCHEM", relation.schema())
                        || !matchesIfPresent(resultSet, "TABLE_NAME", relation.name())) {
                    continue;
                }
                String name = resultSet.getString("COLUMN_NAME");
                if (name == null || name.isEmpty()) {
                    continue;
                }
                columns.add(new ColumnSource(
                        name,
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getString("TYPE_NAME"),
                        nullableInteger(resultSet, "COLUMN_SIZE"),
                        nullableInteger(resultSet, "DECIMAL_DIGITS"),
                        nullableInteger(resultSet, "NULLABLE"),
                        resultSet.getString("IS_AUTOINCREMENT"),
                        nullableInteger(resultSet, "ORDINAL_POSITION"),
                        resultSet.getString("REMARKS")));
            }
        }
        columns.sort(Comparator
                .comparing(ColumnSource::ordinalPosition, Comparator.nullsLast(Integer::compareTo))
                .thenComparing(ColumnSource::name));
        return columns;
    }

    private static ColumnMetadata columnMetadata(
            ColumnSource source,
            TypeMapper.Mapping mapping,
            boolean updatable) {
        ColumnMetadata.Builder column = ColumnMetadata.newBuilder()
                .setName(source.name())
                .setType(mapping.type())
                .setNativeType(source.nativeType() == null ? "unknown" : source.nativeType())
                .setUpdatable(updatable)
                .setSearchability(mapping.searchability())
                .setTypeDescriptor(typeDescriptor(mapping));

        if (source.nullable() != null) {
            if (source.nullable() == DatabaseMetaData.columnNullable) {
                column.setNullable(true);
            } else if (source.nullable() == DatabaseMetaData.columnNoNulls) {
                column.setNullable(false);
            }
        }
        if ("YES".equalsIgnoreCase(source.autoIncremented())) {
            column.setAutoIncremented(true);
        } else if ("NO".equalsIgnoreCase(source.autoIncremented())) {
            column.setAutoIncremented(false);
        }
        if (mapping.length() != null) {
            column.setLength(mapping.length());
        }
        if (mapping.precision() != null) {
            column.setPrecision(mapping.precision());
        }
        if (mapping.scale() != null) {
            column.setScale(mapping.scale());
        }
        if (source.remarks() != null && !source.remarks().isBlank()) {
            column.setAnnotation(source.remarks());
        }
        return column.build();
    }

    private static TypeDescriptor typeDescriptor(TypeMapper.Mapping mapping) {
        TypeDescriptor.Builder descriptor = TypeDescriptor.newBuilder().setType(mapping.type());
        if ((mapping.type() == ValueType.VALUE_TYPE_BIGINTEGER
                        || mapping.type() == ValueType.VALUE_TYPE_BIGDECIMAL)
                && mapping.precision() != null) {
            descriptor.setPrecision(mapping.precision());
        }
        if (mapping.type() == ValueType.VALUE_TYPE_BIGDECIMAL
                && mapping.precision() != null
                && mapping.scale() != null
                && mapping.scale() <= mapping.precision()) {
            descriptor.setScale(mapping.scale());
        }
        return descriptor.build();
    }

    private static List<KeyMetadata> readKeys(
            DatabaseMetaData databaseMetadata,
            Relation relation,
            Set<String> exposedColumnNames) throws SQLException {
        List<KeyMetadata> primaryKeys = readPrimaryKeys(
                databaseMetadata,
                relation,
                exposedColumnNames);
        Set<List<String>> primaryColumnSets = new HashSet<>();
        for (KeyMetadata key : primaryKeys) {
            primaryColumnSets.add(List.copyOf(key.getColumnsList()));
        }

        List<KeyMetadata> keys = new ArrayList<>(primaryKeys);
        keys.addAll(readUniqueIndexes(
                databaseMetadata,
                relation,
                exposedColumnNames,
                primaryColumnSets));
        keys.sort(Comparator
                .comparing((KeyMetadata key) -> key.getKind().getNumber())
                .thenComparing(KeyMetadata::getName));
        return keys;
    }

    private static List<KeyMetadata> readPrimaryKeys(
            DatabaseMetaData databaseMetadata,
            Relation relation,
            Set<String> exposedColumnNames) throws SQLException {
        Map<String, OrderedKeyColumns> grouped = new LinkedHashMap<>();
        try (ResultSet resultSet = databaseMetadata.getPrimaryKeys(
                relation.catalog(),
                relation.schema(),
                relation.name())) {
            while (resultSet.next()) {
                String keyName = resultSet.getString("PK_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                if (keyName == null || keyName.isEmpty() || columnName == null) {
                    continue;
                }
                grouped.computeIfAbsent(keyName, ignored -> new OrderedKeyColumns())
                        .add(nullableInteger(resultSet, "KEY_SEQ"), columnName);
            }
        }
        return buildKeys(grouped, KeyKind.KEY_KIND_PRIMARY, exposedColumnNames, Set.of());
    }

    private static List<KeyMetadata> readUniqueIndexes(
            DatabaseMetaData databaseMetadata,
            Relation relation,
            Set<String> exposedColumnNames,
            Set<List<String>> excludedColumnSets) throws SQLException {
        Map<String, OrderedKeyColumns> grouped = new LinkedHashMap<>();
        try (ResultSet resultSet = databaseMetadata.getIndexInfo(
                relation.catalog(),
                relation.schema(),
                quoteIndexMetadataIdentifier(relation.name()),
                true,
                true)) {
            while (resultSet.next()) {
                int indexType = resultSet.getInt("TYPE");
                if (indexType == DatabaseMetaData.tableIndexStatistic
                        || resultSet.getBoolean("NON_UNIQUE")) {
                    continue;
                }
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                if (indexName == null || indexName.isEmpty()) {
                    continue;
                }
                OrderedKeyColumns columns = grouped.computeIfAbsent(
                        indexName,
                        ignored -> new OrderedKeyColumns());
                if (columnName == null || columnName.isEmpty()) {
                    columns.invalidate();
                } else {
                    columns.add(nullableInteger(resultSet, "ORDINAL_POSITION"), columnName);
                }
            }
        }
        return buildKeys(
                grouped,
                KeyKind.KEY_KIND_UNIQUE,
                exposedColumnNames,
                excludedColumnSets);
    }

    static String quoteIndexMetadataIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static List<KeyMetadata> buildKeys(
            Map<String, OrderedKeyColumns> grouped,
            KeyKind kind,
            Set<String> exposedColumnNames,
            Set<List<String>> excludedColumnSets) {
        List<KeyMetadata> keys = new ArrayList<>();
        for (Map.Entry<String, OrderedKeyColumns> entry : grouped.entrySet()) {
            List<String> columns = entry.getValue().orderedColumns();
            if (columns.isEmpty()
                    || !entry.getValue().valid()
                    || !exposedColumnNames.containsAll(columns)
                    || excludedColumnSets.contains(columns)) {
                continue;
            }
            keys.add(KeyMetadata.newBuilder()
                    .setName(entry.getKey())
                    .setKind(kind)
                    .addAllColumns(columns)
                    .build());
        }
        return keys;
    }

    static String escapeMetadataPattern(String identifier, String escape) {
        Objects.requireNonNull(identifier, "identifier");
        if (escape == null || escape.isEmpty() || escape.equals("%") || escape.equals("_")) {
            return identifier;
        }
        return identifier
                .replace(escape, escape + escape)
                .replace("%", escape + "%")
                .replace("_", escape + "_");
    }

    private static boolean matchesIfPresent(
            ResultSet resultSet,
            String column,
            String expected) throws SQLException {
        String actual = resultSet.getString(column);
        return actual == null || actual.equals(expected);
    }

    private static Integer nullableInteger(ResultSet resultSet, String column)
            throws SQLException {
        int value = resultSet.getInt(column);
        return resultSet.wasNull() ? null : value;
    }

    private static boolean yes(String value) {
        return "YES".equalsIgnoreCase(value);
    }

    private static TableKind tableKind(String tableType) {
        if ("TABLE".equalsIgnoreCase(tableType)) {
            return TableKind.TABLE_KIND_TABLE;
        }
        if ("VIEW".equalsIgnoreCase(tableType)) {
            return TableKind.TABLE_KIND_VIEW;
        }
        return null;
    }

    private record RelationIdentity(String schema, String name) {}

    private record Relation(
            String catalog,
            String schema,
            String name,
            TableKind kind,
            String remarks) {}

    private record ColumnSource(
            String name,
            int jdbcType,
            String nativeType,
            Integer columnSize,
            Integer decimalDigits,
            Integer nullable,
            String autoIncremented,
            Integer ordinalPosition,
            String remarks) {}

    private record ColumnMutability(
            boolean updatable,
            boolean insertable,
            boolean deletable) {
        private static final ColumnMutability NONE =
                new ColumnMutability(false, false, false);

        boolean input() {
            return updatable || insertable;
        }

        boolean writable() {
            return input() || deletable;
        }

        ColumnMutability merge(ColumnMutability other) {
            return new ColumnMutability(
                    updatable || other.updatable,
                    insertable || other.insertable,
                    deletable || other.deletable);
        }
    }

    private record RelationMutability(Map<String, ColumnMutability> columns) {
        private static final RelationMutability NONE = new RelationMutability(Map.of());

        RelationMutability {
            columns = Map.copyOf(columns);
        }

        ColumnMutability column(String name) {
            return columns.getOrDefault(name, ColumnMutability.NONE);
        }

        boolean writable() {
            return columns.values().stream().anyMatch(ColumnMutability::writable);
        }

        private static final class Builder {
            private final Map<String, ColumnMutability> columns = new LinkedHashMap<>();

            void add(
                    String column,
                    boolean updatable,
                    boolean insertable,
                    boolean deletable) {
                columns.merge(
                        column,
                        new ColumnMutability(updatable, insertable, deletable),
                        ColumnMutability::merge);
            }

            RelationMutability build() {
                return new RelationMutability(columns);
            }
        }
    }

    private static final class OrderedKeyColumns {
        private final List<OrderedColumn> columns = new ArrayList<>();
        private boolean valid = true;

        void add(Integer position, String column) {
            columns.add(new OrderedColumn(position, column));
        }

        void invalidate() {
            valid = false;
        }

        boolean valid() {
            return valid;
        }

        List<String> orderedColumns() {
            columns.sort(Comparator
                    .comparing(OrderedColumn::position, Comparator.nullsLast(Integer::compareTo))
                    .thenComparing(OrderedColumn::name));
            return columns.stream().map(OrderedColumn::name).toList();
        }
    }

    private record OrderedColumn(Integer position, String name) {}
}
