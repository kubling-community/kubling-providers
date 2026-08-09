package cassandra

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func schemaMetadata(
	namespace string,
	keyspace *gocql.KeyspaceMetadata,
) *providerv1.SchemaMetadata {
	tableNames := make([]string, 0, len(keyspace.Tables))
	for tableName := range keyspace.Tables {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	metadata := &providerv1.SchemaMetadata{
		Tables: make([]*providerv1.TableMetadata, 0, len(tableNames)),
		Namespaces: []*providerv1.NamespaceMetadata{{
			Name: namespace,
			Properties: map[string]string{
				"cassandra.keyspace":             keyspace.Name,
				"cassandra.replication_strategy": keyspace.StrategyClass,
			},
		}},
		Properties: map[string]string{
			"cassandra.keyspace":             keyspace.Name,
			"cassandra.replication_strategy": keyspace.StrategyClass,
		},
	}
	for _, tableName := range tableNames {
		metadata.Tables = append(
			metadata.Tables,
			tableMetadata(namespace, keyspace.Name, keyspace.Tables[tableName]),
		)
	}

	return metadata
}

func tableMetadata(
	namespace string,
	keyspace string,
	table *gocql.TableMetadata,
) *providerv1.TableMetadata {
	updatable := true
	metadata := &providerv1.TableMetadata{
		Name:       table.Name,
		SourceName: table.Name,
		Namespace:  namespace,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Properties: map[string]string{
			"cassandra.keyspace":       keyspace,
			"cassandra.partition_key":  joinColumnNames(table.PartitionKey),
			"cassandra.clustering_key": joinColumnNames(table.ClusteringColumns),
		},
	}

	for _, columnName := range orderedColumnNames(table) {
		column := table.Columns[columnName]
		if column == nil {
			continue
		}
		metadata.Columns = append(metadata.Columns, columnMetadata(column))
	}

	primaryKeyColumns := append(
		columnNames(table.PartitionKey),
		columnNames(table.ClusteringColumns)...,
	)
	if len(primaryKeyColumns) > 0 {
		metadata.Keys = append(metadata.Keys, &providerv1.KeyMetadata{
			Name:       "PK_" + table.Name,
			Kind:       providerv1.KeyKind_KEY_KIND_PRIMARY,
			Columns:    primaryKeyColumns,
			Properties: map[string]string{"cassandra.composite": "true"},
		})
	}

	return metadata
}

func columnMetadata(column *gocql.ColumnMetadata) *providerv1.ColumnMetadata {
	nullable := column.Kind != gocql.ColumnPartitionKey &&
		column.Kind != gocql.ColumnClusteringKey
	updatable := true
	properties := map[string]string{
		"cassandra.kind": column.Kind.String(),
	}
	if column.Kind == gocql.ColumnClusteringKey {
		if column.Order == gocql.DESC {
			properties["cassandra.clustering_order"] = "DESC"
		} else {
			properties["cassandra.clustering_order"] = "ASC"
		}
	}
	if column.Type != nil && column.Type.Type() == gocql.TypeCounter {
		properties["cassandra.counter"] = "true"
	}
	if column.Index.Name != "" {
		properties["cassandra.index_name"] = column.Index.Name
		properties["cassandra.index_type"] = column.Index.Type
	}

	return &providerv1.ColumnMetadata{
		Name:          column.Name,
		SourceName:    column.Name,
		Type:          logicalValueType(column.Type),
		NativeType:    nativeTypeName(column.Type),
		Nullable:      &nullable,
		Updatable:     &updatable,
		Searchability: columnSearchability(column),
		Properties:    properties,
	}
}

func logicalValueType(typeInfo gocql.TypeInfo) kublingv1.ValueType {
	if typeInfo == nil {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN
	}

	switch typeInfo.Type() {
	case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar,
		gocql.TypeUUID, gocql.TypeTimeUUID, gocql.TypeInet,
		gocql.TypeDuration:
		return kublingv1.ValueType_VALUE_TYPE_STRING
	case gocql.TypeBigInt, gocql.TypeCounter:
		return kublingv1.ValueType_VALUE_TYPE_LONG
	case gocql.TypeBlob:
		return kublingv1.ValueType_VALUE_TYPE_BLOB
	case gocql.TypeBoolean:
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN
	case gocql.TypeDecimal:
		return kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL
	case gocql.TypeDouble:
		return kublingv1.ValueType_VALUE_TYPE_DOUBLE
	case gocql.TypeFloat:
		return kublingv1.ValueType_VALUE_TYPE_FLOAT
	case gocql.TypeInt:
		return kublingv1.ValueType_VALUE_TYPE_INTEGER
	case gocql.TypeVarint:
		return kublingv1.ValueType_VALUE_TYPE_BIGINTEGER
	case gocql.TypeSmallInt:
		return kublingv1.ValueType_VALUE_TYPE_SHORT
	case gocql.TypeTinyInt:
		return kublingv1.ValueType_VALUE_TYPE_BYTE
	case gocql.TypeTimestamp:
		return kublingv1.ValueType_VALUE_TYPE_TIMESTAMP
	case gocql.TypeDate:
		return kublingv1.ValueType_VALUE_TYPE_DATE
	case gocql.TypeTime:
		return kublingv1.ValueType_VALUE_TYPE_TIME
	case gocql.TypeList, gocql.TypeMap, gocql.TypeSet,
		gocql.TypeUDT, gocql.TypeTuple, gocql.TypeCustom:
		return kublingv1.ValueType_VALUE_TYPE_JSON
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN
	}
}

func nativeTypeName(typeInfo gocql.TypeInfo) string {
	if typeInfo == nil {
		return "unknown"
	}

	switch typed := typeInfo.(type) {
	case gocql.CollectionType:
		switch typed.Type() {
		case gocql.TypeMap:
			return fmt.Sprintf(
				"map<%s,%s>",
				nativeTypeName(typed.Key),
				nativeTypeName(typed.Elem),
			)
		case gocql.TypeList:
			return fmt.Sprintf("list<%s>", nativeTypeName(typed.Elem))
		case gocql.TypeSet:
			return fmt.Sprintf("set<%s>", nativeTypeName(typed.Elem))
		}
	case gocql.TupleTypeInfo:
		elements := make([]string, 0, len(typed.Elems))
		for _, element := range typed.Elems {
			elements = append(elements, nativeTypeName(element))
		}
		return "tuple<" + strings.Join(elements, ",") + ">"
	case gocql.UDTTypeInfo:
		if typed.Keyspace == "" {
			return typed.Name
		}
		return typed.Keyspace + "." + typed.Name
	case fmt.Stringer:
		return typed.String()
	}

	return simpleNativeTypeName(typeInfo.Type())
}

func simpleNativeTypeName(valueType gocql.Type) string {
	names := map[gocql.Type]string{
		gocql.TypeCustom:    "custom",
		gocql.TypeAscii:     "ascii",
		gocql.TypeBigInt:    "bigint",
		gocql.TypeBlob:      "blob",
		gocql.TypeBoolean:   "boolean",
		gocql.TypeCounter:   "counter",
		gocql.TypeDecimal:   "decimal",
		gocql.TypeDouble:    "double",
		gocql.TypeFloat:     "float",
		gocql.TypeInt:       "int",
		gocql.TypeText:      "text",
		gocql.TypeTimestamp: "timestamp",
		gocql.TypeUUID:      "uuid",
		gocql.TypeVarchar:   "varchar",
		gocql.TypeVarint:    "varint",
		gocql.TypeTimeUUID:  "timeuuid",
		gocql.TypeInet:      "inet",
		gocql.TypeDate:      "date",
		gocql.TypeTime:      "time",
		gocql.TypeSmallInt:  "smallint",
		gocql.TypeTinyInt:   "tinyint",
		gocql.TypeDuration:  "duration",
		gocql.TypeList:      "list",
		gocql.TypeMap:       "map",
		gocql.TypeSet:       "set",
		gocql.TypeUDT:       "udt",
		gocql.TypeTuple:     "tuple",
	}
	if name, exists := names[valueType]; exists {
		return name
	}
	return fmt.Sprintf("unknown<%d>", valueType)
}

func columnSearchability(
	column *gocql.ColumnMetadata,
) providerv1.ColumnSearchability {
	switch column.Kind {
	case gocql.ColumnPartitionKey:
		return providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
	case gocql.ColumnClusteringKey:
		return providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_ORDERED
	default:
		if column.Index.Name != "" {
			return providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
		}
		return providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
	}
}

func orderedColumnNames(table *gocql.TableMetadata) []string {
	names := make([]string, 0, len(table.Columns))
	seen := make(map[string]struct{}, len(table.Columns))
	for _, columnName := range table.OrderedColumns {
		if _, exists := table.Columns[columnName]; !exists {
			continue
		}
		if _, exists := seen[columnName]; exists {
			continue
		}
		seen[columnName] = struct{}{}
		names = append(names, columnName)
	}

	remaining := make([]string, 0, len(table.Columns)-len(names))
	for columnName := range table.Columns {
		if _, exists := seen[columnName]; !exists {
			remaining = append(remaining, columnName)
		}
	}
	sort.Strings(remaining)

	return append(names, remaining...)
}

func joinColumnNames(columns []*gocql.ColumnMetadata) string {
	return strings.Join(columnNames(columns), ",")
}

func columnNames(columns []*gocql.ColumnMetadata) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		if column != nil {
			names = append(names, column.Name)
		}
	}
	return names
}
