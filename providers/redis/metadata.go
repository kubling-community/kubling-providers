package redis

import (
	"sort"
	"strconv"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

func buildMetadata(config Config) *providersdk.Metadata {
	metadata := &providersdk.Metadata{
		Properties: map[string]string{
			"redis.namespace_count": strconv.Itoa(len(config.Namespaces)),
		},
	}
	for _, namespace := range sortedNamespaces(config) {
		namespaceConfig := config.Namespaces[namespace]
		metadata.Namespaces = append(metadata.Namespaces, &providerv1.NamespaceMetadata{
			Name: namespace,
			Properties: map[string]string{
				"redis.database": strconv.Itoa(namespaceConfig.Database),
			},
		})
		tables := append([]TableConfig(nil), namespaceConfig.Tables...)
		sort.Slice(tables, func(left int, right int) bool {
			return tables[left].Name < tables[right].Name
		})
		for _, table := range tables {
			metadata.Tables = append(metadata.Tables, tableMetadata(namespace, table))
		}
	}
	return metadata
}

func tableMetadata(namespace string, table TableConfig) *providerv1.TableMetadata {
	updatable := table.Updatable
	metadata := &providerv1.TableMetadata{
		Name:       table.Name,
		SourceName: table.Name,
		Namespace:  namespace,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Annotation: table.Annotation,
		Properties: map[string]string{
			"redis.data_structure": "hash",
			"redis.key_prefix":     table.KeyPrefix,
		},
	}
	key := table.Key
	key.Updatable = table.Updatable
	metadata.Columns = append(metadata.Columns, columnMetadata(key))
	for _, field := range table.Fields {
		field.Updatable = table.Updatable && field.Updatable
		metadata.Columns = append(metadata.Columns, columnMetadata(field))
	}
	metadata.Keys = []*providerv1.KeyMetadata{{
		Name:       "PK_" + table.Name,
		Kind:       providerv1.KeyKind_KEY_KIND_PRIMARY,
		Columns:    []string{table.Key.Name},
		Properties: map[string]string{"redis.key_prefix": table.KeyPrefix},
	}}
	return metadata
}

func columnMetadata(column ColumnConfig) *providerv1.ColumnMetadata {
	nullable := column.Nullable
	updatable := column.Updatable
	return &providerv1.ColumnMetadata{
		Name:          column.Name,
		SourceName:    column.Name,
		Type:          column.Type,
		NativeType:    "redis hash field",
		Nullable:      &nullable,
		Updatable:     &updatable,
		Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_ALL,
		Annotation:    column.Annotation,
	}
}
