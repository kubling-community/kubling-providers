package provider

import (
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// AddStablePrimaryKey adds an engine-generated stable key column and makes it
// the table primary key.
func AddStablePrimaryKey(
	table *providerv1.TableMetadata,
	columnName string,
	componentColumns ...string,
) error {
	if table == nil {
		return fmt.Errorf("stable primary key table is required")
	}

	columnName = strings.TrimSpace(columnName)
	if columnName == "" {
		return fmt.Errorf("stable primary key column name is required")
	}
	if len(componentColumns) == 0 {
		return fmt.Errorf("stable primary key requires at least one component column")
	}

	columns := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		if column == nil || strings.TrimSpace(column.GetName()) == "" {
			continue
		}
		columns[strings.ToUpper(column.GetName())] = column
	}
	if columns[strings.ToUpper(columnName)] != nil {
		return fmt.Errorf("stable primary key column %q already exists", columnName)
	}

	components := make([]string, 0, len(componentColumns))
	seenComponents := make(map[string]struct{}, len(componentColumns))
	for _, requestedName := range componentColumns {
		requestedName = strings.TrimSpace(requestedName)
		if requestedName == "" {
			return fmt.Errorf("stable primary key component column name is required")
		}
		if strings.Contains(requestedName, "+") {
			return fmt.Errorf("stable primary key component column %q contains reserved separator +", requestedName)
		}
		key := strings.ToUpper(requestedName)
		component := columns[key]
		if component == nil {
			return fmt.Errorf("stable primary key component column %q does not exist", requestedName)
		}
		if component.GetStableKey() != nil {
			return fmt.Errorf("stable primary key component column %q is generated", component.GetName())
		}
		if _, exists := seenComponents[key]; exists {
			return fmt.Errorf("stable primary key component column %q is duplicated", component.GetName())
		}
		seenComponents[key] = struct{}{}
		components = append(components, component.GetName())
	}

	keyName := "PK_" + strings.TrimSpace(table.GetName())
	if strings.TrimSpace(table.GetName()) == "" {
		return fmt.Errorf("stable primary key table name is required")
	}
	for _, key := range table.GetKeys() {
		if key == nil {
			continue
		}
		if key.GetKind() == providerv1.KeyKind_KEY_KIND_PRIMARY {
			return fmt.Errorf("table %q already defines primary key %q", table.GetName(), key.GetName())
		}
		if strings.EqualFold(key.GetName(), keyName) {
			return fmt.Errorf("table %q already defines key %q", table.GetName(), key.GetName())
		}
	}

	nullable := false
	updatable := false
	table.Columns = append(table.Columns, &providerv1.ColumnMetadata{
		Name:          columnName,
		Type:          kublingv1.ValueType_VALUE_TYPE_STRING,
		Nullable:      &nullable,
		Updatable:     &updatable,
		Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY,
		Properties: map[string]string{
			"val_pk": strings.Join(components, "+"),
		},
		StableKey: &providerv1.StableKeyMetadata{
			Columns: components,
			Format:  providerv1.StableKeyFormat_STABLE_KEY_FORMAT_VAL_PK_V1,
		},
	})
	primaryKey := &providerv1.KeyMetadata{
		Name:    keyName,
		Kind:    providerv1.KeyKind_KEY_KIND_PRIMARY,
		Columns: []string{columnName},
	}
	table.Keys = append([]*providerv1.KeyMetadata{primaryKey}, table.Keys...)
	return nil
}

// MustAddStablePrimaryKey is AddStablePrimaryKey for statically defined
// metadata and panics when that metadata is invalid.
func MustAddStablePrimaryKey(
	table *providerv1.TableMetadata,
	columnName string,
	componentColumns ...string,
) {
	if err := AddStablePrimaryKey(table, columnName, componentColumns...); err != nil {
		panic(err)
	}
}
