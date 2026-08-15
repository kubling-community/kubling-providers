package provider

import (
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestAddStablePrimaryKey(t *testing.T) {
	nullable := false
	table := &providerv1.TableMetadata{
		Name: "TASK",
		Columns: []*providerv1.ColumnMetadata{
			{Name: "namespace", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: &nullable},
			{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: &nullable},
		},
		Keys: []*providerv1.KeyMetadata{
			{Name: "UK_TASK_SOURCE", Kind: providerv1.KeyKind_KEY_KIND_UNIQUE, Columns: []string{"namespace", "id"}},
		},
	}

	if err := AddStablePrimaryKey(table, "identifier", "NAMESPACE", "id"); err != nil {
		t.Fatalf("AddStablePrimaryKey() error = %v", err)
	}

	if len(table.GetColumns()) != 3 {
		t.Fatalf("columns = %d, want 3", len(table.GetColumns()))
	}
	identifier := table.GetColumns()[2]
	if identifier.GetName() != "identifier" ||
		identifier.GetType() != kublingv1.ValueType_VALUE_TYPE_STRING ||
		identifier.GetNullable() ||
		identifier.GetUpdatable() ||
		identifier.GetSearchability() != providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY {
		t.Fatalf("identifier metadata = %v", identifier)
	}
	stableKey := identifier.GetStableKey()
	if stableKey == nil {
		t.Fatal("identifier stable key is nil")
	}
	if got := stableKey.GetColumns(); len(got) != 2 || got[0] != "namespace" || got[1] != "id" {
		t.Fatalf("stable key columns = %v", got)
	}
	if stableKey.GetFormat() != providerv1.StableKeyFormat_STABLE_KEY_FORMAT_VAL_PK_V1 {
		t.Fatalf("stable key format = %v", stableKey.GetFormat())
	}
	if got := identifier.GetProperties()["val_pk"]; got != "namespace+id" {
		t.Fatalf("legacy val_pk property = %q, want %q", got, "namespace+id")
	}

	if len(table.GetKeys()) != 2 {
		t.Fatalf("keys = %d, want 2", len(table.GetKeys()))
	}
	primaryKey := table.GetKeys()[0]
	if primaryKey.GetName() != "PK_TASK" ||
		primaryKey.GetKind() != providerv1.KeyKind_KEY_KIND_PRIMARY ||
		len(primaryKey.GetColumns()) != 1 ||
		primaryKey.GetColumns()[0] != "identifier" {
		t.Fatalf("primary key metadata = %v", primaryKey)
	}
}

func TestAddStablePrimaryKeyRejectsInvalidMetadata(t *testing.T) {
	newTable := func() *providerv1.TableMetadata {
		return &providerv1.TableMetadata{
			Name: "TASK",
			Columns: []*providerv1.ColumnMetadata{
				{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
			},
		}
	}

	tests := []struct {
		name       string
		table      *providerv1.TableMetadata
		column     string
		components []string
		wantError  string
	}{
		{name: "nil table", column: "identifier", components: []string{"id"}, wantError: "table is required"},
		{name: "blank column", table: newTable(), components: []string{"id"}, wantError: "column name is required"},
		{name: "no components", table: newTable(), column: "identifier", wantError: "at least one component"},
		{name: "missing component", table: newTable(), column: "identifier", components: []string{"missing"}, wantError: "does not exist"},
		{name: "duplicate component", table: newTable(), column: "identifier", components: []string{"id", "ID"}, wantError: "is duplicated"},
		{name: "reserved separator", table: newTable(), column: "identifier", components: []string{"id+tenant"}, wantError: "contains reserved separator"},
		{name: "existing column", table: newTable(), column: "id", components: []string{"id"}, wantError: "already exists"},
		{
			name: "existing primary key",
			table: &providerv1.TableMetadata{
				Name:    "TASK",
				Columns: []*providerv1.ColumnMetadata{{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING}},
				Keys: []*providerv1.KeyMetadata{{
					Name:    "PK_TASK_SOURCE",
					Kind:    providerv1.KeyKind_KEY_KIND_PRIMARY,
					Columns: []string{"id"},
				}},
			},
			column:     "identifier",
			components: []string{"id"},
			wantError:  "already defines primary key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := AddStablePrimaryKey(test.table, test.column, test.components...)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("AddStablePrimaryKey() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestAddNamespaceColumns(t *testing.T) {
	nullable := false
	metadata := &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{
		{
			Name:      "POD",
			Namespace: " cluster-a ",
			Columns: []*providerv1.ColumnMetadata{
				{Name: "namespace", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: &nullable},
				{Name: "name", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: &nullable},
			},
		},
		{
			Name:      "NODE",
			Namespace: "cluster-b",
			Columns: []*providerv1.ColumnMetadata{
				{Name: "name", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: &nullable},
			},
		},
	}}
	MustAddStablePrimaryKey(metadata.Tables[0], "identifier", "namespace", "name")
	MustAddStablePrimaryKey(metadata.Tables[1], "identifier", "name")

	if err := AddNamespaceColumns(metadata, NamespaceColumnOptions{IncludeInStableKeys: true}); err != nil {
		t.Fatalf("AddNamespaceColumns() error = %v", err)
	}

	for index, table := range metadata.GetTables() {
		column := table.GetColumns()[len(table.GetColumns())-1]
		if column.GetName() != DefaultNamespaceColumnName ||
			column.GetType() != kublingv1.ValueType_VALUE_TYPE_STRING ||
			column.GetNullable() ||
			column.GetUpdatable() ||
			column.GetSearchability() != providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_ALL {
			t.Fatalf("table %d namespace column = %v", index, column)
		}
		if got := column.GetProperties()["val_constant"]; got != table.GetNamespace() {
			t.Fatalf("table %d val_constant = %q, want %q", index, got, table.GetNamespace())
		}
	}

	podIdentifier := metadata.GetTables()[0].GetColumns()[2]
	if got := podIdentifier.GetStableKey().GetColumns(); len(got) != 3 || got[0] != DefaultNamespaceColumnName || got[1] != "namespace" || got[2] != "name" {
		t.Fatalf("POD stable key columns = %v", got)
	}
	if got := podIdentifier.GetProperties()["val_pk"]; got != "kubling_namespace+namespace+name" {
		t.Fatalf("POD val_pk = %q", got)
	}

	nodeIdentifier := metadata.GetTables()[1].GetColumns()[1]
	if got := nodeIdentifier.GetStableKey().GetColumns(); len(got) != 2 || got[0] != DefaultNamespaceColumnName || got[1] != "name" {
		t.Fatalf("NODE stable key columns = %v", got)
	}
}

func TestAddNamespaceColumnsCanPreserveStableKeys(t *testing.T) {
	metadata := &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{{
		Name:      "TASK",
		Namespace: "tasks",
		Columns: []*providerv1.ColumnMetadata{{
			Name: "id",
			Type: kublingv1.ValueType_VALUE_TYPE_STRING,
		}},
	}}}
	MustAddStablePrimaryKey(metadata.Tables[0], "identifier", "id")

	if err := AddNamespaceColumns(metadata, NamespaceColumnOptions{
		ColumnName: "source_namespace",
	}); err != nil {
		t.Fatalf("AddNamespaceColumns() error = %v", err)
	}

	identifier := metadata.GetTables()[0].GetColumns()[1]
	if got := identifier.GetStableKey().GetColumns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("stable key columns = %v", got)
	}
	constant := metadata.GetTables()[0].GetColumns()[2]
	if constant.GetName() != "source_namespace" || constant.GetProperties()["val_constant"] != "tasks" {
		t.Fatalf("namespace column = %v", constant)
	}
}

func TestAddNamespaceColumnsRejectsInvalidMetadataWithoutMutation(t *testing.T) {
	validTable := &providerv1.TableMetadata{Name: "VALID", Namespace: "source-a"}
	metadata := &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{
		validTable,
		{Name: "INVALID"},
	}}

	err := AddNamespaceColumns(metadata, NamespaceColumnOptions{})
	if err == nil || !strings.Contains(err.Error(), "namespace is required") {
		t.Fatalf("AddNamespaceColumns() error = %v", err)
	}
	if len(validTable.GetColumns()) != 0 {
		t.Fatalf("valid table was mutated before validation completed: %v", validTable.GetColumns())
	}

	tests := []struct {
		name      string
		metadata  *providerv1.SchemaMetadata
		options   NamespaceColumnOptions
		wantError string
	}{
		{name: "nil metadata", wantError: "metadata is required"},
		{
			name: "reserved separator",
			metadata: &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{{
				Name:      "TASK",
				Namespace: "tasks",
			}}},
			options:   NamespaceColumnOptions{ColumnName: "source+namespace"},
			wantError: "reserved separator",
		},
		{
			name: "existing column",
			metadata: &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{{
				Name:      "TASK",
				Namespace: "tasks",
				Columns:   []*providerv1.ColumnMetadata{{Name: "KUBLING_NAMESPACE"}},
			}}},
			wantError: "already defines namespace column",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := AddNamespaceColumns(test.metadata, test.options)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("AddNamespaceColumns() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}
