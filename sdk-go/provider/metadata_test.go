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
