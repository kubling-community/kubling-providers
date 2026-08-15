package kubernetes

import (
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestApplyConfiguredNamespaceMaterializesStableIdentity(t *testing.T) {
	metadata := buildMetadata(queryResourceLists(), nil)
	config, err := normalizeConfig(Config{
		Namespace: "kubernetes-production",
		NamespaceColumn: NamespaceColumnConfig{
			Enabled:             true,
			IncludeInStableKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	if err := applyConfiguredNamespace(metadata, config); err != nil {
		t.Fatalf("applyConfiguredNamespace() error = %v", err)
	}

	if len(metadata.GetNamespaces()) != 1 || metadata.GetNamespaces()[0].GetName() != "kubernetes-production" {
		t.Fatalf("namespaces = %v", metadata.GetNamespaces())
	}
	for _, table := range metadata.GetTables() {
		if table.GetNamespace() != "kubernetes-production" {
			t.Fatalf("table %q namespace = %q", table.GetName(), table.GetNamespace())
		}
		constant := namespaceTestColumn(table, "kubling_namespace")
		if constant == nil || constant.GetProperties()["val_constant"] != "kubernetes-production" {
			t.Fatalf("table %q namespace column = %v", table.GetName(), constant)
		}
		identifier := namespaceTestColumn(table, "identifier")
		if identifier == nil || len(identifier.GetStableKey().GetColumns()) < 2 ||
			identifier.GetStableKey().GetColumns()[0] != "kubling_namespace" {
			t.Fatalf("table %q stable key = %v", table.GetName(), identifier.GetStableKey())
		}
	}
}

func TestApplyConfiguredNamespaceCanUseCustomColumnWithoutChangingStableKeys(t *testing.T) {
	metadata := buildMetadata(queryResourceLists(), nil)
	config, err := normalizeConfig(Config{
		Namespace: "shared-cluster",
		NamespaceColumn: NamespaceColumnConfig{
			Enabled: true,
			Name:    "cluster_name",
		},
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	if err := applyConfiguredNamespace(metadata, config); err != nil {
		t.Fatalf("applyConfiguredNamespace() error = %v", err)
	}

	pod := namespaceTestTable(metadata.GetTables(), "POD")
	constant := namespaceTestColumn(pod, "cluster_name")
	if constant == nil || constant.GetProperties()["val_constant"] != "shared-cluster" {
		t.Fatalf("cluster_name = %v", constant)
	}
	identifier := namespaceTestColumn(pod, "identifier")
	if got := identifier.GetStableKey().GetColumns(); len(got) != 2 ||
		got[0] != "metadata__namespace" || got[1] != "metadata__name" {
		t.Fatalf("stable key columns = %v", got)
	}
}

func namespaceTestTable(tables []*providerv1.TableMetadata, name string) *providerv1.TableMetadata {
	for _, table := range tables {
		if table.GetName() == name {
			return table
		}
	}
	return nil
}

func namespaceTestColumn(table *providerv1.TableMetadata, name string) *providerv1.ColumnMetadata {
	if table == nil {
		return nil
	}
	for _, column := range table.GetColumns() {
		if column.GetName() == name {
			return column
		}
	}
	return nil
}
