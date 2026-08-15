package kubernetes

import (
	"fmt"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

func applyConfiguredNamespace(
	metadata *providerv1.SchemaMetadata,
	config Config,
) error {
	if metadata == nil {
		return fmt.Errorf("Kubernetes metadata is required")
	}
	if config.Namespace == "" {
		return nil
	}

	metadata.Namespaces = []*providerv1.NamespaceMetadata{{
		Name:       config.Namespace,
		Annotation: fmt.Sprintf("Kubernetes cluster exposed as namespace %s", config.Namespace),
	}}
	for _, table := range metadata.GetTables() {
		if table == nil {
			return fmt.Errorf("Kubernetes metadata contains a nil table")
		}
		table.Namespace = config.Namespace
	}
	if !config.NamespaceColumn.Enabled {
		return nil
	}
	return providersdk.AddNamespaceColumns(metadata, providersdk.NamespaceColumnOptions{
		ColumnName:          config.NamespaceColumn.Name,
		IncludeInStableKeys: config.NamespaceColumn.IncludeInStableKeys,
	})
}

func configuredTableMetadata(
	table *providerv1.TableMetadata,
	config Config,
) (*providerv1.TableMetadata, error) {
	metadata := &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{table}}
	if err := applyConfiguredNamespace(metadata, config); err != nil {
		return nil, err
	}
	return table, nil
}

func expectedEntityNamespace(config Config, groupVersion string) string {
	if config.Namespace != "" {
		return config.Namespace
	}
	return groupVersion
}
