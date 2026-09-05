package kubernetes

import (
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	ownerReferenceKindProperty  = "kubernetes.owner_reference_kind"
	ownerReferenceFieldProperty = "kubernetes.owner_reference_field"
)

type ownerReferenceProjection struct {
	columnName string
	ownerKind  string
	field      string
}

func ownerReferenceProjections(descriptor *resourceDescriptor) []ownerReferenceProjection {
	if descriptor == nil {
		return nil
	}

	switch {
	case descriptor.groupVersion == (schema.GroupVersion{Group: "apps", Version: "v1"}) &&
		descriptor.resource.Name == "replicasets":
		return []ownerReferenceProjection{
			{columnName: "deployment__uid", ownerKind: "Deployment", field: "uid"},
			{columnName: "deployment__name", ownerKind: "Deployment", field: "name"},
		}
	case descriptor.groupVersion == (schema.GroupVersion{Version: "v1"}) &&
		descriptor.resource.Name == "pods":
		return []ownerReferenceProjection{
			{columnName: "replica_set__uid", ownerKind: "ReplicaSet", field: "uid"},
			{columnName: "replica_set__name", ownerKind: "ReplicaSet", field: "name"},
		}
	default:
		return nil
	}
}

func ownerReferenceColumns(descriptor *resourceDescriptor) []*providerv1.ColumnMetadata {
	projections := ownerReferenceProjections(descriptor)
	columns := make([]*providerv1.ColumnMetadata, 0, len(projections))
	for _, projection := range projections {
		nullable := true
		updatable := false
		columns = append(columns, &providerv1.ColumnMetadata{
			Name:          projection.columnName,
			SourceName:    "metadata.ownerReferences",
			Type:          kublingv1.ValueType_VALUE_TYPE_STRING,
			NativeType:    "io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference",
			Nullable:      &nullable,
			Updatable:     &updatable,
			Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
			Annotation: fmt.Sprintf(
				"%s of the controlling Kubernetes %s owner reference.",
				projection.field,
				projection.ownerKind,
			),
			Properties: map[string]string{
				ownerReferenceKindProperty:  projection.ownerKind,
				ownerReferenceFieldProperty: projection.field,
			},
		})
	}
	return columns
}

func ownerReferenceColumnValue(
	resource *unstructured.Unstructured,
	column *providerv1.ColumnMetadata,
) (*kublingv1.Value, bool, error) {
	ownerKind := strings.TrimSpace(column.GetProperties()[ownerReferenceKindProperty])
	field := strings.TrimSpace(column.GetProperties()[ownerReferenceFieldProperty])
	if ownerKind == "" && field == "" {
		return nil, false, nil
	}
	if ownerKind == "" || field == "" {
		return nil, true, fmt.Errorf("incomplete Kubernetes owner reference projection metadata")
	}

	references, found, err := unstructured.NestedSlice(
		resource.Object,
		"metadata",
		"ownerReferences",
	)
	if err != nil {
		return nil, true, err
	}
	if !found {
		return nullValue(), true, nil
	}

	for _, candidate := range references {
		reference, ok := candidate.(map[string]any)
		if !ok {
			return nil, true, fmt.Errorf("owner reference is %T, want object", candidate)
		}
		controller, _, err := unstructured.NestedBool(reference, "controller")
		if err != nil {
			return nil, true, err
		}
		kind, _, err := unstructured.NestedString(reference, "kind")
		if err != nil {
			return nil, true, err
		}
		if !controller || kind != ownerKind {
			continue
		}
		value, found, err := unstructured.NestedString(reference, field)
		if err != nil {
			return nil, true, err
		}
		if !found || value == "" {
			return nullValue(), true, nil
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_StringValue{StringValue: value},
		}, true, nil
	}

	return nullValue(), true, nil
}

func applyOwnerReferenceForeignKeys(metadata *providerv1.SchemaMetadata) {
	deployment := kubernetesResourceTable(metadata, "apps", "v1", "deployments")
	replicaSet := kubernetesResourceTable(metadata, "apps", "v1", "replicasets")
	pod := kubernetesResourceTable(metadata, "", "v1", "pods")

	if deployment != nil && replicaSet != nil {
		replicaSet.Keys = append(replicaSet.Keys, ownerReferenceForeignKey(
			"FK_"+replicaSet.GetName()+"_DEPLOYMENT",
			"deployment__uid",
			deployment.GetName(),
		))
	}
	if replicaSet != nil && pod != nil {
		pod.Keys = append(pod.Keys, ownerReferenceForeignKey(
			"FK_"+pod.GetName()+"_REPLICA_SET",
			"replica_set__uid",
			replicaSet.GetName(),
		))
	}
}

func kubernetesResourceTable(
	metadata *providerv1.SchemaMetadata,
	group string,
	version string,
	resource string,
) *providerv1.TableMetadata {
	for _, table := range metadata.GetTables() {
		if table.GetProperties()["kubernetes.group"] == group &&
			table.GetProperties()["kubernetes.version"] == version &&
			table.GetProperties()["kubernetes.resource"] == resource {
			return table
		}
	}
	return nil
}

func ownerReferenceForeignKey(
	name string,
	column string,
	referencedTable string,
) *providerv1.KeyMetadata {
	return &providerv1.KeyMetadata{
		Name:              name,
		Kind:              providerv1.KeyKind_KEY_KIND_FOREIGN,
		Columns:           []string{column},
		ReferencedTable:   referencedTable,
		ReferencedColumns: []string{"metadata__uid"},
	}
}
