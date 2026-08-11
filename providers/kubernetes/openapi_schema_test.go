package kubernetes

import (
	"context"
	"errors"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/openapi"
)

type openAPIDiscovery struct {
	*staticDiscovery
	client openapi.Client
}

func (d *openAPIDiscovery) OpenAPIV3() openapi.Client {
	return d.client
}

type staticOpenAPIClient struct {
	paths map[string]openapi.GroupVersion
	err   error
}

func (c *staticOpenAPIClient) Paths() (map[string]openapi.GroupVersion, error) {
	return c.paths, c.err
}

type staticOpenAPIGroupVersion struct {
	contents          []byte
	err               error
	serverRelativeURL string
}

func (g *staticOpenAPIGroupVersion) Schema(contentType string) ([]byte, error) {
	if contentType != openAPIJSONContentType {
		return nil, errors.New("unexpected content type")
	}
	if g.err != nil {
		return nil, g.err
	}
	return append([]byte(nil), g.contents...), nil
}

func (g *staticOpenAPIGroupVersion) ServerRelativeURL() string {
	return g.serverRelativeURL
}

func TestProviderMetadataExpandsDeploymentFromOpenAPIV3(t *testing.T) {
	groupVersion := &staticOpenAPIGroupVersion{
		contents:          []byte(testDeploymentOpenAPIV3),
		serverRelativeURL: "/openapi/v3/apis/apps/v1?hash=test",
	}
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: deploymentResourceLists()},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"apis/apps/v1": groupVersion,
		}},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	config, err := normalizeConfig(Config{Schema: SchemaConfig{FieldExpansionDepth: 4}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}

	deployment := metadataTable(t, metadata, "DEPLOYMENT")
	if got, want := deployment.GetAnnotation(), "Deployment enables declarative updates for Pods and ReplicaSets."; got != want {
		t.Fatalf("DEPLOYMENT annotation = %q, want %q", got, want)
	}
	assertExpandedColumn(t, deployment, "metadata__labels", "metadata.labels", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, deployment, "spec__replicas", "spec.replicas", kublingv1.ValueType_VALUE_TYPE_INTEGER, true)
	assertExpandedColumn(t, deployment, "spec__selector__matchLabels", "spec.selector.matchLabels", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, deployment, "spec__template__metadata__labels", "spec.template.metadata.labels", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, deployment, "spec__template__spec__containers", "spec.template.spec.containers", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, deployment, "status__conditions", "status.conditions", kublingv1.ValueType_VALUE_TYPE_JSON, false)
	assertExpandedColumn(t, deployment, "status__availableReplicas", "status.availableReplicas", kublingv1.ValueType_VALUE_TYPE_INTEGER, false)
	assertExpandedColumn(t, deployment, "status__readyReplicas", "status.readyReplicas", kublingv1.ValueType_VALUE_TYPE_INTEGER, false)
	assertExpandedColumn(t, deployment, "status__replicas", "status.replicas", kublingv1.ValueType_VALUE_TYPE_INTEGER, false)
	assertExpandedColumn(t, deployment, "status__updatedReplicas", "status.updatedReplicas", kublingv1.ValueType_VALUE_TYPE_INTEGER, false)

	for _, name := range []string{"metadata", "spec", "status"} {
		if metadataColumnExists(deployment, name) {
			t.Fatalf("expanded object column %q should not coexist with its flattened children", name)
		}
	}
	object := metadataColumn(t, deployment, "object")
	if !object.GetNullable() {
		t.Fatal("expanded object escape hatch is not nullable")
	}
	if object.GetDefaultExpression() != "" {
		t.Fatalf("expanded object default = %q, want empty", object.GetDefaultExpression())
	}

	if deployment.GetProperties()["kubernetes.schema_expansion"] != "openapi_v3" {
		t.Fatalf("schema expansion property = %q", deployment.GetProperties()["kubernetes.schema_expansion"])
	}
	if deployment.GetProperties()["kubernetes.field_expansion_depth"] != "4" {
		t.Fatalf("field expansion depth property = %q", deployment.GetProperties()["kubernetes.field_expansion_depth"])
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
	}
}

func TestProviderMetadataExpandsConfigMapTopLevelFieldsFromOpenAPIV3(t *testing.T) {
	groupVersion := &staticOpenAPIGroupVersion{
		contents:          []byte(testConfigMapOpenAPIV3),
		serverRelativeURL: "/openapi/v3/api/v1?hash=config-map-test",
	}
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: configMapResourceLists()},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"api/v1": groupVersion,
		}},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	config, err := normalizeConfig(Config{Schema: SchemaConfig{FieldExpansionDepth: 4}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}

	configMap := metadataTable(t, metadata, "CONFIG_MAP")
	if got, want := configMap.GetAnnotation(), "Kubernetes ConfigMap resource from v1"; got != want {
		t.Fatalf("CONFIG_MAP fallback annotation = %q, want %q", got, want)
	}
	assertExpandedColumn(t, configMap, "data", "data", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, configMap, "binaryData", "binaryData", kublingv1.ValueType_VALUE_TYPE_JSON, true)
	assertExpandedColumn(t, configMap, "immutable", "immutable", kublingv1.ValueType_VALUE_TYPE_BOOLEAN, true)
	assertExpandedColumn(t, configMap, "metadata__labels", "metadata.labels", kublingv1.ValueType_VALUE_TYPE_JSON, true)

	if metadataColumnExists(configMap, "apiVersion") {
		t.Fatal("OpenAPI apiVersion was exposed in addition to canonical api_version")
	}
	if !metadataColumnExists(configMap, "api_version") || !metadataColumnExists(configMap, "kind") {
		t.Fatalf("canonical identity columns are missing: %v", configMap.GetColumns())
	}
	for _, name := range []string{"metadata", "spec", "status"} {
		if metadataColumnExists(configMap, name) {
			t.Fatalf("expanded ConfigMap unexpectedly exposes document root %q", name)
		}
	}
}

func TestProviderMetadataCanOmitObjectColumn(t *testing.T) {
	includeObject := false
	groupVersion := &staticOpenAPIGroupVersion{
		contents:          []byte(testConfigMapOpenAPIV3),
		serverRelativeURL: "/openapi/v3/api/v1?hash=config-map-test",
	}
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: configMapResourceLists()},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"api/v1": groupVersion,
		}},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	config, err := normalizeConfig(Config{Schema: SchemaConfig{
		FieldExpansionDepth: 4,
		IncludeObject:       &includeObject,
	}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}

	configMap := metadataTable(t, metadata, "CONFIG_MAP")
	if metadataColumnExists(configMap, "object") {
		t.Fatal("object column is present with includeObject=false")
	}
	if !metadataColumnExists(configMap, "data") || !metadataColumnExists(configMap, "metadata__labels") {
		t.Fatalf("expected relationalized columns are missing: %v", configMap.GetColumns())
	}
	if metadataColumnExists(configMap, "metadata") {
		t.Fatal("expanded metadata JSON root should not coexist with metadata children")
	}
}

func TestOpenAPIExpansionUsesJSONOnlyAtDepthBoundary(t *testing.T) {
	groupVersion := &staticOpenAPIGroupVersion{
		contents:          []byte(testDeploymentOpenAPIV3),
		serverRelativeURL: "/openapi/v3/apis/apps/v1?hash=depth-boundary",
	}
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: deploymentResourceLists()},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"apis/apps/v1": groupVersion,
		}},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	config, err := normalizeConfig(Config{Schema: SchemaConfig{FieldExpansionDepth: 2}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	deployment := metadataTable(t, metadata, "DEPLOYMENT")

	assertExpandedColumn(
		t, deployment, "spec__template", "spec.template",
		kublingv1.ValueType_VALUE_TYPE_JSON, true,
	)
	if metadataColumnExists(deployment, "spec__template__metadata__labels") {
		t.Fatal("field below configured depth was unexpectedly flattened")
	}
	if metadataColumnExists(deployment, "spec") {
		t.Fatal("structured parent inside configured depth was exposed as JSON")
	}
}

func TestProviderMetadataFallsBackWhenOpenAPIV3IsUnavailable(t *testing.T) {
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: deploymentResourceLists()},
		client:          &staticOpenAPIClient{err: errors.New("openapi unavailable")},
	}
	client := &fakeKubernetesClient{discoveryClient: discoveryClient}

	config, err := normalizeConfig(Config{Schema: SchemaConfig{FieldExpansionDepth: 4}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	deployment := metadataTable(t, metadata, "DEPLOYMENT")
	if len(deployment.GetColumns()) != 10 {
		t.Fatalf("columns = %d, want compact 10 columns", len(deployment.GetColumns()))
	}
	if deployment.GetProperties()["kubernetes.schema_expansion"] != "" {
		t.Fatalf("unexpected schema expansion property = %q", deployment.GetProperties()["kubernetes.schema_expansion"])
	}
}

func TestExpandedColumnsReadNestedUnstructuredValues(t *testing.T) {
	resource := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "demo",
			"namespace": "default",
			"labels": map[string]any{
				"app": "demo",
			},
		},
		"spec": map[string]any{
			"replicas": int64(3),
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "app", "image": "example/app:v1"},
					},
				},
			},
		},
		"status": map[string]any{
			"readyReplicas": int64(2),
		},
	}}

	readyColumn := &providerv1.ColumnMetadata{
		Name:       "status__readyReplicas",
		SourceName: "status.readyReplicas",
		Type:       kublingv1.ValueType_VALUE_TYPE_INTEGER,
	}
	ready, err := resourceColumnValue(resource, readyColumn)
	if err != nil {
		t.Fatalf("ready replicas error = %v", err)
	}
	if ready.GetIntegerValue() != 2 {
		t.Fatalf("ready replicas = %d, want 2", ready.GetIntegerValue())
	}

	containersColumn := &providerv1.ColumnMetadata{
		Name:       "spec__template__spec__containers",
		SourceName: "spec.template.spec.containers",
		Type:       kublingv1.ValueType_VALUE_TYPE_JSON,
	}
	containers, err := resourceColumnValue(resource, containersColumn)
	if err != nil {
		t.Fatalf("containers error = %v", err)
	}
	if !strings.Contains(containers.GetJsonValue(), `"image":"example/app:v1"`) {
		t.Fatalf("containers JSON = %q", containers.GetJsonValue())
	}

	missingColumn := &providerv1.ColumnMetadata{
		Name:       "status__updatedReplicas",
		SourceName: "status.updatedReplicas",
		Type:       kublingv1.ValueType_VALUE_TYPE_INTEGER,
	}
	missing, err := resourceColumnValue(resource, missingColumn)
	if err != nil {
		t.Fatalf("missing field error = %v", err)
	}
	if missing.GetNullValue() == nil {
		t.Fatalf("missing field = %v, want null", missing)
	}
}

func assertExpandedColumn(
	t *testing.T,
	table *providerv1.TableMetadata,
	name string,
	sourceName string,
	valueType kublingv1.ValueType,
	updatable bool,
) {
	t.Helper()
	column := metadataColumn(t, table, name)
	if column.GetSourceName() != sourceName {
		t.Fatalf("column %q source = %q, want %q", name, column.GetSourceName(), sourceName)
	}
	if column.GetType() != valueType {
		t.Fatalf("column %q type = %v, want %v", name, column.GetType(), valueType)
	}
	if column.GetUpdatable() != updatable {
		t.Fatalf("column %q updatable = %v, want %v", name, column.GetUpdatable(), updatable)
	}
}

func metadataColumnExists(table *providerv1.TableMetadata, name string) bool {
	for _, column := range table.GetColumns() {
		if strings.EqualFold(column.GetName(), name) {
			return true
		}
	}
	return false
}

func configMapResourceLists() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{
			Name:       "configmaps",
			Kind:       "ConfigMap",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list", "create", "patch", "update", "delete"},
		}},
	}}
}

const testConfigMapOpenAPIV3 = `{
  "openapi": "3.0.0",
  "components": {
    "schemas": {
      "io.k8s.api.core.v1.ConfigMap": {
        "type": "object",
        "x-kubernetes-group-version-kind": [
          {"group": "", "version": "v1", "kind": "ConfigMap"}
        ],
        "properties": {
          "apiVersion": {"type": "string"},
          "kind": {"type": "string"},
          "metadata": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
          "immutable": {"type": "boolean"},
          "data": {
            "type": "object",
            "additionalProperties": {"type": "string"}
          },
          "binaryData": {
            "type": "object",
            "additionalProperties": {"type": "string", "format": "byte"}
          }
        }
      },
      "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "namespace": {"type": "string"},
          "uid": {"type": "string"},
          "resourceVersion": {"type": "string"},
          "labels": {
            "type": "object",
            "additionalProperties": {"type": "string"}
          }
        }
      }
    }
  }
}`

func deploymentResourceLists() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{{
			Name:       "deployments",
			Kind:       "Deployment",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list", "create", "patch", "update", "delete"},
		}},
	}}
}

const testDeploymentOpenAPIV3 = `{
  "openapi": "3.0.0",
  "components": {
    "schemas": {
      "io.k8s.api.apps.v1.Deployment": {
        "type": "object",
        "description": "Deployment enables declarative updates for Pods and ReplicaSets.",
        "x-kubernetes-group-version-kind": [
          {"group": "apps", "version": "v1", "kind": "Deployment"}
        ],
        "properties": {
          "metadata": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
          "spec": {"$ref": "#/components/schemas/io.k8s.api.apps.v1.DeploymentSpec"},
          "status": {"$ref": "#/components/schemas/io.k8s.api.apps.v1.DeploymentStatus"}
        }
      },
      "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "namespace": {"type": "string"},
          "uid": {"type": "string"},
          "resourceVersion": {"type": "string"},
          "labels": {
            "type": "object",
            "additionalProperties": {"type": "string"}
          }
        }
      },
      "io.k8s.api.apps.v1.DeploymentSpec": {
        "type": "object",
        "properties": {
          "replicas": {"type": "integer", "format": "int32"},
          "selector": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.LabelSelector"},
          "template": {"$ref": "#/components/schemas/io.k8s.api.core.v1.PodTemplateSpec"}
        }
      },
      "io.k8s.apimachinery.pkg.apis.meta.v1.LabelSelector": {
        "type": "object",
        "properties": {
          "matchLabels": {
            "type": "object",
            "additionalProperties": {"type": "string"}
          }
        }
      },
      "io.k8s.api.core.v1.PodTemplateSpec": {
        "type": "object",
        "properties": {
          "metadata": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
          "spec": {"$ref": "#/components/schemas/io.k8s.api.core.v1.PodSpec"}
        }
      },
      "io.k8s.api.core.v1.PodSpec": {
        "type": "object",
        "properties": {
          "containers": {
            "type": "array",
            "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.Container"}
          }
        }
      },
      "io.k8s.api.core.v1.Container": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "image": {"type": "string"}
        }
      },
      "io.k8s.api.apps.v1.DeploymentStatus": {
        "type": "object",
        "properties": {
          "conditions": {
            "type": "array",
            "items": {"type": "object"}
          },
          "availableReplicas": {"type": "integer", "format": "int32"},
          "readyReplicas": {"type": "integer", "format": "int32"},
          "replicas": {"type": "integer", "format": "int32"},
          "updatedReplicas": {"type": "integer", "format": "int32"}
        }
      }
    }
  }
}`

var _ openapi.Client = (*staticOpenAPIClient)(nil)
var _ openapi.GroupVersion = (*staticOpenAPIGroupVersion)(nil)
