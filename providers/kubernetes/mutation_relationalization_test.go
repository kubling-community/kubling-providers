package kubernetes

import (
	"context"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/openapi"
)

func TestCronJobFlattenedInsertMaterializesNestedKubernetesObject(t *testing.T) {
	descriptor := &resourceDescriptor{
		groupVersion: schema.GroupVersion{Group: "batch", Version: "v1"},
		resource: metav1.APIResource{
			Name:       "cronjobs",
			Kind:       "CronJob",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list", "create", "patch", "update", "delete"},
		},
		baseName:  "CRON_JOB",
		tableName: "CRON_JOB",
	}

	resolver := newOpenAPISchemaResolver(
		context.Background(),
		&openAPIDiscovery{
			staticDiscovery: &staticDiscovery{},
			client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
				"apis/batch/v1": &staticOpenAPIGroupVersion{
					contents:          []byte(testCronJobOpenAPIV3),
					serverRelativeURL: "/openapi/v3/apis/batch/v1?hash=cronjob",
				},
			}},
		},
		&openAPISchemaCache{},
	)
	if resolver == nil {
		t.Fatal("newOpenAPISchemaResolver() = nil")
	}

	// Depth 6 reaches PodSpec scalar/array fields through:
	// spec.jobTemplate.spec.template.spec.<field>.
	table := resourceTableMetadataWithSchema(descriptor, resolver, 6, true)
	if metadataColumnExists(table, "spec__jobTemplate") {
		t.Fatal("spec__jobTemplate JSON parent should not coexist with flattened descendants")
	}
	assertExpandedColumn(
		t,
		table,
		"spec__jobTemplate__spec__template__spec__containers",
		"spec.jobTemplate.spec.template.spec.containers",
		kublingv1.ValueType_VALUE_TYPE_JSON,
		true,
	)
	assertExpandedColumn(
		t,
		table,
		"spec__jobTemplate__spec__template__spec__restartPolicy",
		"spec.jobTemplate.spec.template.spec.restartPolicy",
		kublingv1.ValueType_VALUE_TYPE_STRING,
		true,
	)

	fieldNames := []string{
		"metadata__name",
		"metadata__namespace",
		"metadata__labels",
		"spec__concurrencyPolicy",
		"spec__failedJobsHistoryLimit",
		"spec__jobTemplate__metadata__name",
		"spec__jobTemplate__spec__template__metadata__labels",
		"spec__jobTemplate__spec__template__spec__containers",
		"spec__jobTemplate__spec__template__spec__restartPolicy",
		"spec__schedule",
		"spec__startingDeadlineSeconds",
		"spec__successfulJobsHistoryLimit",
		"spec__suspend",
		"spec__timeZone",
	}
	fields := make([]*providerv1.Field, 0, len(fieldNames))
	for _, name := range fieldNames {
		fields = append(fields, &providerv1.Field{Name: name})
	}
	columns, err := mutationColumns(table, fields)
	if err != nil {
		t.Fatalf("mutationColumns() error = %v", err)
	}

	values := []*kublingv1.Value{
		kubernetesStringValue("demo-cronjob"),
		kubernetesStringValue("default"),
		kubernetesJSONValue(`{"app":"demo","component":"batch"}`),
		kubernetesStringValue("Allow"),
		kubernetesIntegerValue(1),
		kubernetesStringValue("demo-cronjob-template"),
		kubernetesJSONValue(`{"app":"demo"}`),
		kubernetesJSONValue(`[{"name":"demo","image":"busybox","args":["echo","Hello from CronJob!"]}]`),
		kubernetesStringValue("OnFailure"),
		kubernetesStringValue("*/5 * * * *"),
		kubernetesLongValue(30),
		kubernetesIntegerValue(3),
		kubernetesBooleanValue(false),
		kubernetesStringValue("UTC"),
	}

	resolved := &resolvedResource{
		descriptor: descriptor,
		table:      table,
		client:     &fakeKubernetesClient{defaultNamespace: "default"},
		strategy:   BlankNamespaceAll,
	}
	object, err := buildInsertObject(resolved, columns, values)
	if err != nil {
		t.Fatalf("buildInsertObject() error = %v", err)
	}

	if object.GetAPIVersion() != "batch/v1" || object.GetKind() != "CronJob" {
		t.Fatalf("identity = %s %s, want batch/v1 CronJob", object.GetAPIVersion(), object.GetKind())
	}
	if object.GetName() != "demo-cronjob" || object.GetNamespace() != "default" {
		t.Fatalf("metadata identity = %s/%s", object.GetNamespace(), object.GetName())
	}
	assertNestedString(t, object.Object, "*/5 * * * *", "spec", "schedule")
	assertNestedString(t, object.Object, "Allow", "spec", "concurrencyPolicy")
	assertNestedString(t, object.Object, "demo-cronjob-template", "spec", "jobTemplate", "metadata", "name")
	assertNestedString(t, object.Object, "OnFailure", "spec", "jobTemplate", "spec", "template", "spec", "restartPolicy")
	assertNestedString(t, object.Object, "UTC", "spec", "timeZone")

	containers, found, err := unstructured.NestedSlice(
		object.Object,
		"spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("containers = %v, found=%v, err=%v", containers, found, err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok || container["image"] != "busybox" {
		t.Fatalf("container = %#v", containers[0])
	}
}

func TestObjectEscapeHatchCannotMixWithModeledContent(t *testing.T) {
	descriptor := &resourceDescriptor{
		groupVersion: schema.GroupVersion{Group: "batch", Version: "v1"},
		resource: metav1.APIResource{
			Name:       "cronjobs",
			Kind:       "CronJob",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list", "create", "patch", "update"},
		},
		baseName:  "CRON_JOB",
		tableName: "CRON_JOB",
	}
	resolver := newOpenAPISchemaResolver(
		context.Background(),
		&openAPIDiscovery{
			staticDiscovery: &staticDiscovery{},
			client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
				"apis/batch/v1": &staticOpenAPIGroupVersion{
					contents:          []byte(testCronJobOpenAPIV3),
					serverRelativeURL: "/openapi/v3/apis/batch/v1?hash=cronjob-conflict",
				},
			}},
		},
		&openAPISchemaCache{},
	)
	table := resourceTableMetadataWithSchema(descriptor, resolver, 6, true)

	_, err := mutationColumns(table, []*providerv1.Field{
		{Name: "object"},
		{Name: "spec__schedule"},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with object") {
		t.Fatalf("mutationColumns() error = %v, want object/content conflict", err)
	}

	// Identity columns are allowed companions because apiVersion/kind may be
	// provider defaults and name/namespace are validated against the object.
	if _, err := mutationColumns(table, []*providerv1.Field{
		{Name: "object"},
		{Name: "metadata__name"},
		{Name: "metadata__namespace"},
	}); err != nil {
		t.Fatalf("object + identity mutationColumns() error = %v", err)
	}
}

func assertNestedString(t *testing.T, object map[string]any, want string, fields ...string) {
	t.Helper()
	value, found, err := unstructured.NestedString(object, fields...)
	if err != nil || !found || value != want {
		t.Fatalf("NestedString(%v) = %q, found=%v, err=%v; want %q", fields, value, found, err, want)
	}
}

func kubernetesStringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func kubernetesIntegerValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func kubernetesLongValue(value int64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}
}

func kubernetesBooleanValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}

func kubernetesJSONValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: value}}
}

const testCronJobOpenAPIV3 = `{
  "openapi": "3.0.0",
  "components": {
    "schemas": {
      "io.k8s.api.batch.v1.CronJob": {
        "type": "object",
        "x-kubernetes-group-version-kind": [
          {"group": "batch", "version": "v1", "kind": "CronJob"}
        ],
        "properties": {
          "apiVersion": {"type": "string"},
          "kind": {"type": "string"},
          "metadata": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
          "spec": {"$ref": "#/components/schemas/io.k8s.api.batch.v1.CronJobSpec"},
          "status": {"$ref": "#/components/schemas/io.k8s.api.batch.v1.CronJobStatus"}
        }
      },
      "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "namespace": {"type": "string"},
          "uid": {"type": "string", "readOnly": true},
          "resourceVersion": {"type": "string", "readOnly": true},
          "labels": {
            "type": "object",
            "additionalProperties": {"type": "string"}
          }
        }
      },
      "io.k8s.api.batch.v1.CronJobSpec": {
        "type": "object",
        "properties": {
          "concurrencyPolicy": {"type": "string"},
          "failedJobsHistoryLimit": {"type": "integer", "format": "int32"},
          "jobTemplate": {"$ref": "#/components/schemas/io.k8s.api.batch.v1.JobTemplateSpec"},
          "schedule": {"type": "string"},
          "startingDeadlineSeconds": {"type": "integer", "format": "int64"},
          "successfulJobsHistoryLimit": {"type": "integer", "format": "int32"},
          "suspend": {"type": "boolean"},
          "timeZone": {"type": "string"}
        }
      },
      "io.k8s.api.batch.v1.JobTemplateSpec": {
        "type": "object",
        "properties": {
          "metadata": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
          "spec": {"$ref": "#/components/schemas/io.k8s.api.batch.v1.JobSpec"}
        }
      },
      "io.k8s.api.batch.v1.JobSpec": {
        "type": "object",
        "properties": {
          "template": {"$ref": "#/components/schemas/io.k8s.api.core.v1.PodTemplateSpec"}
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
            "items": {"type": "object"}
          },
          "restartPolicy": {"type": "string"}
        }
      },
      "io.k8s.api.batch.v1.CronJobStatus": {
        "type": "object",
        "properties": {
          "active": {
            "type": "array",
            "items": {"type": "object"}
          }
        }
      }
    }
  }
}`
