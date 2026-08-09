package kubernetes

import (
	"context"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/openapi"
)

func TestKubernetesMutationLifecycle(t *testing.T) {
	connection, client := mutationTestConnection(t, metav1.Verbs{"get", "list", "create", "patch", "update", "delete"})
	ctx := context.Background()
	entity := &providerv1.EntityReference{Name: "POD", Namespace: "v1"}

	inserted, err := connection.Insert(ctx, &providerv1.InsertRequest{
		Entity: entity,
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: "object", Type: kublingv1.ValueType_VALUE_TYPE_JSON},
				{Name: "metadata__name", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
			},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{jsonProviderValue(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"namespace":"team-a"},
				"spec":{"nodeName":"node-a"}
			}`), stringValue("pod-a")}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = %v, %v", inserted, err)
	}

	updated, err := connection.Update(ctx, &providerv1.UpdateRequest{
		Entity: entity,
		Assignments: []*providerv1.Assignment{{
			Field: "spec",
			Value: literalExpression(jsonProviderValue(`{"nodeName":"node-b"}`)),
		}},
		Filter: mutationIdentity("pod-a", "team-a"),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %v, %v", updated, err)
	}

	resource := client.Resource(podResource()).Namespace("team-a")
	pod, err := resource.Get(ctx, "pod-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if nodeName, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName"); nodeName != "node-b" {
		t.Fatalf("spec.nodeName = %q, want node-b", nodeName)
	}

	deleted, err := connection.Delete(ctx, &providerv1.DeleteRequest{
		Entity: entity,
		Filter: mutationIdentity("pod-a", "team-a"),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = %v, %v", deleted, err)
	}
	if _, err := resource.Get(ctx, "pod-a", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Get() after delete error = %v, want NotFound", err)
	}
}

func TestKubernetesExpandedFieldMutationLifecycle(t *testing.T) {
	connection, client := deploymentMutationTestConnection(
		t,
		metav1.Verbs{"get", "list", "create", "patch", "update", "delete"},
	)
	ctx := context.Background()
	entity := &providerv1.EntityReference{Name: "DEPLOYMENT", Namespace: "apps/v1"}

	inserted, err := connection.Insert(ctx, &providerv1.InsertRequest{
		Entity: entity,
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: "metadata__name", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
				{Name: "metadata__namespace", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
				{Name: "metadata__labels", Type: kublingv1.ValueType_VALUE_TYPE_JSON},
				{Name: "spec__replicas", Type: kublingv1.ValueType_VALUE_TYPE_INTEGER},
				{Name: "spec__selector__matchLabels", Type: kublingv1.ValueType_VALUE_TYPE_JSON},
				{Name: "spec__template__metadata__labels", Type: kublingv1.ValueType_VALUE_TYPE_JSON},
				{Name: "spec__template__spec__containers", Type: kublingv1.ValueType_VALUE_TYPE_JSON},
			},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("deployment-a"),
				stringValue("team-a"),
				jsonProviderValue(`{"app":"demo"}`),
				integerProviderValue(2),
				jsonProviderValue(`{"app":"demo"}`),
				jsonProviderValue(`{"app":"demo"}`),
				jsonProviderValue(`[{"name":"app","image":"example/app:v1"}]`),
			}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = %v, %v", inserted, err)
	}

	resource := client.Resource(deploymentResource()).Namespace("team-a")
	deployment, err := resource.Get(ctx, "deployment-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after insert error = %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas"); replicas != 2 {
		t.Fatalf("spec.replicas = %d, want 2", replicas)
	}
	if label, _, _ := unstructured.NestedString(deployment.Object, "spec", "template", "metadata", "labels", "app"); label != "demo" {
		t.Fatalf("spec.template.metadata.labels.app = %q, want demo", label)
	}

	updated, err := connection.Update(ctx, &providerv1.UpdateRequest{
		Entity: entity,
		Assignments: []*providerv1.Assignment{
			{
				Field: "spec__replicas",
				Value: literalExpression(integerProviderValue(3)),
			},
			{
				Field: "spec__template__metadata__labels",
				Value: literalExpression(jsonProviderValue(`{"app":"updated"}`)),
			},
		},
		Filter: mutationIdentity("deployment-a", "team-a"),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %v, %v", updated, err)
	}

	deployment, err = resource.Get(ctx, "deployment-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas"); replicas != 3 {
		t.Fatalf("spec.replicas = %d, want 3", replicas)
	}
	if label, _, _ := unstructured.NestedString(deployment.Object, "spec", "template", "metadata", "labels", "app"); label != "updated" {
		t.Fatalf("spec.template.metadata.labels.app = %q, want updated", label)
	}
}

func TestKubernetesUpdateFallsBackToResourceUpdate(t *testing.T) {
	existing := testPod("pod-a", "team-a", "Pending")
	existing.SetResourceVersion("7")
	connection, client := mutationTestConnection(
		t,
		metav1.Verbs{"get", "list", "update"},
		&existing,
	)

	response, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "v1"},
		Assignments: []*providerv1.Assignment{
			{
				Field: "metadata",
				Value: literalExpression(jsonProviderValue(`{"labels":{"environment":"test"}}`)),
			},
			{
				Field: "spec",
				Value: literalExpression(jsonProviderValue(`{"nodeName":"node-c"}`)),
			},
		},
		Filter: mutationIdentity("pod-a", "team-a"),
	})
	if err != nil || response.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %v, %v", response, err)
	}
	pod, err := client.Resource(podResource()).Namespace("team-a").Get(
		context.Background(),
		"pod-a",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if nodeName, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName"); nodeName != "node-c" {
		t.Fatalf("spec.nodeName = %q, want node-c", nodeName)
	}
	if label, _, _ := unstructured.NestedString(pod.Object, "metadata", "labels", "environment"); label != "test" {
		t.Fatalf("metadata.labels.environment = %q, want test", label)
	}
}

func TestKubernetesObjectAssignmentUsesResourceUpdate(t *testing.T) {
	existing := testPod("pod-a", "team-a", "Pending")
	existing.SetResourceVersion("7")
	connection, client := mutationTestConnection(
		t,
		metav1.Verbs{"get", "list", "patch", "update"},
		&existing,
	)

	response, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "v1"},
		Assignments: []*providerv1.Assignment{{
			Field: "object",
			Value: literalExpression(jsonProviderValue(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"name":"pod-a","namespace":"team-a","resourceVersion":"7"},
				"spec":{"nodeName":"node-d"}
			}`)),
		}},
		Filter: mutationIdentity("pod-a", "team-a"),
	})
	if err != nil || response.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %v, %v", response, err)
	}
	actions := client.Actions()
	if len(actions) == 0 || actions[len(actions)-1].GetVerb() != "update" {
		t.Fatalf("dynamic actions = %v, want final update", actions)
	}
}

func TestKubernetesMutationsValidateResourceVerbsAndIdentity(t *testing.T) {
	connection, _ := mutationTestConnection(t, metav1.Verbs{"get", "list"})
	entity := &providerv1.EntityReference{Name: "POD", Namespace: "v1"}

	_, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: entity,
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "object"}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{jsonProviderValue(`{"metadata":{"name":"pod-a","namespace":"team-a"}}`)}}},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Insert() code = %v, want FailedPrecondition; error = %v", status.Code(err), err)
	}

	_, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: entity,
		Filter: equalExpression("metadata__namespace", "team-a"),
		Assignments: []*providerv1.Assignment{{
			Field: "spec",
			Value: literalExpression(jsonProviderValue(`{}`)),
		}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Update() code = %v, want FailedPrecondition; error = %v", status.Code(err), err)
	}

	connection, _ = mutationTestConnection(t, metav1.Verbs{"get", "list", "update"})
	_, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: entity,
		Filter: equalExpression("metadata__namespace", "team-a"),
		Assignments: []*providerv1.Assignment{{
			Field: "spec",
			Value: literalExpression(jsonProviderValue(`{}`)),
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Update() without name code = %v, want InvalidArgument; error = %v", status.Code(err), err)
	}
}

func mutationTestConnection(
	t *testing.T,
	verbs metav1.Verbs,
	objects ...runtime.Object,
) (*Connection, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return &fakeKubernetesClient{
			discoveryClient: &staticDiscovery{resourceLists: []*metav1.APIResourceList{{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{{
					Name:       "pods",
					Kind:       "Pod",
					Namespaced: true,
					Verbs:      verbs,
				}},
			}}},
			dynamicClient:    client,
			defaultNamespace: "default",
		}, nil
	})
	return openTestConnection(t, provider), client
}

func deploymentMutationTestConnection(
	t *testing.T,
	verbs metav1.Verbs,
	objects ...runtime.Object,
) (*Connection, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
	discoveryClient := &openAPIDiscovery{
		staticDiscovery: &staticDiscovery{resourceLists: deploymentResourceLists()},
		client: &staticOpenAPIClient{paths: map[string]openapi.GroupVersion{
			"apis/apps/v1": &staticOpenAPIGroupVersion{
				contents:          []byte(testDeploymentOpenAPIV3),
				serverRelativeURL: "/openapi/v3/apis/apps/v1?hash=mutations",
			},
		}},
	}
	discoveryClient.resourceLists[0].APIResources[0].Verbs = verbs
	config, err := normalizeConfig(Config{Schema: SchemaConfig{FieldExpansionDepth: 4}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(context.Context, Config) (kubernetesClient, error) {
		return &fakeKubernetesClient{
			discoveryClient:  discoveryClient,
			dynamicClient:    client,
			defaultNamespace: "default",
		}, nil
	})
	return openTestConnection(t, provider), client
}

func mutationIdentity(name string, namespace string) *providerv1.Expression {
	return andExpression(
		equalExpression("metadata__name", name),
		equalExpression("metadata__namespace", namespace),
	)
}

func literalExpression(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{Value: value}}}
}

func jsonProviderValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: value}}
}

func integerProviderValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func podResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Version: "v1", Resource: "pods"}
}

func deploymentResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
}
