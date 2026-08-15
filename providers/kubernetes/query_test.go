package kubernetes

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type dynamicListCall struct {
	resource  schema.GroupVersionResource
	namespace string
	options   metav1.ListOptions
}

type fakeDynamicState struct {
	mu        sync.Mutex
	resources []schema.GroupVersionResource
	calls     []dynamicListCall
	lists     []*unstructured.UnstructuredList
	errors    []error
}

type fakeDynamicInterface struct {
	dynamic.Interface
	state *fakeDynamicState
}

func (c *fakeDynamicInterface) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	c.state.mu.Lock()
	c.state.resources = append(c.state.resources, resource)
	c.state.mu.Unlock()
	return &fakeNamespaceableResource{state: c.state, resource: resource}
}

type fakeNamespaceableResource struct {
	dynamic.NamespaceableResourceInterface
	state     *fakeDynamicState
	resource  schema.GroupVersionResource
	namespace string
}

func (r *fakeNamespaceableResource) Namespace(namespace string) dynamic.ResourceInterface {
	return &fakeNamespaceableResource{state: r.state, resource: r.resource, namespace: namespace}
}

func (r *fakeNamespaceableResource) List(
	ctx context.Context,
	options metav1.ListOptions,
) (*unstructured.UnstructuredList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.calls = append(r.state.calls, dynamicListCall{
		resource:  r.resource,
		namespace: r.namespace,
		options:   options,
	})
	index := len(r.state.calls) - 1
	if index < len(r.state.errors) && r.state.errors[index] != nil {
		return nil, r.state.errors[index]
	}
	if index >= len(r.state.lists) || r.state.lists[index] == nil {
		return &unstructured.UnstructuredList{}, nil
	}
	return r.state.lists[index].DeepCopy(), nil
}

func TestQueryStreamsProjectedResourcesUsingDefaultNamespace(t *testing.T) {
	state := &fakeDynamicState{lists: []*unstructured.UnstructuredList{{
		Items: []unstructured.Unstructured{
			testPod("pod-a", "team-a", "Running"),
			testPod("pod-b", "team-a", "Pending"),
		},
	}}}
	connection, client := queryTestConnection(t, Config{}, state, "team-a")
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "v1"},
		Projections: []*providerv1.Projection{
			fieldProjection("metadata__name", ""),
			fieldProjection("spec", "pod_spec"),
			literalProjection("cluster", "cluster-a"),
		},
		BatchSize: uint32Pointer(2),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.GetTuples()) != 2 || len(batch.GetFields()) != 3 {
		t.Fatalf("batch = %v", batch)
	}
	if batch.GetFields()[0].GetName() != "metadata__name" || batch.GetFields()[1].GetName() != "pod_spec" {
		t.Fatalf("fields = %v", batch.GetFields())
	}
	if batch.GetTuples()[0].GetValues()[0].GetStringValue() != "pod-a" ||
		batch.GetTuples()[0].GetValues()[2].GetStringValue() != "cluster-a" {
		t.Fatalf("first tuple = %v", batch.GetTuples()[0])
	}
	if !strings.Contains(batch.GetTuples()[0].GetValues()[1].GetJsonValue(), `"nodeName":"node-a"`) {
		t.Fatalf("spec JSON = %q", batch.GetTuples()[0].GetValues()[1].GetJsonValue())
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want EOF", err)
	}
	if len(state.calls) != 1 || state.calls[0].namespace != "team-a" || state.calls[0].options.Limit != 2 {
		t.Fatalf("list calls = %v", state.calls)
	}
	if len(state.resources) != 1 || state.resources[0] != (schema.GroupVersionResource{Version: "v1", Resource: "pods"}) {
		t.Fatalf("resources = %v", state.resources)
	}
	if client.closes.Load() != 0 {
		t.Fatalf("client closed while connection remains open: %d", client.closes.Load())
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("stream Close() error = %v", err)
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("connection Close() error = %v", err)
	}
	if client.closes.Load() != 1 {
		t.Fatalf("client Close() calls = %d, want 1", client.closes.Load())
	}
}

func TestQueryResolvesConfiguredProviderNamespace(t *testing.T) {
	state := &fakeDynamicState{lists: []*unstructured.UnstructuredList{{
		Items: []unstructured.Unstructured{testPod("pod-a", "team-a", "Running")},
	}}}
	connection, _ := queryTestConnection(t, Config{
		Namespace: "kubernetes-production",
		NamespaceColumn: NamespaceColumnConfig{
			Enabled:             true,
			IncludeInStableKeys: true,
		},
	}, state, "team-a")

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "kubernetes-production"},
		Projections: []*providerv1.Projection{
			fieldProjection("metadata__name", ""),
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.GetTuples()) != 1 || batch.GetTuples()[0].GetValues()[0].GetStringValue() != "pod-a" {
		t.Fatalf("batch = %v", batch)
	}
}

func TestQueryUsesNamespaceNameFilterAndContinuation(t *testing.T) {
	first := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testPod("pod-a", "team-b", "Running")}}
	first.SetContinue("next-page")
	second := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testPod("pod-b", "team-b", "Running")}}
	state := &fakeDynamicState{lists: []*unstructured.UnstructuredList{first, second}}
	connection, _ := queryTestConnection(t, Config{BlankNamespaceStrategy: BlankNamespaceAll}, state, "ignored")
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "v1"},
		Filter: andExpression(
			equalExpression("metadata__namespace", "team-b"),
			equalExpression("metadata__name", "pod-a"),
		),
		Projections: []*providerv1.Projection{fieldProjection("metadata__name", "")},
		BatchSize:   uint32Pointer(1),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	for index := 0; index < 2; index++ {
		batch, err := stream.Next(context.Background())
		if err != nil || len(batch.GetTuples()) != 1 {
			t.Fatalf("Next(%d) = %v, %v", index, batch, err)
		}
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next() error = %v, want EOF", err)
	}
	if len(state.calls) != 2 {
		t.Fatalf("list calls = %d, want 2", len(state.calls))
	}
	if state.calls[0].namespace != "team-b" || state.calls[0].options.FieldSelector != "metadata.name=pod-a" {
		t.Fatalf("first list call = %+v", state.calls[0])
	}
	if state.calls[0].options.Continue != "" || state.calls[1].options.Continue != "next-page" {
		t.Fatalf("continue tokens = %q, %q", state.calls[0].options.Continue, state.calls[1].options.Continue)
	}
}

func TestQueryLimitStopsBeforeNextKubernetesPage(t *testing.T) {
	first := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testPod("pod-a", "team-a", "Running")}}
	first.SetContinue("unused-page")
	state := &fakeDynamicState{lists: []*unstructured.UnstructuredList{first}}
	connection, _ := queryTestConnection(t, Config{BlankNamespaceStrategy: BlankNamespaceAll}, state, "default")
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "POD", Namespace: "v1"},
		Limit:  uint64Pointer(1),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next() error = %v, want EOF", err)
	}
	if len(state.calls) != 1 || state.calls[0].namespace != metav1.NamespaceAll || state.calls[0].options.Limit != 1 {
		t.Fatalf("list calls = %v", state.calls)
	}
}

func TestQueryReturnsEmptyForClusterScopedNamespaceCriterion(t *testing.T) {
	state := &fakeDynamicState{}
	connection, _ := queryTestConnection(t, Config{}, state, "default")
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "NODE", Namespace: "v1"},
		Filter: equalExpression("metadata__namespace", "team-a"),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() error = %v, want EOF", err)
	}
	if len(state.calls) != 0 {
		t.Fatalf("list calls = %d, want 0", len(state.calls))
	}
}

func TestQueryRejectsUnsupportedRequests(t *testing.T) {
	state := &fakeDynamicState{}
	connection, _ := queryTestConnection(t, Config{BlankNamespaceStrategy: BlankNamespaceFail}, state, "default")
	tests := []struct {
		name    string
		request *providerv1.QueryRequest
		code    codes.Code
	}{
		{name: "nil request", request: nil, code: codes.InvalidArgument},
		{name: "missing entity", request: &providerv1.QueryRequest{}, code: codes.InvalidArgument},
		{name: "missing namespace", request: &providerv1.QueryRequest{Entity: &providerv1.EntityReference{Name: "POD"}}, code: codes.InvalidArgument},
		{name: "unknown entity", request: queryRequest("UNKNOWN"), code: codes.NotFound},
		{name: "offset", request: withOffset(queryRequest("POD"), 1), code: codes.InvalidArgument},
		{name: "ordering", request: withOrdering(queryRequest("POD")), code: codes.InvalidArgument},
		{name: "unsupported filter", request: withFilter(queryRequest("POD"), equalExpression("kind", "Pod")), code: codes.InvalidArgument},
		{name: "missing namespace criterion", request: queryRequest("POD"), code: codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := connection.Query(context.Background(), test.request)
			if status.Code(err) != test.code {
				t.Fatalf("Query() code = %v, want %v; error = %v", status.Code(err), test.code, err)
			}
		})
	}
}

func TestQueryStreamMapsKubernetesErrorsAndCancellation(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("denied"))
	state := &fakeDynamicState{errors: []error{forbidden}}
	connection, _ := queryTestConnection(t, Config{}, state, "default")
	stream, err := connection.Query(context.Background(), queryRequest("POD"))
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	_, err = stream.Next(context.Background())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Next() code = %v, want PermissionDenied", status.Code(err))
	}
	if closeErr := stream.Close(); status.Code(closeErr) != codes.PermissionDenied {
		t.Fatalf("Close() code = %v, want PermissionDenied", status.Code(closeErr))
	}

	state = &fakeDynamicState{}
	connection, _ = queryTestConnection(t, Config{}, state, "default")
	stream, err = connection.Query(context.Background(), queryRequest("POD"))
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stream.Next(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Next() code = %v, want Canceled", status.Code(err))
	}
}

func queryTestConnection(
	t *testing.T,
	config Config,
	state *fakeDynamicState,
	defaultNamespace string,
) (*Connection, *fakeKubernetesClient) {
	t.Helper()
	normalized, err := normalizeConfig(config)
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	client := &fakeKubernetesClient{
		discoveryClient:  &staticDiscovery{resourceLists: queryResourceLists()},
		dynamicClient:    &fakeDynamicInterface{state: state},
		defaultNamespace: defaultNamespace,
	}
	provider := newProvider(normalized, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})
	return openTestConnection(t, provider), client
}

func queryResourceLists() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}},
			{Name: "nodes", Kind: "Node", Verbs: metav1.Verbs{"get", "list"}},
		},
	}}
}

func testPod(name string, namespace string, phase string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"uid":             name + "-uid",
			"name":            name,
			"namespace":       namespace,
			"resourceVersion": "7",
		},
		"spec":   map[string]any{"nodeName": "node-a"},
		"status": map[string]any{"phase": phase},
	}}
}

func queryRequest(entity string) *providerv1.QueryRequest {
	return &providerv1.QueryRequest{Entity: &providerv1.EntityReference{Name: entity, Namespace: "v1"}}
}

func withOffset(request *providerv1.QueryRequest, offset uint64) *providerv1.QueryRequest {
	request.Offset = &offset
	return request
}

func withOrdering(request *providerv1.QueryRequest) *providerv1.QueryRequest {
	request.OrderBy = []*providerv1.OrderBy{{Expression: fieldExpression("metadata__name")}}
	return request
}

func withFilter(request *providerv1.QueryRequest, filter *providerv1.Expression) *providerv1.QueryRequest {
	request.Filter = filter
	return request
}

func fieldProjection(name string, outputName string) *providerv1.Projection {
	return &providerv1.Projection{Expression: fieldExpression(name), OutputName: outputName}
}

func literalProjection(name string, value string) *providerv1.Projection {
	return &providerv1.Projection{
		Expression: &providerv1.Expression{Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{
			Value: stringValue(value),
		}}},
		OutputName: name,
	}
}

func fieldExpression(name string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Field{Field: &providerv1.FieldReference{Name: name}}}
}

func equalExpression(field string, value string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{Comparison: &providerv1.ComparisonExpression{
		Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
		Left:     fieldExpression(field),
		Right: &providerv1.Expression{Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{
			Value: stringValue(value),
		}}},
	}}}
}

func andExpression(expressions ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{Logical: &providerv1.LogicalExpression{
		Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
		Operands: expressions,
	}}}
}

func uint32Pointer(value uint32) *uint32 { return &value }
func uint64Pointer(value uint64) *uint64 { return &value }

var _ dynamic.Interface = (*fakeDynamicInterface)(nil)
var _ dynamic.NamespaceableResourceInterface = (*fakeNamespaceableResource)(nil)
