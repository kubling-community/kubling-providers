package kubernetes

import (
	"context"
	"errors"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

type staticDiscovery struct {
	discovery.DiscoveryInterface
	resourceLists []*metav1.APIResourceList
	err           error
}

func (d *staticDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.resourceLists, d.err
}

func TestBuildMetadataCreatesDeterministicResourceCatalog(t *testing.T) {
	metadata := buildMetadata([]*metav1.APIResourceList{
		{
			GroupVersion: "events.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "events", Kind: "Event", Namespaced: true, Verbs: metav1.Verbs{"watch", "list", "get"}},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments/status", Kind: "Deployment", Namespaced: true, Verbs: metav1.Verbs{"get"}},
				{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: metav1.Verbs{"update", "list", "get"}},
				{Name: "controllerrevisions", Kind: "ControllerRevision", Namespaced: true, Verbs: metav1.Verbs{"get"}},
			},
		},
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"list", "get"}, ShortNames: []string{"po"}, Categories: []string{"all"}},
				{Name: "nodes", Kind: "Node", Verbs: metav1.Verbs{"list", "get"}},
				{Name: "events", Kind: "Event", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}},
			},
		},
	}, nil)

	wantNames := []string{"CORE_EVENT", "DEPLOYMENT", "EVENTS_K8S_IO_EVENT", "NODE", "POD"}
	if len(metadata.GetTables()) != len(wantNames) {
		t.Fatalf("tables = %d, want %d: %v", len(metadata.GetTables()), len(wantNames), metadata.GetTables())
	}
	for index, expected := range wantNames {
		if actual := metadata.GetTables()[index].GetName(); actual != expected {
			t.Fatalf("table %d = %q, want %q", index, actual, expected)
		}
	}
	if metadata.GetProperties()["kubernetes.resource_count"] != "5" ||
		metadata.GetProperties()["kubernetes.group_version_count"] != "3" {
		t.Fatalf("schema properties = %v", metadata.GetProperties())
	}
	wantNamespaces := []string{"apps/v1", "events.k8s.io/v1", "v1"}
	for index, expected := range wantNamespaces {
		if actual := metadata.GetNamespaces()[index].GetName(); actual != expected {
			t.Fatalf("namespace %d = %q, want %q", index, actual, expected)
		}
	}

	pod := metadataTable(t, metadata, "POD")
	if pod.GetSourceName() != "pods" || pod.GetNamespace() != "v1" {
		t.Fatalf("POD source/namespace = %q/%q", pod.GetSourceName(), pod.GetNamespace())
	}
	if pod.GetUpdatable() {
		t.Fatal("POD updatable = true without mutation verbs")
	}
	if pod.GetProperties()["kubernetes.namespaced"] != "true" ||
		pod.GetProperties()["kubernetes.short_names"] != "po" ||
		pod.GetProperties()["kubernetes.categories"] != "all" {
		t.Fatalf("POD properties = %v", pod.GetProperties())
	}
	assertStablePrimaryKey(t, pod, "metadata__namespace", "metadata__name")
	assertJSONColumns(t, pod, "metadata", "spec", "status", "object")

	deployment := metadataTable(t, metadata, "DEPLOYMENT")
	if !deployment.GetUpdatable() {
		t.Fatal("DEPLOYMENT updatable = false with update verb")
	}
	if !metadataColumn(t, deployment, "object").GetUpdatable() ||
		!metadataColumn(t, deployment, "spec").GetUpdatable() ||
		metadataColumn(t, deployment, "status").GetUpdatable() {
		t.Fatalf("DEPLOYMENT mutation columns = %v", deployment.GetColumns())
	}

	node := metadataTable(t, metadata, "NODE")
	assertStablePrimaryKey(t, node, "metadata__name")
	if !metadataColumn(t, node, "metadata__namespace").GetNullable() {
		t.Fatal("cluster-scoped namespace column nullable = false")
	}
}

func TestBuildMetadataMarksPartialDiscovery(t *testing.T) {
	metadata := buildMetadata(testResourceLists(), []string{"metrics.k8s.io/v1beta1", "broken.example/v1"})
	if metadata.GetProperties()["kubernetes.discovery_partial"] != "true" {
		t.Fatalf("partial property = %q", metadata.GetProperties()["kubernetes.discovery_partial"])
	}
	if metadata.GetProperties()["kubernetes.failed_group_versions"] != "broken.example/v1,metrics.k8s.io/v1beta1" {
		t.Fatalf("failed groups = %q", metadata.GetProperties()["kubernetes.failed_group_versions"])
	}
}

func TestResourceTableMetadataDerivesMutationFlagsFromVerbs(t *testing.T) {
	tests := []struct {
		name              string
		verbs             metav1.Verbs
		tableUpdatable    bool
		identityUpdatable bool
		documentUpdatable bool
	}{
		{name: "read only", verbs: metav1.Verbs{"get", "list"}},
		{name: "create", verbs: metav1.Verbs{"get", "list", "create"}, tableUpdatable: true, identityUpdatable: true, documentUpdatable: true},
		{name: "update", verbs: metav1.Verbs{"get", "list", "update"}, tableUpdatable: true, documentUpdatable: true},
		{name: "patch", verbs: metav1.Verbs{"get", "list", "patch"}, tableUpdatable: true, documentUpdatable: true},
		{name: "delete", verbs: metav1.Verbs{"get", "list", "delete"}, tableUpdatable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := resourceTableMetadata(&resourceDescriptor{
				groupVersion: schema.GroupVersion{Version: "v1"},
				resource: metav1.APIResource{
					Name:       "pods",
					Kind:       "Pod",
					Namespaced: true,
					Verbs:      test.verbs,
				},
				tableName: "POD",
			})
			if table.GetUpdatable() != test.tableUpdatable {
				t.Fatalf("table updatable = %v, want %v", table.GetUpdatable(), test.tableUpdatable)
			}
			if metadataColumn(t, table, "metadata__name").GetUpdatable() != test.identityUpdatable {
				t.Fatalf("identity updatable = %v, want %v", metadataColumn(t, table, "metadata__name").GetUpdatable(), test.identityUpdatable)
			}
			if metadataColumn(t, table, "object").GetUpdatable() != test.documentUpdatable {
				t.Fatalf("object updatable = %v, want %v", metadataColumn(t, table, "object").GetUpdatable(), test.documentUpdatable)
			}
			if metadataColumn(t, table, "status").GetUpdatable() {
				t.Fatal("status updatable = true")
			}
		})
	}
}

func TestResourceTableMetadataProvidesInsertDefaultsFromDiscovery(t *testing.T) {
	table := resourceTableMetadata(&resourceDescriptor{
		groupVersion: schema.GroupVersion{Group: "apps", Version: "v1"},
		resource: metav1.APIResource{
			Name:       "deployments",
			Kind:       "Deployment",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list", "create"},
		},
		tableName: "DEPLOYMENT",
	})

	if got := metadataColumn(t, table, "api_version").GetDefaultExpression(); got != "'apps/v1'" {
		t.Fatalf("api_version default = %q, want %q", got, "'apps/v1'")
	}
	if got := metadataColumn(t, table, "kind").GetDefaultExpression(); got != "'Deployment'" {
		t.Fatalf("kind default = %q, want %q", got, "'Deployment'")
	}
	for _, name := range []string{"metadata", "object"} {
		if got := metadataColumn(t, table, name).GetDefaultExpression(); got != "jsonParse('{}', true)" {
			t.Fatalf("%s default = %q", name, got)
		}
	}
	if got := metadataColumn(t, table, "metadata__name").GetDefaultExpression(); got != "" {
		t.Fatalf("metadata__name default = %q, want empty", got)
	}
}

func TestNamespaceInsertDefaultFollowsBlankNamespaceStrategy(t *testing.T) {
	build := func() *providerv1.SchemaMetadata {
		return buildMetadata([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{{
				Name:       "pods",
				Kind:       "Pod",
				Namespaced: true,
				Verbs:      metav1.Verbs{"get", "list", "create"},
			}},
		}}, nil)
	}

	metadata := build()
	applyNamespaceInsertDefaults(metadata, BlankNamespaceDefault, "team-a")
	if got := metadataColumn(t, metadataTable(t, metadata, "POD"), "metadata__namespace").GetDefaultExpression(); got != "'team-a'" {
		t.Fatalf("DEFAULT namespace expression = %q", got)
	}

	for _, strategy := range []BlankNamespaceStrategy{BlankNamespaceAll, BlankNamespaceFail} {
		metadata = build()
		applyNamespaceInsertDefaults(metadata, strategy, "team-a")
		if got := metadataColumn(t, metadataTable(t, metadata, "POD"), "metadata__namespace").GetDefaultExpression(); got != "" {
			t.Fatalf("%s namespace expression = %q, want empty", strategy, got)
		}
	}
}

func TestLogicalIdentifierPreservesAcronymBoundaries(t *testing.T) {
	tests := map[string]string{
		"APIService":        "API_SERVICE",
		"PodSecurityPolicy": "POD_SECURITY_POLICY",
		"events.k8s.io":     "EVENTS_K8S_IO",
		"resource-claims":   "RESOURCE_CLAIMS",
	}
	for input, expected := range tests {
		if actual := logicalIdentifier(input); actual != expected {
			t.Fatalf("logicalIdentifier(%q) = %q, want %q", input, actual, expected)
		}
	}
}

func TestProviderMetadataAcceptsPartialGroupDiscovery(t *testing.T) {
	client := &fakeKubernetesClient{
		discoveryClient: &staticDiscovery{
			resourceLists: testResourceLists(),
			err: &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
				{Group: "metrics.k8s.io", Version: "v1beta1"}: errors.New("unavailable"),
			}},
		},
	}
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.GetTables()) != 1 || metadata.GetProperties()["kubernetes.discovery_partial"] != "true" {
		t.Fatalf("Metadata() = %v", metadata)
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
	}
}

func TestProviderMetadataErrors(t *testing.T) {
	tests := []struct {
		name      string
		discovery discovery.DiscoveryInterface
		closeErr  error
		code      codes.Code
	}{
		{
			name:      "nil discovery",
			discovery: nil,
			code:      codes.Internal,
		},
		{
			name:      "discovery failure",
			discovery: &staticDiscovery{err: errors.New("forbidden")},
			code:      codes.Unavailable,
		},
		{
			name:      "no listable resources",
			discovery: &staticDiscovery{resourceLists: []*metav1.APIResourceList{{GroupVersion: "v1"}}},
			code:      codes.FailedPrecondition,
		},
		{
			name:      "close failure",
			discovery: &staticDiscovery{resourceLists: testResourceLists()},
			closeErr:  errors.New("close failed"),
			code:      codes.Unavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeKubernetesClient{discoveryClient: test.discovery, closeErr: test.closeErr}
			provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
				return client, nil
			})
			_, err := provider.Metadata(context.Background())
			if status.Code(err) != test.code {
				t.Fatalf("Metadata() code = %v, want %v; error = %v", status.Code(err), test.code, err)
			}
			if client.closes.Load() != 1 {
				t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
			}
		})
	}
}

func TestProviderMetadataPreservesCanceledContext(t *testing.T) {
	client := &fakeKubernetesClient{discoveryClient: &staticDiscovery{resourceLists: testResourceLists()}}
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return client, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := provider.Metadata(ctx)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("Metadata() code = %v, want Canceled", status.Code(err))
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
	}
}

func testResourceLists() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{
			Name:       "pods",
			Kind:       "Pod",
			Namespaced: true,
			Verbs:      metav1.Verbs{"get", "list"},
		}},
	}}
}

func metadataTable(t *testing.T, metadata *providerv1.SchemaMetadata, name string) *providerv1.TableMetadata {
	t.Helper()
	for _, table := range metadata.GetTables() {
		if table.GetName() == name {
			return table
		}
	}
	t.Fatalf("table %q not found", name)
	return nil
}

func metadataColumn(t *testing.T, table *providerv1.TableMetadata, name string) *providerv1.ColumnMetadata {
	t.Helper()
	for _, column := range table.GetColumns() {
		if column.GetName() == name {
			return column
		}
	}
	t.Fatalf("column %q not found in %q", name, table.GetName())
	return nil
}

func assertStablePrimaryKey(
	t *testing.T,
	table *providerv1.TableMetadata,
	components ...string,
) {
	t.Helper()
	identifier := metadataColumn(t, table, "identifier")
	stableKey := identifier.GetStableKey()
	if stableKey == nil {
		t.Fatalf("%s identifier stable key is nil", table.GetName())
	}
	if stableKey.GetFormat() != providerv1.StableKeyFormat_STABLE_KEY_FORMAT_VAL_PK_V1 {
		t.Fatalf("%s stable key format = %v", table.GetName(), stableKey.GetFormat())
	}
	if got := stableKey.GetColumns(); len(got) != len(components) {
		t.Fatalf("%s stable key columns = %v, want %v", table.GetName(), got, components)
	} else {
		for index := range components {
			if got[index] != components[index] {
				t.Fatalf("%s stable key columns = %v, want %v", table.GetName(), got, components)
			}
		}
	}
	if len(table.GetKeys()) < 2 {
		t.Fatalf("%s keys = %v, want generated primary and source unique keys", table.GetName(), table.GetKeys())
	}
	primary := table.GetKeys()[0]
	if primary.GetKind() != providerv1.KeyKind_KEY_KIND_PRIMARY ||
		len(primary.GetColumns()) != 1 ||
		primary.GetColumns()[0] != "identifier" {
		t.Fatalf("%s primary key = %v", table.GetName(), primary)
	}
	sourceIdentity := table.GetKeys()[1]
	if sourceIdentity.GetKind() != providerv1.KeyKind_KEY_KIND_UNIQUE {
		t.Fatalf("%s source identity key = %v", table.GetName(), sourceIdentity)
	}
	if got := sourceIdentity.GetColumns(); len(got) != len(components) {
		t.Fatalf("%s source identity columns = %v, want %v", table.GetName(), got, components)
	} else {
		for index := range components {
			if got[index] != components[index] {
				t.Fatalf("%s source identity columns = %v, want %v", table.GetName(), got, components)
			}
		}
	}
}

func assertJSONColumns(t *testing.T, table *providerv1.TableMetadata, names ...string) {
	t.Helper()
	for _, name := range names {
		column := metadataColumn(t, table, name)
		if column.GetType() != kublingv1.ValueType_VALUE_TYPE_JSON {
			t.Fatalf("column %q type = %v, want JSON", name, column.GetType())
		}
		if column.GetSearchability() != providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE {
			t.Fatalf("column %q searchability = %v, want unsearchable", name, column.GetSearchability())
		}
	}
}
