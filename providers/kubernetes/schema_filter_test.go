package kubernetes

import (
	"context"
	"errors"
	"io"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildMetadataWithSchemaFiltersBeforeAssigningTableNames(
	t *testing.T,
) {
	resourceLists := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{
					Name:       "events",
					Kind:       "Event",
					Namespaced: true,
					Verbs:      metav1.Verbs{"get", "list"},
				},
			},
		},
		{
			GroupVersion: "events.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{
					Name:       "events",
					Kind:       "Event",
					Namespaced: true,
					Verbs:      metav1.Verbs{"get", "list"},
				},
			},
		},
	}

	metadata := buildMetadataWithSchema(
		resourceLists,
		nil,
		SchemaConfig{
			Include: []string{
				"events.k8s.io/v1/events",
			},
		},
	)

	if len(metadata.GetTables()) != 1 {
		t.Fatalf(
			"tables = %d, want 1: %v",
			len(metadata.GetTables()),
			metadata.GetTables(),
		)
	}

	table := metadata.GetTables()[0]

	if table.GetName() != "EVENT" {
		t.Fatalf(
			"table name = %q, want EVENT",
			table.GetName(),
		)
	}

	if table.GetNamespace() !=
		"events.k8s.io/v1" {
		t.Fatalf(
			"table namespace = %q, want events.k8s.io/v1",
			table.GetNamespace(),
		)
	}

	if table.GetSourceName() != "events" {
		t.Fatalf(
			"table source name = %q, want events",
			table.GetSourceName(),
		)
	}

	if got :=
		metadata.GetProperties()["kubernetes.resource_count"]; got != "1" {
		t.Fatalf(
			"kubernetes.resource_count = %q, want 1",
			got,
		)
	}

	if got :=
		metadata.GetProperties()["kubernetes.group_version_count"]; got != "1" {
		t.Fatalf(
			"kubernetes.group_version_count = %q, want 1",
			got,
		)
	}
}

func TestProviderMetadataAppliesSchemaIncludeAndExclude(
	t *testing.T,
) {
	client := &fakeKubernetesClient{
		discoveryClient: &staticDiscovery{
			resourceLists: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "pods",
							Kind:       "Pod",
							Namespaced: true,
							Verbs:      metav1.Verbs{"get", "list"},
						},
						{
							Name:       "events",
							Kind:       "Event",
							Namespaced: true,
							Verbs:      metav1.Verbs{"get", "list"},
						},
						{
							Name:  "nodes",
							Kind:  "Node",
							Verbs: metav1.Verbs{"get", "list"},
						},
					},
				},
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "deployments",
							Kind:       "Deployment",
							Namespaced: true,
							Verbs:      metav1.Verbs{"get", "list"},
						},
					},
				},
			},
		},
	}

	config, err := normalizeConfig(
		Config{
			Schema: SchemaConfig{
				Include: []string{
					"v1/*",
				},
				Exclude: []string{
					"v1/events",
				},
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"normalizeConfig() error = %v",
			err,
		)
	}

	provider := newProvider(
		config,
		func(
			context.Context,
			Config,
		) (kubernetesClient, error) {
			return client, nil
		},
	)

	metadata, err :=
		provider.Metadata(
			context.Background(),
		)
	if err != nil {
		t.Fatalf(
			"Metadata() error = %v",
			err,
		)
	}

	if len(metadata.GetTables()) != 2 {
		t.Fatalf(
			"tables = %d, want 2: %v",
			len(metadata.GetTables()),
			metadata.GetTables(),
		)
	}

	if metadata.GetTables()[0].GetName() !=
		"NODE" {
		t.Fatalf(
			"table 0 = %q, want NODE",
			metadata.GetTables()[0].GetName(),
		)
	}

	if metadata.GetTables()[1].GetName() !=
		"POD" {
		t.Fatalf(
			"table 1 = %q, want POD",
			metadata.GetTables()[1].GetName(),
		)
	}

	if got :=
		metadata.GetProperties()["kubernetes.resource_count"]; got != "2" {
		t.Fatalf(
			"kubernetes.resource_count = %q, want 2",
			got,
		)
	}

	if client.closes.Load() != 1 {
		t.Fatalf(
			"Close() calls = %d, want 1",
			client.closes.Load(),
		)
	}
}

func TestQueryCannotResolveResourceExcludedBySchema(
	t *testing.T,
) {
	state := &fakeDynamicState{}

	connection, client :=
		queryTestConnection(
			t,
			Config{
				Schema: SchemaConfig{
					Include: []string{
						"v1/nodes",
					},
				},
			},
			state,
			"default",
		)

	_, err :=
		connection.Query(
			context.Background(),
			&providerv1.QueryRequest{
				Entity: &providerv1.EntityReference{
					Name:      "POD",
					Namespace: "v1",
				},
			},
		)

	if status.Code(err) != codes.NotFound {
		t.Fatalf(
			"Query() code = %v, want NotFound; error = %v",
			status.Code(err),
			err,
		)
	}

	if len(state.resources) != 0 {
		t.Fatalf(
			"dynamic resources = %v, want none",
			state.resources,
		)
	}

	if err :=
		connection.Close(
			context.Background(),
		); err != nil {
		t.Fatalf(
			"connection Close() error = %v",
			err,
		)
	}

	if client.closes.Load() != 1 {
		t.Fatalf(
			"client Close() calls = %d, want 1",
			client.closes.Load(),
		)
	}
}

func TestQueryCanResolveResourceIncludedBySchema(
	t *testing.T,
) {
	state := &fakeDynamicState{
		lists: []*unstructured.UnstructuredList{
			{
				Items: []unstructured.Unstructured{
					testPod(
						"pod-a",
						"team-a",
						"Running",
					),
				},
			},
		},
	}

	connection, _ :=
		queryTestConnection(
			t,
			Config{
				Schema: SchemaConfig{
					Include: []string{
						"v1/pods",
					},
				},
			},
			state,
			"team-a",
		)

	stream, err :=
		connection.Query(
			context.Background(),
			&providerv1.QueryRequest{
				Entity: &providerv1.EntityReference{
					Name:      "POD",
					Namespace: "v1",
				},
				Projections: []*providerv1.Projection{
					fieldProjection(
						"metadata__name",
						"",
					),
				},
			},
		)

	if err != nil {
		t.Fatalf(
			"Query() error = %v",
			err,
		)
	}

	batch, err :=
		stream.Next(
			context.Background(),
		)
	if err != nil {
		t.Fatalf(
			"Next() error = %v",
			err,
		)
	}

	if len(batch.GetTuples()) != 1 {
		t.Fatalf(
			"tuples = %d, want 1",
			len(batch.GetTuples()),
		)
	}

	if got :=
		batch.GetTuples()[0].
			GetValues()[0].
			GetStringValue(); got != "pod-a" {
		t.Fatalf(
			"pod name = %q, want pod-a",
			got,
		)
	}

	_, err =
		stream.Next(
			context.Background(),
		)

	if !errors.Is(err, io.EOF) {
		t.Fatalf(
			"final Next() error = %v, want EOF",
			err,
		)
	}
}
