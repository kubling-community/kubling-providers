package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

const kubernetesSemanticFragmentDigest = "sha256:0f3f8025d144fff5bcd4802004a0724f9a425f92649e4e5ff9141aba8e330e5e"

type semanticTestDocument struct {
	Format        string `yaml:"format"`
	SchemaVersion int    `yaml:"schemaVersion"`
	Kind          string `yaml:"kind"`
	Metadata      struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
		Version   string `yaml:"version"`
	} `yaml:"metadata"`
	Spec struct {
		Entities      []semanticTestEntity       `yaml:"entities"`
		Relationships []semanticTestRelationship `yaml:"relationships"`
	} `yaml:"spec"`
}

type semanticTestEntity struct {
	ID      string `yaml:"id"`
	Binding struct {
		Relation string `yaml:"relation"`
	} `yaml:"binding"`
	Properties []struct {
		ID      string `yaml:"id"`
		Binding struct {
			Field string `yaml:"field"`
		} `yaml:"binding"`
	} `yaml:"properties"`
}

type semanticTestRelationship struct {
	ID    string `yaml:"id"`
	From  string `yaml:"from"`
	To    string `yaml:"to"`
	Joins []struct {
		Left  string `yaml:"left"`
		Right string `yaml:"right"`
	} `yaml:"joins"`
}

func TestProviderSemanticFragmentIsDeterministicAndConnectionAgnostic(t *testing.T) {
	var clientCreations atomic.Int32
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		clientCreations.Add(1)
		return nil, fmt.Errorf("cluster access is not expected")
	})
	server := providersdk.NewServer(provider)

	first, err := server.GetSemanticFragment(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	second, err := server.GetSemanticFragment(context.Background(), nil)
	if err != nil {
		t.Fatalf("second GetSemanticFragment() error = %v", err)
	}
	if clientCreations.Load() != 0 {
		t.Fatalf("cluster client creations = %d, want 0", clientCreations.Load())
	}

	firstFragment := first.GetFragment()
	secondFragment := second.GetFragment()
	if firstFragment == nil || secondFragment == nil {
		t.Fatalf("semantic fragments = %v, %v", firstFragment, secondFragment)
	}
	if !bytes.Equal(firstFragment.GetDocument(), secondFragment.GetDocument()) {
		t.Fatal("semantic fragment bytes are not deterministic")
	}
	if firstFragment.GetMediaType() != providersdk.SemanticFragmentMediaTypeYAML {
		t.Fatalf("media type = %q", firstFragment.GetMediaType())
	}
	if firstFragment.GetVersion() != kubernetesSemanticFragmentVersion {
		t.Fatalf("version = %q", firstFragment.GetVersion())
	}
	wantDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(firstFragment.GetDocument()))
	if firstFragment.GetDigest() != wantDigest || wantDigest != kubernetesSemanticFragmentDigest {
		t.Fatalf(
			"digest = %q, computed %q, pinned %q",
			firstFragment.GetDigest(),
			wantDigest,
			kubernetesSemanticFragmentDigest,
		)
	}
}

func TestProviderSemanticFragmentMatchesKubernetesMetadata(t *testing.T) {
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return nil, fmt.Errorf("cluster access is not expected")
	})
	fragment, err := provider.SemanticFragment(context.Background())
	if err != nil {
		t.Fatalf("SemanticFragment() error = %v", err)
	}

	var document semanticTestDocument
	if err := yaml.Unmarshal(fragment.Document, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if document.Format != "kubling-semantic" || document.SchemaVersion != 1 || document.Kind != "fragment" {
		t.Fatalf("document envelope = %#v", document)
	}
	if document.Metadata.Namespace != "k8s" || document.Metadata.Version != fragment.Version {
		t.Fatalf("document metadata = %#v", document.Metadata)
	}

	metadata := buildMetadata(kubernetesWorkloadResourceLists(), nil)
	entities := make(map[string]semanticTestEntity, len(document.Spec.Entities))
	for _, entity := range document.Spec.Entities {
		if strings.Contains(entity.Binding.Relation, ".") {
			t.Fatalf("entity %s relation %q is qualified", entity.ID, entity.Binding.Relation)
		}
		table := metadataTable(t, metadata, entity.Binding.Relation)
		for _, property := range entity.Properties {
			if strings.Contains(property.Binding.Field, ".") {
				t.Fatalf("property %s.%s field %q is qualified", entity.ID, property.ID, property.Binding.Field)
			}
			metadataColumn(t, table, property.Binding.Field)
		}
		entities[entity.ID] = entity
	}
	for _, expected := range []string{"Deployment", "ReplicaSet", "Pod"} {
		if _, exists := entities[expected]; !exists {
			t.Fatalf("entity %q is missing", expected)
		}
	}

	relationships := make(map[string]semanticTestRelationship, len(document.Spec.Relationships))
	for _, relationship := range document.Spec.Relationships {
		if _, exists := entities[relationship.From]; !exists {
			t.Fatalf("relationship %s source %q is missing", relationship.ID, relationship.From)
		}
		if _, exists := entities[relationship.To]; !exists {
			t.Fatalf("relationship %s target %q is missing", relationship.ID, relationship.To)
		}
		relationships[relationship.ID] = relationship
	}
	if len(relationships["DeploymentOwnsReplicaSet"].Joins) != 1 ||
		len(relationships["ReplicaSetOwnsPod"].Joins) != 1 {
		t.Fatalf("executable owner relationships = %v", relationships)
	}
	if len(relationships["DeploymentOwnsPod"].Joins) != 0 {
		t.Fatal("transitive DeploymentOwnsPod relationship must not invent a direct join")
	}
}

func TestProviderSemanticFragmentPreservesCanceledContext(t *testing.T) {
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return nil, fmt.Errorf("cluster access is not expected")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := provider.SemanticFragment(ctx)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("SemanticFragment() code = %v, want Canceled", status.Code(err))
	}
}

func TestKubernetesSemanticFragmentGRPCIntegration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	var clientCreations atomic.Int32
	implementation := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		clientCreations.Add(1)
		return nil, fmt.Errorf("cluster access is not expected")
	})
	service := providersdk.NewServer(implementation)
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if serveErr := <-serveErrors; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("grpcServer.Serve() error = %v", serveErr)
		}
	})

	connection, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer connection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := providerv1.NewProviderServiceClient(connection).GetSemanticFragment(
		ctx,
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if response.GetFragment() == nil || !bytes.Equal(
		response.GetFragment().GetDocument(),
		kubernetesSemanticFragmentDocument,
	) {
		t.Fatalf("GetSemanticFragment() response = %v", response)
	}
	if clientCreations.Load() != 0 {
		t.Fatalf("cluster client creations = %d, want 0", clientCreations.Load())
	}
}
