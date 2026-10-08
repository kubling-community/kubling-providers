package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/semanticmodel"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"gopkg.in/yaml.v3"
)

const hostSemanticFragmentDigest = "sha256:b06c9d85ecfe9cf772764e7538468ff370e2203cde97f90941f01817ef3f825a"

type hostSemanticDocument struct {
	Format        string `yaml:"format"`
	SchemaVersion int    `yaml:"schemaVersion"`
	Kind          string `yaml:"kind"`
	Metadata      struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
		Version   string `yaml:"version"`
	} `yaml:"metadata"`
	Spec struct {
		Entities      []hostSemanticEntity       `yaml:"entities"`
		Relationships []hostSemanticRelationship `yaml:"relationships"`
	} `yaml:"spec"`
}

type hostSemanticEntity struct {
	ID      string `yaml:"id"`
	Binding struct {
		Relation string `yaml:"relation"`
	} `yaml:"binding"`
	Properties []hostSemanticProperty `yaml:"properties"`
	Identities []struct {
		ID         string   `yaml:"id"`
		Properties []string `yaml:"properties"`
	} `yaml:"identities"`
}

type hostSemanticProperty struct {
	ID      string `yaml:"id"`
	Type    string `yaml:"type"`
	Binding struct {
		Field string `yaml:"field"`
	} `yaml:"binding"`
}

type hostSemanticRelationship struct {
	ID          string `yaml:"id"`
	From        string `yaml:"from"`
	To          string `yaml:"to"`
	Cardinality string `yaml:"cardinality"`
	Joins       []struct {
		Left  string `yaml:"left"`
		Right string `yaml:"right"`
	} `yaml:"joins"`
}

type semanticReadiness struct {
	calls atomic.Int32
}

func (s *semanticReadiness) Ready(context.Context) error {
	s.calls.Add(1)
	return nil
}

type semanticQueryExecutor struct {
	calls atomic.Int32
}

func (s *semanticQueryExecutor) Query(
	context.Context,
	*providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	s.calls.Add(1)
	return nil, fmt.Errorf("query is not expected")
}

func TestHostSemanticFragmentIsGeneratedDeterministicAndConnectionAgnostic(t *testing.T) {
	generated, err := semanticmodel.Generate()
	if err != nil {
		t.Fatalf("semanticmodel.Generate() error = %v", err)
	}
	if !bytes.Equal(generated, hostSemanticFragmentDocument) {
		t.Fatal("embedded host semantic fragment is stale; run go generate .")
	}

	readiness := &semanticReadiness{}
	queryExecutor := &semanticQueryExecutor{}
	provider, err := New(readiness, queryExecutor, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server := providersdk.NewServer(provider)

	first, err := server.GetSemanticFragment(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	second, err := server.GetSemanticFragment(context.Background(), nil)
	if err != nil {
		t.Fatalf("second GetSemanticFragment() error = %v", err)
	}
	if readiness.calls.Load() != 0 || queryExecutor.calls.Load() != 0 {
		t.Fatalf(
			"semantic retrieval touched runtime dependencies: readiness=%d query=%d",
			readiness.calls.Load(),
			queryExecutor.calls.Load(),
		)
	}

	firstFragment := first.GetFragment()
	secondFragment := second.GetFragment()
	if firstFragment == nil || secondFragment == nil {
		t.Fatalf("semantic fragments = %v, %v", firstFragment, secondFragment)
	}
	if !bytes.Equal(firstFragment.GetDocument(), secondFragment.GetDocument()) {
		t.Fatal("semantic fragment bytes are not deterministic")
	}
	if firstFragment.GetMediaType() != providersdk.SemanticFragmentMediaTypeYAML ||
		firstFragment.GetVersion() != hostSemanticFragmentVersion {
		t.Fatalf("semantic fragment envelope = %v", firstFragment)
	}
	wantDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(firstFragment.GetDocument()))
	if firstFragment.GetDigest() != wantDigest || wantDigest != hostSemanticFragmentDigest {
		t.Fatalf(
			"digest = %q, computed %q, pinned %q",
			firstFragment.GetDigest(),
			wantDigest,
			hostSemanticFragmentDigest,
		)
	}
}

func TestHostSemanticFragmentMatchesCompleteMetadata(t *testing.T) {
	var document hostSemanticDocument
	if err := yaml.Unmarshal(hostSemanticFragmentDocument, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if document.Format != "kubling-semantic" || document.SchemaVersion != 1 ||
		document.Kind != "fragment" {
		t.Fatalf("document envelope = %#v", document)
	}
	if document.Metadata.Name != semanticmodel.Name ||
		document.Metadata.Namespace != semanticmodel.Namespace ||
		document.Metadata.Version != hostSemanticFragmentVersion {
		t.Fatalf("document metadata = %#v", document.Metadata)
	}
	if hostSemanticFragmentVersion != semanticmodel.Version {
		t.Fatalf(
			"embedded semantic version = %q, generator version = %q",
			hostSemanticFragmentVersion,
			semanticmodel.Version,
		)
	}

	metadata := baseMetadata(Config{})
	tables := make(map[string]*providerv1.TableMetadata, len(metadata.GetTables()))
	for _, table := range metadata.GetTables() {
		tables[table.GetName()] = table
	}
	if len(document.Spec.Entities) != len(tables) {
		t.Fatalf("semantic entities = %d, metadata tables = %d", len(document.Spec.Entities), len(tables))
	}

	entities := make(map[string]hostSemanticEntity, len(document.Spec.Entities))
	properties := make(map[string]map[string]hostSemanticProperty, len(document.Spec.Entities))
	for _, entity := range document.Spec.Entities {
		if _, duplicate := entities[entity.ID]; duplicate {
			t.Fatalf("duplicate semantic entity %q", entity.ID)
		}
		if strings.Contains(entity.Binding.Relation, ".") {
			t.Fatalf("entity %s relation %q is qualified", entity.ID, entity.Binding.Relation)
		}
		table := tables[entity.Binding.Relation]
		if table == nil {
			t.Fatalf("entity %s binds unknown table %q", entity.ID, entity.Binding.Relation)
		}
		fields := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
		for _, column := range table.GetColumns() {
			if column.GetName() != hostschema.IdentifierColumn {
				fields[column.GetName()] = column
			}
		}
		if len(entity.Properties) != len(fields) {
			t.Fatalf(
				"entity %s properties = %d, source fields = %d",
				entity.ID,
				len(entity.Properties),
				len(fields),
			)
		}
		propertyIndex := make(map[string]hostSemanticProperty, len(entity.Properties))
		fieldToProperty := make(map[string]string, len(entity.Properties))
		for _, property := range entity.Properties {
			if strings.Contains(property.Binding.Field, ".") {
				t.Fatalf("property %s.%s field %q is qualified", entity.ID, property.ID, property.Binding.Field)
			}
			column := fields[property.Binding.Field]
			if column == nil {
				t.Fatalf(
					"property %s.%s binds unknown field %q",
					entity.ID,
					property.ID,
					property.Binding.Field,
				)
			}
			if property.Type != hostSemanticType(t, column) {
				t.Fatalf(
					"property %s.%s type = %q, want %q",
					entity.ID,
					property.ID,
					property.Type,
					hostSemanticType(t, column),
				)
			}
			if _, duplicate := propertyIndex[property.ID]; duplicate {
				t.Fatalf("entity %s has duplicate property %q", entity.ID, property.ID)
			}
			propertyIndex[property.ID] = property
			fieldToProperty[property.Binding.Field] = property.ID
		}
		if len(entity.Identities) != 1 {
			t.Fatalf("entity %s identities = %v, want one", entity.ID, entity.Identities)
		}
		identifier := hostMetadataColumn(t, table, hostschema.IdentifierColumn)
		wantIdentity := make([]string, 0, len(identifier.GetStableKey().GetColumns()))
		for _, field := range identifier.GetStableKey().GetColumns() {
			wantIdentity = append(wantIdentity, fieldToProperty[field])
		}
		if !slices.Equal(entity.Identities[0].Properties, wantIdentity) {
			t.Fatalf(
				"entity %s identity = %v, want %v",
				entity.ID,
				entity.Identities[0].Properties,
				wantIdentity,
			)
		}
		entities[entity.ID] = entity
		properties[entity.ID] = propertyIndex
		delete(tables, entity.Binding.Relation)
	}
	if len(tables) != 0 {
		t.Fatalf("metadata tables without semantic entities = %v", tables)
	}

	relationships := make(map[string]hostSemanticRelationship, len(document.Spec.Relationships))
	executable := 0
	for _, relationship := range document.Spec.Relationships {
		if _, duplicate := relationships[relationship.ID]; duplicate {
			t.Fatalf("duplicate semantic relationship %q", relationship.ID)
		}
		if _, exists := entities[relationship.From]; !exists {
			t.Fatalf("relationship %s source %q is missing", relationship.ID, relationship.From)
		}
		if _, exists := entities[relationship.To]; !exists {
			t.Fatalf("relationship %s target %q is missing", relationship.ID, relationship.To)
		}
		for _, join := range relationship.Joins {
			left := hostSemanticJoinProperty(t, properties, relationship.From, join.Left)
			right := hostSemanticJoinProperty(t, properties, relationship.To, join.Right)
			if left.Type != right.Type {
				t.Fatalf(
					"relationship %s joins incompatible types %s and %s",
					relationship.ID,
					left.Type,
					right.Type,
				)
			}
			if left.ID == "hostname" || right.ID == "hostname" {
				t.Fatalf("relationship %s uses mutable hostname as a join key", relationship.ID)
			}
		}
		if len(relationship.Joins) > 0 {
			executable++
		}
		relationships[relationship.ID] = relationship
	}
	if len(relationships) != 11 || executable != 9 {
		t.Fatalf("relationships = %d, executable = %d; want 11 and 9", len(relationships), executable)
	}
	for _, semanticOnly := range []string{
		"CPUInfoCorrespondsToCPUTimes",
		"ProcessHasParentProcess",
	} {
		if len(relationships[semanticOnly].Joins) != 0 {
			t.Fatalf("relationship %s must remain semantic-only", semanticOnly)
		}
	}
	if len(relationships["NetworkInterfaceHasIO"].Joins) != 3 {
		t.Fatal("NetworkInterfaceHasIO must join namespace, host ID and interface name")
	}
}

func TestHostSemanticFragmentPreservesCanceledContext(t *testing.T) {
	provider, err := New(&semanticReadiness{}, &semanticQueryExecutor{}, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.SemanticFragment(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("SemanticFragment() code = %v, want Canceled", status.Code(err))
	}
}

func TestHostSemanticFragmentGRPCIntegration(t *testing.T) {
	provider, err := New(&semanticReadiness{}, &semanticQueryExecutor{}, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, providersdk.NewServer(provider))
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serveErrors; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("grpcServer.Serve() error = %v", serveErr)
		}
	})

	connection, err := grpc.NewClient(
		"passthrough:///host-semantic-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
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
	if response.GetFragment() == nil ||
		!bytes.Equal(response.GetFragment().GetDocument(), hostSemanticFragmentDocument) {
		t.Fatalf("GetSemanticFragment() response = %v", response)
	}
}

func hostMetadataColumn(
	t *testing.T,
	table *providerv1.TableMetadata,
	name string,
) *providerv1.ColumnMetadata {
	t.Helper()
	for _, column := range table.GetColumns() {
		if column.GetName() == name {
			return column
		}
	}
	t.Fatalf("table %s has no column %q", table.GetName(), name)
	return nil
}

func hostSemanticType(t *testing.T, column *providerv1.ColumnMetadata) string {
	t.Helper()
	if column.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
		if column.GetTypeDescriptor().GetElementType().GetType() !=
			kublingv1.ValueType_VALUE_TYPE_STRING {
			t.Fatalf("column %s has unsupported array descriptor %v", column.GetName(), column.GetTypeDescriptor())
		}
		return "string[]"
	}
	types := map[kublingv1.ValueType]string{
		kublingv1.ValueType_VALUE_TYPE_STRING:     "string",
		kublingv1.ValueType_VALUE_TYPE_INTEGER:    "integer",
		kublingv1.ValueType_VALUE_TYPE_LONG:       "long",
		kublingv1.ValueType_VALUE_TYPE_BIGINTEGER: "biginteger",
		kublingv1.ValueType_VALUE_TYPE_DOUBLE:     "double",
		kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:  "timestamp",
	}
	semanticType, exists := types[column.GetType()]
	if !exists {
		t.Fatalf("column %s has unsupported semantic type %s", column.GetName(), column.GetType())
	}
	return semanticType
}

func hostSemanticJoinProperty(
	t *testing.T,
	properties map[string]map[string]hostSemanticProperty,
	expectedEntity string,
	reference string,
) hostSemanticProperty {
	t.Helper()
	parts := strings.Split(reference, ".")
	if len(parts) != 2 || parts[0] != expectedEntity {
		t.Fatalf("invalid semantic property reference %q; want %s.<property>", reference, expectedEntity)
	}
	property, exists := properties[expectedEntity][parts[1]]
	if !exists {
		t.Fatalf("semantic property reference %q is missing", reference)
	}
	return property
}
