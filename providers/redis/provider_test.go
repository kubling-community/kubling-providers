package redis

import (
	"context"
	"errors"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProviderCapabilitiesAndMetadata(t *testing.T) {
	provider := testProvider(t, map[string]*fakeRedisClient{testNamespace: newFakeRedisClient()})

	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() {
		t.Fatal("Capabilities() transactions supported = true, want false")
	}
	if capabilities.GetQuery().GetRequiresCriteria() ||
		!capabilities.GetQuery().GetOrdering().GetExplicitNullOrdering() ||
		len(capabilities.GetQuery().GetExpressions().GetPatternOperators()) != 1 {
		t.Fatalf("Capabilities() query = %v", capabilities.GetQuery())
	}
	if !capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		!capabilities.GetMutations().GetDelete() {
		t.Fatalf("Capabilities() mutations = %v", capabilities.GetMutations())
	}

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.GetNamespaces()) != 1 || metadata.GetNamespaces()[0].GetName() != testNamespace {
		t.Fatalf("Metadata() namespaces = %v", metadata.GetNamespaces())
	}
	if len(metadata.GetTables()) != 1 || metadata.GetTables()[0].GetName() != "TASK" {
		t.Fatalf("Metadata() tables = %v", metadata.GetTables())
	}
	task := metadata.GetTables()[0]
	if len(task.GetColumns()) != 5 || task.GetKeys()[0].GetColumns()[0] != "id" {
		t.Fatalf("Metadata() TASK = %v", task)
	}
	if !task.GetUpdatable() || !task.GetColumns()[0].GetUpdatable() {
		t.Fatalf("Metadata() mutable TASK or key is not updatable: %v", task)
	}
	exampleConfig, err := LoadConfig("config.example.yaml")
	if err != nil {
		t.Fatalf("LoadConfig(config.example.yaml) error = %v", err)
	}
	exampleMetadata := buildMetadata(exampleConfig)
	for _, table := range exampleMetadata.GetTables() {
		if table.GetName() != "PROJECT" {
			continue
		}
		for _, column := range table.GetColumns() {
			if column.GetUpdatable() {
				t.Fatalf("read-only PROJECT column %q is updatable", column.GetName())
			}
		}
	}
	if strings.Contains(metadata.String(), "127.0.0.1") {
		t.Fatalf("Metadata() leaks endpoint: %v", metadata)
	}
}

func TestProviderHealthPingsAndReleasesClient(t *testing.T) {
	client := newFakeRedisClient()
	provider := testProvider(t, map[string]*fakeRedisClient{testNamespace: client})

	health, err := provider.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if !health.GetHealthy() || client.closeCount.Load() != 1 {
		t.Fatalf("Health() = %v, close count = %d", health, client.closeCount.Load())
	}
}

func TestProviderSharesClientUntilLastConnectionCloses(t *testing.T) {
	client := newFakeRedisClient()
	provider := testProvider(t, map[string]*fakeRedisClient{testNamespace: client})
	first, _ := provider.Open(context.Background())
	second, _ := provider.Open(context.Background())

	if _, err := first.(*Connection).client(context.Background(), testNamespace); err != nil {
		t.Fatalf("first client() error = %v", err)
	}
	if _, err := second.(*Connection).client(context.Background(), testNamespace); err != nil {
		t.Fatalf("second client() error = %v", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if client.closeCount.Load() != 0 {
		t.Fatalf("close count after first connection = %d, want 0", client.closeCount.Load())
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if client.closeCount.Load() != 1 {
		t.Fatalf("close count after last connection = %d, want 1", client.closeCount.Load())
	}
	if err := second.Close(context.Background()); err != nil || client.closeCount.Load() != 1 {
		t.Fatalf("repeated Close() = %v, count = %d", err, client.closeCount.Load())
	}
}

func TestConnectionCloseJoinsClientErrors(t *testing.T) {
	firstErr := errors.New("first close")
	secondErr := errors.New("second close")
	firstClient := newFakeRedisClient()
	firstClient.closeErr = firstErr
	secondClient := newFakeRedisClient()
	secondClient.closeErr = secondErr

	config, err := normalizeConfig(Config{Namespaces: map[string]NamespaceConfig{
		"first":  testNamespaceConfig("first-address"),
		"second": testNamespaceConfig("second-address"),
	}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	provider := newProvider(config, func(config NamespaceConfig) redisClient {
		if config.Address == "first-address" {
			return firstClient
		}
		return secondClient
	})
	opened, _ := provider.Open(context.Background())
	connection := opened.(*Connection)
	_, _ = connection.client(context.Background(), "first")
	_, _ = connection.client(context.Background(), "second")

	closeErr := connection.Close(context.Background())
	if !errors.Is(closeErr, firstErr) || !errors.Is(closeErr, secondErr) {
		t.Fatalf("Close() error = %v, want both causes", closeErr)
	}
}

func TestClosedConnectionRejectsOperationsAndTransactionsAreUnsupported(t *testing.T) {
	connection := testConnection(t, newFakeRedisClient())
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := connection.Query(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Query(nil) code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := connection.Query(context.Background(), queryByID("task-1")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query() after close code = %v, want %v", status.Code(err), codes.FailedPrecondition)
	}
	if err := connection.Begin(context.Background()); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Begin() code = %v, want %v", status.Code(err), codes.Unimplemented)
	}
	if inTransaction, err := connection.InTransaction(context.Background()); inTransaction || status.Code(err) != codes.Unimplemented {
		t.Fatalf("InTransaction() = (%v, %v)", inTransaction, err)
	}
}

func testNamespaceConfig(address string) NamespaceConfig {
	return NamespaceConfig{
		Address: address,
		Tables: []TableConfig{{
			Name:      "TASK",
			Key:       ColumnConfig{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
			Fields:    []ColumnConfig{{Name: "title", Type: kublingv1.ValueType_VALUE_TYPE_STRING}},
			Updatable: true,
		}},
	}
}
