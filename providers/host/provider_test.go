package host

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeReadiness struct {
	err error
}

func (f *fakeReadiness) Ready(context.Context) error {
	return f.err
}

type fakeQueryExecutor struct {
	err error
}

func (f *fakeQueryExecutor) Query(
	context.Context,
	*providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	return nil, f.err
}

func TestProviderExposesConservativeSurface(t *testing.T) {
	readiness := &fakeReadiness{}
	provider, err := New(readiness, &fakeQueryExecutor{}, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() ||
		capabilities.GetQuery().GetPartialResults() ||
		capabilities.GetMutations().GetInsert() ||
		capabilities.GetMutations().GetUpdate() ||
		capabilities.GetMutations().GetDelete() {
		t.Fatalf("Capabilities() advertises unsupported behavior: %v", capabilities)
	}
	if !slices.Contains(capabilities.GetValues().GetFeatures(), grpcfeatures.ArrayValuesV1) ||
		capabilities.GetValues().GetMaxArrayDimensions() != 1 {
		t.Fatalf("Capabilities() array values = %v", capabilities.GetValues())
	}

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.GetTables()) != 9 {
		t.Fatalf("Metadata() tables = %d, want 9", len(metadata.GetTables()))
	}
	table := metadata.GetTables()[0]
	if table.GetName() != "HOST" || table.GetUpdatable() {
		t.Fatalf("Metadata() HOST table = %v", table)
	}
	if len(table.GetKeys()) != 1 ||
		strings.Join(table.GetKeys()[0].GetColumns(), ",") != "identifier" {
		t.Fatalf("Metadata() HOST key = %v", table.GetKeys())
	}
	identifier := table.GetColumns()[len(table.GetColumns())-1]
	if strings.Join(identifier.GetStableKey().GetColumns(), ",") != "namespace,host_id" {
		t.Fatalf("Metadata() HOST stable key = %v", identifier.GetStableKey())
	}
	if got := table.GetColumns()[4].GetType(); got != kublingv1.ValueType_VALUE_TYPE_TIMESTAMP {
		t.Fatalf("Metadata() enrolled_at type = %v, want TIMESTAMP", got)
	}
	if facts := metadata.GetTables()[1]; facts.GetName() != "HOST_FACTS" {
		t.Fatalf("Metadata() HOST_FACTS table = %v", facts)
	}
	if processes := metadata.GetTables()[8]; processes.GetName() != "PROCESSES" {
		t.Fatalf("Metadata() PROCESSES table = %v", processes)
	}

	health, err := provider.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if !health.GetHealthy() {
		t.Fatalf("Health() = %v, want healthy", health)
	}

	readiness.err = errors.New("agent API unavailable")
	health, err = provider.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() unavailable error = %v", err)
	}
	if health.GetHealthy() || !strings.Contains(health.GetMessage(), "agent API unavailable") {
		t.Fatalf("Health() unavailable = %v", health)
	}
}

func TestProviderConnectionRejectsUnimplementedOperations(t *testing.T) {
	queryErr := status.Error(codes.InvalidArgument, "test query")
	provider, err := New(
		&fakeReadiness{},
		&fakeQueryExecutor{err: queryErr},
		Config{},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := connection.Query(context.Background(), &providerv1.QueryRequest{}); !errors.Is(err, queryErr) {
		t.Fatalf("Query() error = %v, want delegated error", err)
	}
	if _, err := connection.Insert(context.Background(), &providerv1.InsertRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Insert() code = %v, want Unimplemented", status.Code(err))
	}
	if active, err := connection.InTransaction(context.Background()); active || status.Code(err) != codes.Unimplemented {
		t.Fatalf("InTransaction() = %v, %v; want false, Unimplemented", active, err)
	}

	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := connection.Query(context.Background(), &providerv1.QueryRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query() after Close code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestProviderRejectsMissingReadinessAndCanceledContexts(t *testing.T) {
	if _, err := New(nil, &fakeQueryExecutor{}, Config{}); err == nil {
		t.Fatal("New(nil readiness) error = nil")
	}
	if _, err := New(&fakeReadiness{}, nil, Config{}); err == nil {
		t.Fatal("New(nil query executor) error = nil")
	}
	provider, err := New(&fakeReadiness{}, &fakeQueryExecutor{}, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Health(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Health(canceled) code = %v, want Canceled", status.Code(err))
	}
	if _, err := provider.Metadata(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Metadata(canceled) code = %v, want Canceled", status.Code(err))
	}
}
