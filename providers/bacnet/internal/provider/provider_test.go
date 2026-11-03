package provider

import (
	"context"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProviderExposesConservativeFoundation(t *testing.T) {
	implementation := newTestProvider(t)
	capabilities, err := implementation.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() ||
		capabilities.GetQuery().GetPartialResults() ||
		capabilities.GetQuery().GetOrdering().GetSupported() ||
		capabilities.GetQuery().GetPagination().GetLimit() ||
		capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		capabilities.GetMutations().GetDelete() ||
		len(capabilities.GetQuery().GetExpressions().GetComparisonOperators()) != 1 ||
		len(capabilities.GetValues().GetSupportedTypes()) == 0 {
		t.Fatalf("Capabilities() advertises unsupported behavior: %v", capabilities)
	}
	inputTypes := make(map[kublingv1.ValueType]bool)
	for _, supported := range capabilities.GetValues().GetSupportedTypes() {
		inputTypes[supported.GetType()] = supported.GetInput()
	}
	for _, valueType := range []kublingv1.ValueType{
		kublingv1.ValueType_VALUE_TYPE_STRING,
		kublingv1.ValueType_VALUE_TYPE_VARBINARY,
		kublingv1.ValueType_VALUE_TYPE_BOOLEAN,
		kublingv1.ValueType_VALUE_TYPE_INTEGER,
		kublingv1.ValueType_VALUE_TYPE_LONG,
		kublingv1.ValueType_VALUE_TYPE_BIGINTEGER,
		kublingv1.ValueType_VALUE_TYPE_FLOAT,
		kublingv1.ValueType_VALUE_TYPE_DOUBLE,
		kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
	} {
		if !inputTypes[valueType] {
			t.Fatalf("Capabilities() does not accept writable type %s", valueType)
		}
	}

	health, err := implementation.Health(context.Background())
	if err != nil || !health.GetHealthy() || health.GetMessage() != "BACnet provider process is ready" {
		t.Fatalf("Health() = %v, %v", health, err)
	}
	metadata, err := implementation.Metadata(context.Background())
	if err != nil || metadata == nil || len(metadata.GetTables()) != 7 {
		t.Fatalf("Metadata() = %v, %v", metadata, err)
	}
	fragment, err := implementation.SemanticFragment(context.Background())
	if err != nil || fragment == nil || len(fragment.Document) == 0 {
		t.Fatalf("SemanticFragment() = %v, %v", fragment, err)
	}
	connection, err := implementation.Open(context.Background())
	if err != nil || connection == nil {
		t.Fatalf("Open() = %v, %v", connection, err)
	}
}

func TestProviderHonorsCanceledContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	implementation := newTestProvider(t)

	if _, err := implementation.Capabilities(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Capabilities() code = %v", status.Code(err))
	}
	if _, err := implementation.Health(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Health() code = %v", status.Code(err))
	}
	if _, err := implementation.Metadata(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Metadata() code = %v", status.Code(err))
	}
	if _, err := implementation.SemanticFragment(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("SemanticFragment() code = %v", status.Code(err))
	}
	if _, err := implementation.Open(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("Open() code = %v", status.Code(err))
	}
}

func TestConnectionRejectsUnavailableOperations(t *testing.T) {
	connection := &Connection{}
	ctx := context.Background()
	if err := connection.Begin(ctx); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Begin() code = %v", status.Code(err))
	}
	if active, err := connection.InTransaction(ctx); active || status.Code(err) != codes.Unimplemented {
		t.Fatalf("InTransaction() = %v, %v", active, err)
	}
	if _, err := connection.Insert(ctx, &providerv1.InsertRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Insert() code = %v", status.Code(err))
	}
	if _, err := connection.Delete(ctx, &providerv1.DeleteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Delete() code = %v", status.Code(err))
	}
}

func TestConnectionCloseIsIdempotent(t *testing.T) {
	connection := &Connection{}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := connection.Query(context.Background(), &providerv1.QueryRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query() after close code = %v", status.Code(err))
	}
}

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	profiles, err := profile.Load(nil)
	if err != nil {
		t.Fatalf("profile.Load() error = %v", err)
	}
	compiled, err := catalog.Compile(profiles)
	if err != nil {
		t.Fatalf("catalog.Compile() error = %v", err)
	}
	return New(compiled, emptyDeviceRegistry{})
}

type emptyDeviceRegistry struct {
	snapshot discovery.Snapshot
}

func (r emptyDeviceRegistry) Snapshot() discovery.Snapshot {
	return r.snapshot
}

func (emptyDeviceRegistry) ReadObjectList(
	context.Context,
	discovery.Device,
	int,
) ([]bacnet.ObjectIdentifier, error) {
	return nil, nil
}

func (emptyDeviceRegistry) ReadProperties(
	context.Context,
	discovery.Device,
	[]service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	return nil, nil
}

func (emptyDeviceRegistry) MutationPolicy(
	string,
	string,
) (config.MutationPolicy, bool) {
	return config.MutationPolicy{}, false
}

func (emptyDeviceRegistry) WriteProperty(
	context.Context,
	discovery.Device,
	bacnet.ObjectIdentifier,
	bacnet.PropertyReference,
	bacnet.ApplicationValue,
	*uint8,
) error {
	return nil
}
