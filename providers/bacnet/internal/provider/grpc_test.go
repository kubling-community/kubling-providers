package provider

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	bacnet "github.com/otfabric/go-bacnet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCFoundationLifecycle(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	implementation := newTestProvider(t)
	registry := &recordingDeviceRegistry{
		emptyDeviceRegistry: emptyDeviceRegistry{snapshot: discovery.Snapshot{
			CapturedAt: time.Now().UTC(),
			Scopes: []discovery.ScopeSnapshot{{
				Namespace:    "test",
				ScopeID:      "local",
				AttachmentID: "loopback",
				Devices: []discovery.Device{{
					Namespace:    "test",
					ScopeID:      "local",
					AttachmentID: "loopback",
					Instance:     1201,
					ProfileID:    profile.StandardProfileID,
				}},
			}},
		}},
		policy: config.MutationPolicy{Enabled: true, Priority: 16},
	}
	implementation.devices = registry
	service := providersdk.NewServer(implementation)
	server := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(server, service)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///bacnet-provider-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = service.Close(context.Background())
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})

	client := providerv1.NewProviderServiceClient(connection)
	health, err := client.Health(context.Background(), &providerv1.HealthRequest{})
	if err != nil || !health.GetHealthy() {
		t.Fatalf("Health() = %v, %v", health, err)
	}
	capabilities, err := client.GetCapabilities(context.Background(), &providerv1.GetCapabilitiesRequest{})
	if err != nil || capabilities.GetTransactions().GetSupported() ||
		!capabilities.GetMutations().GetUpdate() {
		t.Fatalf("GetCapabilities() = %v, %v", capabilities, err)
	}
	schema, err := client.GetSchema(context.Background(), &providerv1.GetSchemaRequest{})
	if err != nil || schema.GetMetadata() == nil || len(schema.GetMetadata().GetTables()) != 7 {
		t.Fatalf("GetSchema() = %v, %v", schema, err)
	}
	semantic, err := client.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil || semantic.GetFragment() == nil || len(semantic.GetFragment().GetDocument()) == 0 {
		t.Fatalf("GetSemanticFragment() = %v, %v", semantic, err)
	}
	opened, err := client.OpenConnection(context.Background(), &providerv1.OpenConnectionRequest{})
	if err != nil || opened.GetConnectionId() == "" {
		t.Fatalf("OpenConnection() = %v, %v", opened, err)
	}
	queryStream, err := client.Query(context.Background(), &providerv1.QueryRequest{
		ConnectionId: opened.GetConnectionId(),
		Entity:       &providerv1.EntityReference{Name: catalog.ScopesTable},
		Projections: []*providerv1.Projection{{
			Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: catalog.NamespaceColumn},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	response, err := queryStream.Recv()
	if err != nil || len(response.GetBatch().GetTuples()) != 1 ||
		response.GetBatch().GetTuples()[0].GetValues()[0].GetStringValue() != "test" {
		t.Fatalf("Query() response = %v, %v", response, err)
	}
	if _, err := queryStream.Recv(); err != io.EOF {
		t.Fatalf("Query() terminal error = %v, want EOF", err)
	}
	updated, err := client.Update(context.Background(), &providerv1.UpdateRequest{
		ConnectionId: opened.GetConnectionId(),
		Entity:       &providerv1.EntityReference{Name: catalog.AnalogPointsTable},
		Assignments: []*providerv1.Assignment{{
			Field: "present_value",
			Value: grpcLiteral(&kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{
				DoubleValue: 19.5,
			}}),
		}},
		Filter: grpcAnd(
			grpcEqual(catalog.NamespaceColumn, grpcString("test")),
			grpcEqual(catalog.ScopeIDColumn, grpcString("local")),
			grpcEqual(catalog.DeviceInstanceColumn, grpcInteger(1201)),
			grpcEqual(catalog.ObjectTypeColumn, grpcString("analog-value")),
			grpcEqual(catalog.ObjectInstanceColumn, grpcInteger(1)),
		),
	})
	if err != nil || updated.GetAffectedRows() != 1 || registry.writes != 1 ||
		registry.value.Kind != bacnet.ValueReal || registry.value.Real != 19.5 ||
		registry.priority == nil || *registry.priority != 16 {
		t.Fatalf(
			"Update() = %v, %v; write value = %#v priority = %v",
			updated,
			err,
			registry.value,
			registry.priority,
		)
	}
	if _, err := client.CloseConnection(context.Background(), &providerv1.CloseConnectionRequest{
		ConnectionId: opened.GetConnectionId(),
	}); err != nil {
		t.Fatalf("CloseConnection() error = %v", err)
	}
}

type recordingDeviceRegistry struct {
	emptyDeviceRegistry
	policy   config.MutationPolicy
	writes   int
	value    bacnet.ApplicationValue
	priority *uint8
}

func (r *recordingDeviceRegistry) MutationPolicy(string, string) (config.MutationPolicy, bool) {
	return r.policy, true
}

func (r *recordingDeviceRegistry) WriteProperty(
	_ context.Context,
	_ discovery.Device,
	_ bacnet.ObjectIdentifier,
	_ bacnet.PropertyReference,
	value bacnet.ApplicationValue,
	priority *uint8,
) error {
	r.writes++
	r.value = value.Clone()
	if priority != nil {
		copied := *priority
		r.priority = &copied
	}
	return nil
}

func grpcEqual(field string, value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: field},
			}},
			Right: grpcLiteral(value),
		},
	}}
}

func grpcAnd(expressions ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{
		Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: expressions,
		},
	}}
}

func grpcLiteral(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: value},
	}}
}

func grpcString(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func grpcInteger(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}
