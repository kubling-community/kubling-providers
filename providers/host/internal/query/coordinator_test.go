package query

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryMemoryRejectsUnboundedFanoutByDefault(t *testing.T) {
	coordinator, err := New(registry.NewMemory(), gateway.NewDirectory(), Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: "MEMORY"},
		Projections: []*providerv1.Projection{testFieldProjection("total_bytes")},
	}
	if _, err := coordinator.Query(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query(unbounded MEMORY) code = %v, want FailedPrecondition", status.Code(err))
	}

	request.Filter = filterEquality("namespace", "fleet-a")
	stream, err := coordinator.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query(namespace-routed MEMORY) error = %v", err)
	}
	defer stream.Close()
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next(namespace-routed MEMORY) error = %v, want EOF", err)
	}
}

func TestQueryArrayProjectionRequiresNegotiatedFeature(t *testing.T) {
	coordinator, err := New(registry.NewMemory(), gateway.NewDirectory(), Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: "CPU_INFO"},
		Projections: []*providerv1.Projection{testFieldProjection("flags")},
		Filter:      filterEquality("namespace", "fleet-a"),
	}
	if _, err := coordinator.Query(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query(array without feature) code = %v, want FailedPrecondition", status.Code(err))
	}

	request.AcceptedFeatures = []string{grpcfeatures.ArrayValuesV1}
	stream, err := coordinator.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query(array with feature) error = %v", err)
	}
	defer stream.Close()
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next(array with feature) error = %v, want EOF", err)
	}
}

func TestQueryHostsKeepsDisconnectedIdentity(t *testing.T) {
	at := time.Date(2026, time.October, 8, 12, 0, 0, 0, time.UTC)
	fleet := registry.NewMemory()
	snapshot, err := fleet.OpenSession(
		context.Background(),
		registry.Registration{
			Namespace:          "fleet-a",
			AgentIdentifier:    "host-a",
			Hostname:           "node-a",
			AgentVersion:       "test",
			ProtocolVersion:    model.ProtocolVersion{Major: 1},
			SchemaVersion:      hostschema.Version,
			MaxConcurrentScans: 1,
		},
		registry.SessionWindow{ObservedAt: at, ExpiresAt: at.Add(time.Minute)},
	)
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	if err := fleet.CloseSession(context.Background(), snapshot.Session.ID, at.Add(time.Second)); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}
	coordinator, err := newCoordinator(
		fleet,
		gateway.NewDirectory(),
		Config{},
		func() time.Time { return at.Add(2 * time.Second) },
		func() (string, error) { return "unused", nil },
	)
	if err != nil {
		t.Fatalf("newCoordinator() error = %v", err)
	}
	stream, err := coordinator.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "HOST"},
		Projections: []*providerv1.Projection{
			testFieldProjection("host_id"),
			testFieldProjection("state"),
		},
	})
	if err != nil {
		t.Fatalf("Query(HOST) error = %v", err)
	}
	defer stream.Close()
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "host-a" || values[1].GetStringValue() != "offline" {
		t.Fatalf("HOST values = %v, want host-a/offline", values)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next(terminal) error = %v, want EOF", err)
	}
}

func TestNewRejectsInvalidDistributedLimits(t *testing.T) {
	tests := map[string]Config{
		"negative concurrent targets": {MaxConcurrentTargets: -1},
		"negative rows":               {MaxRowsPerTarget: -1},
		"negative bytes":              {MaxBytesPerTarget: -1},
		"oversized preferred batch":   {PreferredBatchRows: maximumPreferredBatchRows + 1},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(registry.NewMemory(), gateway.NewDirectory(), config); err == nil {
				t.Fatal("New() error = nil")
			}
		})
	}
}

func testFieldProjection(name string) *providerv1.Projection {
	return &providerv1.Projection{
		Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
			Field: &providerv1.FieldReference{Name: name},
		}},
	}
}
