package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	inmemory "github.com/kubling-community/kubling-providers/providers/inmemory"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	providercache "github.com/kubling-community/kubling-providers/sdk-go/provider/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestQueryLoggingDistinguishesReceivedExecutedAndCached(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	implementation := inmemory.New(inmemory.WithQueryLogger(logger))
	cachedImplementation, _ := providercache.Wrap(
		implementation,
		providercache.Config{},
	)
	service := providersdk.NewServer(cachedImplementation)

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, &queryLoggingServer{
		Server: service,
		logger: logger,
	})
	go func() { _ = grpcServer.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///inmemory-observability",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		_ = service.Close(context.Background())
		grpcServer.Stop()
		_ = listener.Close()
	})

	client := providerv1.NewProviderServiceClient(connection)
	opened, err := client.OpenConnection(
		context.Background(),
		&providerv1.OpenConnectionRequest{},
	)
	if err != nil {
		t.Fatalf("OpenConnection() error = %v", err)
	}
	request := &providerv1.QueryRequest{
		ConnectionId: opened.GetConnectionId(),
		Entity: &providerv1.EntityReference{
			Name:      "TASK",
			Namespace: "private-namespace",
		},
		Projections: []*providerv1.Projection{{
			OutputName: "task_count",
			Expression: &providerv1.Expression{
				Kind: &providerv1.Expression_Aggregate{
					Aggregate: &providerv1.AggregateCall{
						Function: providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
						ResultType: &kublingv1.TypeDescriptor{
							Type: kublingv1.ValueType_VALUE_TYPE_LONG,
						},
					},
				},
			},
		}},
	}

	for attempt := 0; attempt < 2; attempt++ {
		stream, err := client.Query(context.Background(), request)
		if err != nil {
			t.Fatalf("Query(%d) error = %v", attempt, err)
		}
		for {
			_, err = stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Query(%d).Recv() error = %v", attempt, err)
			}
		}
	}

	logged := output.String()
	if got := strings.Count(logged, `"msg":"provider query received"`); got != 2 {
		t.Fatalf("received log count = %d, want 2: %s", got, logged)
	}
	if got := strings.Count(logged, `"msg":"provider query executed"`); got != 1 {
		t.Fatalf("executed log count = %d, want 1: %s", got, logged)
	}
	if !strings.Contains(logged, `"aggregate_functions":["COUNT_STAR"]`) {
		t.Fatalf("aggregate function missing from log: %s", logged)
	}
	for _, forbidden := range []string{
		opened.GetConnectionId(),
		"private-namespace",
	} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("query log contains %q: %s", forbidden, logged)
		}
	}
}
