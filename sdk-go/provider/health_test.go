package provider

import (
	"context"
	"errors"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestServerHealth(t *testing.T) {
	t.Run("returns provider health", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expected := &providerv1.HealthResponse{
			Healthy: true,
			Message: "ready",
		}
		var receivedContext context.Context
		callCount := 0
		server := NewServer(&serverTestProvider{
			healthFunc: func(
				ctx context.Context,
			) (*providerv1.HealthResponse, error) {
				callCount++
				receivedContext = ctx

				return expected, nil
			},
		})

		response, err := server.Health(
			ctx,
			&providerv1.HealthRequest{},
		)
		if err != nil {
			t.Fatalf("Health: %v", err)
		}
		if !proto.Equal(response, expected) {
			t.Fatalf("Health response = %v, want %v", response, expected)
		}
		if callCount != 1 {
			t.Fatalf("Health call count = %d, want 1", callCount)
		}
		if receivedContext != ctx {
			t.Fatal("Health did not receive the request context")
		}
	})

	t.Run("propagates provider error", func(t *testing.T) {
		healthErr := errors.New("health failed")
		server := NewServer(&serverTestProvider{
			healthFunc: func(
				context.Context,
			) (*providerv1.HealthResponse, error) {
				return nil, healthErr
			},
		})

		response, err := server.Health(
			context.Background(),
			&providerv1.HealthRequest{},
		)
		if response != nil {
			t.Fatal("Health returned a response with an error")
		}
		if !errors.Is(err, healthErr) {
			t.Fatalf("Health error = %v, want %v", err, healthErr)
		}
	})

	t.Run("rejects nil response", func(t *testing.T) {
		server := NewServer(&serverTestProvider{
			healthFunc: func(
				context.Context,
			) (*providerv1.HealthResponse, error) {
				return nil, nil
			},
		})

		response, err := server.Health(
			context.Background(),
			&providerv1.HealthRequest{},
		)
		if response != nil {
			t.Fatal("Health returned a response for nil provider health")
		}
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("Health status = %s, want %s", got, codes.Internal)
		}
	})
}
