package kubernetes

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

type fakeKubernetesClient struct {
	discoveryClient  discovery.DiscoveryInterface
	dynamicClient    dynamic.Interface
	defaultNamespace string
	healthErr        error
	closeErr         error
	closes           atomic.Int32
}

func (c *fakeKubernetesClient) Discovery() discovery.DiscoveryInterface {
	return c.discoveryClient
}
func (c *fakeKubernetesClient) Dynamic() dynamic.Interface { return c.dynamicClient }
func (c *fakeKubernetesClient) DefaultNamespace() string {
	if c.defaultNamespace == "" {
		return "default"
	}
	return c.defaultNamespace
}
func (c *fakeKubernetesClient) Health(context.Context) error { return c.healthErr }
func (c *fakeKubernetesClient) Close() error {
	c.closes.Add(1)
	return c.closeErr
}

func TestProviderCapabilitiesAdvertiseImplementedOperations(t *testing.T) {
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return &fakeKubernetesClient{}, nil
	})
	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() {
		t.Fatal("transactions advertised as supported")
	}
	if !capabilities.GetMutations().GetInsert() || !capabilities.GetMutations().GetUpdate() || !capabilities.GetMutations().GetDelete() {
		t.Fatalf("mutation capabilities = %v", capabilities.GetMutations())
	}
	if capabilities.GetQuery().GetOrdering().GetSupported() {
		t.Fatal("ordering advertised as supported")
	}
	if !capabilities.GetQuery().GetPagination().GetLimit() || capabilities.GetQuery().GetPagination().GetOffset() {
		t.Fatalf("pagination capabilities = %v", capabilities.GetQuery().GetPagination())
	}
	if len(capabilities.GetQuery().GetExpressions().GetComparisonOperators()) != 1 ||
		capabilities.GetQuery().GetExpressions().GetComparisonOperators()[0] != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
		t.Fatalf("comparison capabilities = %v", capabilities.GetQuery().GetExpressions().GetComparisonOperators())
	}
	if len(capabilities.GetQuery().GetExpressions().GetLogicalOperators()) != 1 ||
		capabilities.GetQuery().GetExpressions().GetLogicalOperators()[0] != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND {
		t.Fatalf("logical capabilities = %v", capabilities.GetQuery().GetExpressions().GetLogicalOperators())
	}
}

func TestProviderHealth(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		client := &fakeKubernetesClient{}
		provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
			return client, nil
		})
		response, err := provider.Health(context.Background())
		if err != nil {
			t.Fatalf("Health() error = %v", err)
		}
		if !response.GetHealthy() {
			t.Fatalf("Healthy = false, message = %q", response.GetMessage())
		}
		if client.closes.Load() != 1 {
			t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		client := &fakeKubernetesClient{healthErr: errors.New("not ready")}
		provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
			return client, nil
		})
		response, err := provider.Health(context.Background())
		if err != nil {
			t.Fatalf("Health() error = %v", err)
		}
		if response.GetHealthy() {
			t.Fatal("Healthy = true, want false")
		}
		if client.closes.Load() != 1 {
			t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
		}
	})

	t.Run("canceled", func(t *testing.T) {
		provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
			return &fakeKubernetesClient{healthErr: context.Canceled}, nil
		})
		_, err := provider.Health(context.Background())
		if status.Code(err) != codes.Canceled {
			t.Fatalf("Health() code = %v, want Canceled", status.Code(err))
		}
	})
}

func TestConnectionsShareOneClusterClient(t *testing.T) {
	client := &fakeKubernetesClient{}
	var creates atomic.Int32
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		creates.Add(1)
		return client, nil
	})
	first := openTestConnection(t, provider)
	second := openTestConnection(t, provider)
	if _, err := first.clientFor(context.Background()); err != nil {
		t.Fatalf("first clientFor() error = %v", err)
	}
	if _, err := second.clientFor(context.Background()); err != nil {
		t.Fatalf("second clientFor() error = %v", err)
	}
	if creates.Load() != 1 {
		t.Fatalf("client creations = %d, want 1", creates.Load())
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if client.closes.Load() != 0 {
		t.Fatalf("Close() calls after first connection = %d, want 0", client.closes.Load())
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls after final connection = %d, want 1", client.closes.Load())
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls after repeated close = %d, want 1", client.closes.Load())
	}
}

func TestConcurrentAcquisitionCreatesOneClient(t *testing.T) {
	client := &fakeKubernetesClient{}
	var creates atomic.Int32
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		creates.Add(1)
		return client, nil
	})

	const workers = 32
	start := make(chan struct{})
	entries := make(chan *clientEntry, workers)
	errorsChannel := make(chan error, workers)
	var entered sync.WaitGroup
	entered.Add(workers)
	for range workers {
		go func() {
			entered.Done()
			<-start
			entry, err := provider.acquireClient(context.Background())
			entries <- entry
			errorsChannel <- err
		}()
	}
	entered.Wait()
	close(start)

	acquired := make([]*clientEntry, 0, workers)
	for range workers {
		entry := <-entries
		if err := <-errorsChannel; err != nil {
			t.Fatalf("acquireClient() error = %v", err)
		}
		acquired = append(acquired, entry)
	}
	if creates.Load() != 1 {
		t.Fatalf("client creations = %d, want 1", creates.Load())
	}
	for _, entry := range acquired {
		if err := provider.releaseClient(entry); err != nil {
			t.Fatalf("releaseClient() error = %v", err)
		}
	}
	if client.closes.Load() != 1 {
		t.Fatalf("Close() calls = %d, want 1", client.closes.Load())
	}
}

func TestConnectionPropagatesFinalClientCloseError(t *testing.T) {
	wantErr := errors.New("close failed")
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return &fakeKubernetesClient{closeErr: wantErr}, nil
	})
	connection := openTestConnection(t, provider)
	if _, err := connection.clientFor(context.Background()); err != nil {
		t.Fatalf("clientFor() error = %v", err)
	}
	if err := connection.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want %v", err, wantErr)
	}
}

func TestConnectionUnsupportedTransactionsAndClosedState(t *testing.T) {
	provider := testProvider(t, func(context.Context, Config) (kubernetesClient, error) {
		return &fakeKubernetesClient{}, nil
	})
	connection := openTestConnection(t, provider)

	operations := []func() error{
		func() error { return connection.Begin(context.Background()) },
		func() error { return connection.Commit(context.Background()) },
		func() error { return connection.Rollback(context.Background()) },
	}
	for index, operation := range operations {
		if code := status.Code(operation()); code != codes.Unimplemented {
			t.Fatalf("operation %d code = %v, want Unimplemented", index, code)
		}
	}
	if active, err := connection.InTransaction(context.Background()); active || status.Code(err) != codes.Unimplemented {
		t.Fatalf("InTransaction() = %v, %v; want false, Unimplemented", active, err)
	}

	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := connection.Query(context.Background(), &providerv1.QueryRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query() after close code = %v, want FailedPrecondition", status.Code(err))
	}
}

func testProvider(t *testing.T, factory clientFactory) *Provider {
	t.Helper()
	config, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	return newProvider(config, factory)
}

func openTestConnection(t *testing.T, provider *Provider) *Connection {
	t.Helper()
	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return connection.(*Connection)
}
