package cassandra

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testSession struct {
	closeCount         atomic.Int32
	metadataCalls      atomic.Int32
	metadata           *gocql.KeyspaceMetadata
	metadataErr        error
	metadataKeyspace   string
	metadataKeyspaceMu sync.Mutex
}

type emptyTestIterator struct{}

func (s *testSession) Close() {
	s.closeCount.Add(1)
}

func (s *testSession) KeyspaceMetadata(
	keyspace string,
) (*gocql.KeyspaceMetadata, error) {
	s.metadataCalls.Add(1)
	s.metadataKeyspaceMu.Lock()
	s.metadataKeyspace = keyspace
	s.metadataKeyspaceMu.Unlock()

	if s.metadata != nil || s.metadataErr != nil {
		return s.metadata, s.metadataErr
	}

	return &gocql.KeyspaceMetadata{
		Name:   keyspace,
		Tables: map[string]*gocql.TableMetadata{},
	}, nil
}

func (*testSession) Query(
	context.Context,
	string,
	[]any,
	int,
) driverIterator {
	return emptyTestIterator{}
}

func (*testSession) Exec(context.Context, string, []any) error {
	return nil
}

func (emptyTestIterator) Columns() []gocql.ColumnInfo { return nil }

func (emptyTestIterator) MapScan(map[string]any) bool { return false }

func (emptyTestIterator) Close() error { return nil }

func TestNewValidatesAndCopiesConfig(t *testing.T) {
	hosts := []string{"localhost"}
	provider, err := New(Config{
		DataSources: map[string]DataSourceConfig{
			"inventory": {
				Hosts:    hosts,
				Keyspace: "inventory",
			},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	hosts[0] = "changed"
	if got := provider.config.DataSources["inventory"].Hosts[0]; got != "localhost" {
		t.Fatalf("New() stored host = %q, want localhost", got)
	}

	if _, err := New(Config{}); err == nil {
		t.Fatal("New() error = nil, want validation error")
	}
}

func TestProviderCapabilitiesAndHealth(t *testing.T) {
	provider := newTestProvider(t, []string{"inventory"}, nil)

	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetTransactions().GetSupported() {
		t.Fatal("Capabilities() transactions supported = true, want false")
	}
	if !capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		!capabilities.GetMutations().GetDelete() {
		t.Fatal("Capabilities() does not advertise implemented mutations")
	}
	if !capabilities.GetQuery().GetOrdering().GetSupported() ||
		!capabilities.GetQuery().GetPagination().GetLimit() ||
		capabilities.GetQuery().GetPagination().GetOffset() {
		t.Fatalf("Capabilities() query = %v", capabilities.GetQuery())
	}

	health, err := provider.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if !health.GetHealthy() {
		t.Fatal("Health() healthy = false, want true")
	}
}

func TestProviderOpenCreatesProviderLevelConnection(t *testing.T) {
	var factoryCalls atomic.Int32
	provider := newTestProvider(
		t,
		[]string{"inventory", "events"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			factoryCalls.Add(1)
			return &testSession{}, nil
		},
	)

	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if connection == nil {
		t.Fatal("Open() connection = nil")
	}
	if factoryCalls.Load() != 0 {
		t.Fatalf("Open() session factory calls = %d, want 0", factoryCalls.Load())
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
}

func TestProviderMetadataAggregatesAllNamespacesDeterministically(t *testing.T) {
	var sessionsMu sync.Mutex
	sessions := make(map[string]*testSession)
	provider := newTestProvider(
		t,
		[]string{"zeta", "alpha"},
		func(_ context.Context, config DataSourceConfig) (driverSession, error) {
			session := &testSession{metadata: &gocql.KeyspaceMetadata{
				Name: config.Keyspace,
				Tables: map[string]*gocql.TableMetadata{
					"items": {
						Name:    "items",
						Columns: map[string]*gocql.ColumnMetadata{},
					},
				},
			}}
			sessionsMu.Lock()
			sessions[config.Keyspace] = session
			sessionsMu.Unlock()
			return session, nil
		},
	)

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if got := metadata.GetProperties()["cassandra.namespace_count"]; got != "2" {
		t.Fatalf("Metadata() namespace count = %q, want 2", got)
	}
	if len(metadata.GetNamespaces()) != 2 || len(metadata.GetTables()) != 2 {
		t.Fatalf("Metadata() = %v", metadata)
	}
	if got := metadata.GetNamespaces()[0].GetName(); got != "alpha" {
		t.Fatalf("first namespace = %q, want alpha", got)
	}
	if got := metadata.GetNamespaces()[1].GetName(); got != "zeta" {
		t.Fatalf("second namespace = %q, want zeta", got)
	}
	if got := metadata.GetTables()[0].GetNamespace(); got != "alpha" {
		t.Fatalf("first table namespace = %q, want alpha", got)
	}
	for name, session := range sessions {
		if session.metadataCalls.Load() != 1 {
			t.Fatalf("%s metadata calls = %d, want 1", name, session.metadataCalls.Load())
		}
		if session.closeCount.Load() != 1 {
			t.Fatalf("%s close calls = %d, want 1", name, session.closeCount.Load())
		}
	}
}

func TestProviderMetadataSharesExistingSession(t *testing.T) {
	session := &testSession{}
	var factoryCalls atomic.Int32
	provider := newTestProvider(
		t,
		[]string{"inventory"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			factoryCalls.Add(1)
			return session, nil
		},
	)
	config := provider.config.DataSources["inventory"]
	held, err := provider.acquireSession(context.Background(), "inventory", config)
	if err != nil {
		t.Fatalf("acquireSession() error = %v", err)
	}

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if len(metadata.GetNamespaces()) != 1 {
		t.Fatalf("Metadata() namespaces = %d, want 1", len(metadata.GetNamespaces()))
	}
	if factoryCalls.Load() != 1 {
		t.Fatalf("session factory calls = %d, want 1", factoryCalls.Load())
	}
	if session.closeCount.Load() != 0 {
		t.Fatalf("session close calls = %d, want 0", session.closeCount.Load())
	}

	provider.releaseSession("inventory", held)
	if session.closeCount.Load() != 1 {
		t.Fatalf("session close calls = %d, want 1", session.closeCount.Load())
	}
}

func TestProviderMetadataPropagatesDriverFailureAndReleasesSession(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	session := &testSession{metadataErr: metadataErr}
	provider := newTestProvider(
		t,
		[]string{"inventory"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			return session, nil
		},
	)

	metadata, err := provider.Metadata(context.Background())
	if metadata != nil {
		t.Fatalf("Metadata() metadata = %v, want nil", metadata)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Metadata() code = %v, want %v", status.Code(err), codes.Unavailable)
	}
	if !errors.Is(err, metadataErr) {
		t.Fatalf("Metadata() error = %v, want wrapped driver error", err)
	}
	if session.closeCount.Load() != 1 {
		t.Fatalf("session Close() calls = %d, want 1", session.closeCount.Load())
	}
}

func TestProviderCoordinatesConcurrentSessionCreation(t *testing.T) {
	session := &testSession{}
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var factoryCalls atomic.Int32
	provider := newTestProvider(
		t,
		[]string{"inventory"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			factoryCalls.Add(1)
			startOnce.Do(func() { close(started) })
			<-release
			return session, nil
		},
	)
	config := provider.config.DataSources["inventory"]

	const acquisitionCount = 16
	sessions := make(chan driverSession, acquisitionCount)
	errorsChannel := make(chan error, acquisitionCount)
	var waitGroup sync.WaitGroup
	for range acquisitionCount {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			acquired, err := provider.acquireSession(
				context.Background(),
				"inventory",
				config,
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			sessions <- acquired
		}()
	}

	<-started
	close(release)
	waitGroup.Wait()
	close(errorsChannel)
	close(sessions)

	for err := range errorsChannel {
		t.Fatalf("concurrent acquire error = %v", err)
	}
	if factoryCalls.Load() != 1 {
		t.Fatalf("session factory calls = %d, want 1", factoryCalls.Load())
	}
	for acquired := range sessions {
		if acquired != session {
			t.Fatal("acquireSession() returned a different session")
		}
		provider.releaseSession("inventory", acquired)
	}
	if session.closeCount.Load() != 1 {
		t.Fatalf("session Close() calls = %d, want 1", session.closeCount.Load())
	}
}

func TestAcquireSessionCanCancelWhileCreationIsInProgress(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	provider := newTestProvider(
		t,
		[]string{"inventory"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			close(started)
			<-release
			return &testSession{}, nil
		},
	)
	config := provider.config.DataSources["inventory"]

	firstSession := make(chan driverSession, 1)
	firstError := make(chan error, 1)
	go func() {
		session, err := provider.acquireSession(
			context.Background(),
			"inventory",
			config,
		)
		firstSession <- session
		firstError <- err
	}()

	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session, err := provider.acquireSession(ctx, "inventory", config)
	if session != nil {
		t.Fatalf("canceled acquire session = %T, want nil", session)
	}
	if status.Code(err) != codes.Canceled {
		t.Fatalf("canceled acquire code = %v, want %v", status.Code(err), codes.Canceled)
	}

	close(release)
	session = <-firstSession
	if err := <-firstError; err != nil {
		t.Fatalf("first acquire error = %v", err)
	}
	provider.releaseSession("inventory", session)
}

func TestAcquireSessionPropagatesCreationFailure(t *testing.T) {
	sessionErr := errors.New("cluster unavailable")
	provider := newTestProvider(
		t,
		[]string{"inventory"},
		func(context.Context, DataSourceConfig) (driverSession, error) {
			return nil, sessionErr
		},
	)
	config := provider.config.DataSources["inventory"]

	session, err := provider.acquireSession(
		context.Background(),
		"inventory",
		config,
	)
	if session != nil {
		t.Fatalf("acquireSession() session = %T, want nil", session)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("acquireSession() code = %v, want %v", status.Code(err), codes.Unavailable)
	}
	if !errors.Is(err, sessionErr) {
		t.Fatalf("acquireSession() error = %v, want wrapped session error", err)
	}
}

func TestConnectionTransactionsAreUnsupported(t *testing.T) {
	provider := newTestProvider(t, []string{"inventory"}, nil)
	connection := openTestConnection(t, provider)

	operations := []struct {
		name string
		call func() error
	}{
		{name: "begin", call: func() error { return connection.Begin(context.Background()) }},
		{name: "commit", call: func() error { return connection.Commit(context.Background()) }},
		{name: "rollback", call: func() error { return connection.Rollback(context.Background()) }},
		{name: "in transaction", call: func() error {
			active, err := connection.InTransaction(context.Background())
			if active {
				t.Fatal("InTransaction() active = true, want false")
			}
			return err
		}},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if code := status.Code(operation.call()); code != codes.Unimplemented {
				t.Fatalf("transaction operation code = %v, want %v", code, codes.Unimplemented)
			}
		})
	}
}

func TestConnectionRejectsOperationsAfterClose(t *testing.T) {
	provider := newTestProvider(t, []string{"inventory"}, nil)
	connection := openTestConnection(t, provider)
	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Query() code = %v, want %v", status.Code(err), codes.FailedPrecondition)
	}
}

func newTestProvider(
	t *testing.T,
	namespaces []string,
	factory sessionFactory,
) *Provider {
	t.Helper()

	dataSources := make(map[string]DataSourceConfig, len(namespaces))
	for _, namespace := range namespaces {
		dataSource := validTestDataSource()
		dataSource.Keyspace = namespace
		dataSources[namespace] = dataSource
	}
	config, err := normalizeConfig(Config{DataSources: dataSources})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if factory == nil {
		factory = func(context.Context, DataSourceConfig) (driverSession, error) {
			return &testSession{}, nil
		}
	}

	return newProvider(config, factory)
}

func openTestConnection(
	t *testing.T,
	provider *Provider,
) *Connection {
	t.Helper()

	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	return connection.(*Connection)
}
