package cache

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

type testProvider struct {
	connection providersdk.Connection
}

type testSchemaProvider struct {
	*testProvider
	schemaFunc func(context.Context) (string, error)
}

func (p *testSchemaProvider) Schema(ctx context.Context) (string, error) {
	return p.schemaFunc(ctx)
}

type testMetadataProvider struct {
	*testProvider
	metadataFunc func(context.Context) (*providersdk.Metadata, error)
}

func (p *testMetadataProvider) Metadata(
	ctx context.Context,
) (*providersdk.Metadata, error) {
	return p.metadataFunc(ctx)
}

func (p *testProvider) Capabilities(
	context.Context,
) (*providersdk.Capabilities, error) {
	return &providersdk.Capabilities{}, nil
}

func (p *testProvider) Health(
	context.Context,
) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{Healthy: true}, nil
}

func (p *testProvider) Open(
	context.Context,
) (providersdk.Connection, error) {
	return p.connection, nil
}

type testConnection struct {
	providersdk.Connection

	queryFunc func(
		context.Context,
		*providerv1.QueryRequest,
	) (providersdk.ResultStream, error)
	insertFunc func(
		context.Context,
		*providerv1.InsertRequest,
	) (*providerv1.InsertResponse, error)
	updateFunc func(
		context.Context,
		*providerv1.UpdateRequest,
	) (*providerv1.UpdateResponse, error)
	deleteFunc func(
		context.Context,
		*providerv1.DeleteRequest,
	) (*providerv1.DeleteResponse, error)
	beginFunc         func(context.Context) error
	commitFunc        func(context.Context) error
	rollbackFunc      func(context.Context) error
	inTransactionFunc func(context.Context) (bool, error)
	closeFunc         func(context.Context) error
}

func (c *testConnection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	return c.queryFunc(ctx, request)
}

func (c *testConnection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	return c.insertFunc(ctx, request)
}

func (c *testConnection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	return c.updateFunc(ctx, request)
}

func (c *testConnection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	return c.deleteFunc(ctx, request)
}

func (c *testConnection) Begin(ctx context.Context) error {
	return c.beginFunc(ctx)
}

func (c *testConnection) Commit(ctx context.Context) error {
	return c.commitFunc(ctx)
}

func (c *testConnection) Rollback(ctx context.Context) error {
	return c.rollbackFunc(ctx)
}

func (c *testConnection) InTransaction(
	ctx context.Context,
) (bool, error) {
	return c.inTransactionFunc(ctx)
}

func (c *testConnection) Close(ctx context.Context) error {
	return c.closeFunc(ctx)
}

type testStream struct {
	mu       sync.Mutex
	batches  []*providerv1.TupleBatch
	next     int
	nextErr  error
	closeErr error
	closed   bool
}

func (s *testStream) Next(
	context.Context,
) (*providerv1.TupleBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.next < len(s.batches) {
		batch := s.batches[s.next]
		s.next++
		return batch, nil
	}
	if s.nextErr != nil {
		return nil, s.nextErr
	}

	return nil, io.EOF
}

func (s *testStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	return s.closeErr
}

func TestProviderSchema(t *testing.T) {
	t.Run("delegates global schema", func(t *testing.T) {
		const expectedSchema = "CREATE FOREIGN TABLE sample"
		cachedProvider, _ := Wrap(
			&testSchemaProvider{
				testProvider: &testProvider{},
				schemaFunc: func(context.Context) (string, error) {
					return expectedSchema, nil
				},
			},
			Config{},
		)

		schema, err := cachedProvider.Schema(context.Background())
		if err != nil {
			t.Fatalf("Schema() error = %v", err)
		}
		if schema != expectedSchema {
			t.Fatalf("Schema() = %q, want %q", schema, expectedSchema)
		}
	})
}

func TestProviderMetadata(t *testing.T) {
	t.Run("delegates global metadata", func(t *testing.T) {
		expected := &providersdk.Metadata{
			Tables: []*providerv1.TableMetadata{{Name: "projects"}},
		}
		cachedProvider, _ := Wrap(
			&testMetadataProvider{
				testProvider: &testProvider{},
				metadataFunc: func(context.Context) (*providersdk.Metadata, error) {
					return expected, nil
				},
			},
			Config{},
		)

		metadata, err := cachedProvider.Metadata(context.Background())
		if err != nil {
			t.Fatalf("Metadata() error = %v", err)
		}
		if !proto.Equal(metadata, expected) {
			t.Fatalf("Metadata() = %v, want %v", metadata, expected)
		}
	})

	t.Run("returns nil when unsupported", func(t *testing.T) {
		cachedProvider, _ := Wrap(&testProvider{}, Config{})

		metadata, err := cachedProvider.Metadata(context.Background())
		if err != nil {
			t.Fatalf("Metadata() error = %v", err)
		}
		if metadata != nil {
			t.Fatalf("Metadata() = %v, want nil", metadata)
		}
	})
}

func TestQueryCachesCompletedStream(t *testing.T) {
	var queryCalls atomic.Int32
	expected := testBatch("original")
	connection := &testConnection{
		queryFunc: func(
			context.Context,
			*providerv1.QueryRequest,
		) (providersdk.ResultStream, error) {
			queryCalls.Add(1)
			return &testStream{batches: []*providerv1.TupleBatch{expected}}, nil
		},
	}
	cachedProvider, _ := Wrap(&testProvider{connection: connection}, Config{})
	cachedConnection := openTestConnection(t, cachedProvider, "source-a")

	first := queryAndClose(t, cachedConnection, testQuery("transport-1", "TASK"))
	if !proto.Equal(first[0], expected) {
		t.Fatalf("first query batch = %v, want %v", first[0], expected)
	}

	second := queryAndClose(t, cachedConnection, testQuery("transport-2", "TASK"))
	if queryCalls.Load() != 1 {
		t.Fatalf("underlying Query calls = %d, want 1", queryCalls.Load())
	}
	if !proto.Equal(second[0], expected) {
		t.Fatalf("cached query batch = %v, want %v", second[0], expected)
	}

	second[0].Tuples[0].Values[0] = stringValue("changed")
	third := queryAndClose(t, cachedConnection, testQuery("transport-3", "TASK"))
	if got := third[0].GetTuples()[0].GetValues()[0].GetStringValue(); got != "original" {
		t.Fatalf("cached batch was mutated: value = %q", got)
	}
}

func TestQueryCacheSeparatesLogicalNamespaces(t *testing.T) {
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	cachedProvider, _ := Wrap(&testProvider{connection: connection}, Config{})

	first := openTestConnection(t, cachedProvider, "source-a")
	second := openTestConnection(t, cachedProvider, "source-b")
	queryAndClose(t, first, testNamespacedQuery("operations", "TASK"))
	queryAndClose(t, second, testNamespacedQuery("analytics", "TASK"))

	if queryCalls.Load() != 2 {
		t.Fatalf("underlying Query calls = %d, want 2", queryCalls.Load())
	}
}

func TestEntityNamespaceIsAnOpaqueCacheIdentity(t *testing.T) {
	pathKey, err := normalizedEntityKey(&providerv1.EntityReference{
		Name:      "TASK",
		Namespace: "some/path/to/resource",
	})
	if err != nil {
		t.Fatalf("normalizedEntityKey() error = %v", err)
	}
	caseVariantKey, err := normalizedEntityKey(&providerv1.EntityReference{
		Name:      "TASK",
		Namespace: "SOME/path/to/resource",
	})
	if err != nil {
		t.Fatalf("normalizedEntityKey() case variant error = %v", err)
	}
	if pathKey == caseVariantKey {
		t.Fatal("opaque namespace values produced the same cache identity")
	}
}

func TestControllerInvalidatesByGeneration(t *testing.T) {
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	cachedProvider, controller := Wrap(
		&testProvider{connection: connection},
		Config{},
	)
	cachedConnection := openTestConnection(t, cachedProvider, "source-a")
	request := testQuery("", "TASK")

	queryAndClose(t, cachedConnection, request)
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 1 {
		t.Fatalf("underlying Query calls before invalidation = %d, want 1", queryCalls.Load())
	}
	if err := controller.Invalidate(context.Background(), Invalidation{
		Entities: []*providerv1.EntityReference{testEntity("PROJECT")},
	}); err != nil {
		t.Fatalf("Invalidate(other entity) error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 1 {
		t.Fatalf("other entity invalidated TASK cache: Query calls = %d", queryCalls.Load())
	}

	if err := controller.Invalidate(context.Background(), Invalidation{
		Entities: []*providerv1.EntityReference{testEntity("task")},
	}); err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 2 {
		t.Fatalf("underlying Query calls after entity invalidation = %d, want 2", queryCalls.Load())
	}

	if err := controller.InvalidateAll(context.Background()); err != nil {
		t.Fatalf("InvalidateAll() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 3 {
		t.Fatalf("underlying Query calls after global invalidation = %d, want 3", queryCalls.Load())
	}

	if err := controller.InvalidateAll(context.Background()); err != nil {
		t.Fatalf("InvalidateAll() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 4 {
		t.Fatalf("underlying Query calls after global invalidation = %d, want 4", queryCalls.Load())
	}
}

func TestSuccessfulMutationsInvalidateAffectedEntity(t *testing.T) {
	mutationErr := errors.New("mutation failed")
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	connection.insertFunc = func(
		context.Context,
		*providerv1.InsertRequest,
	) (*providerv1.InsertResponse, error) {
		return &providerv1.InsertResponse{}, nil
	}
	connection.updateFunc = func(
		context.Context,
		*providerv1.UpdateRequest,
	) (*providerv1.UpdateResponse, error) {
		return nil, mutationErr
	}
	connection.deleteFunc = func(
		context.Context,
		*providerv1.DeleteRequest,
	) (*providerv1.DeleteResponse, error) {
		return &providerv1.DeleteResponse{}, nil
	}

	cachedProvider, _ := Wrap(&testProvider{connection: connection}, Config{})
	cachedConnection := openTestConnection(t, cachedProvider, "source")
	request := testQuery("", "TASK")
	queryAndClose(t, cachedConnection, request)
	queryAndClose(t, cachedConnection, request)

	if _, err := cachedConnection.Insert(
		context.Background(),
		&providerv1.InsertRequest{Entity: testEntity("task")},
	); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 2 {
		t.Fatalf("Query calls after Insert = %d, want 2", queryCalls.Load())
	}

	if _, err := cachedConnection.Update(
		context.Background(),
		&providerv1.UpdateRequest{Entity: testEntity("TASK")},
	); !errors.Is(err, mutationErr) {
		t.Fatalf("Update() error = %v, want %v", err, mutationErr)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 2 {
		t.Fatalf("failed Update invalidated cache: Query calls = %d", queryCalls.Load())
	}

	if _, err := cachedConnection.Delete(
		context.Background(),
		&providerv1.DeleteRequest{Entity: testEntity("TASK")},
	); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 3 {
		t.Fatalf("Query calls after Delete = %d, want 3", queryCalls.Load())
	}
}

func TestEntityDependenciesAreInvalidatedTransitively(t *testing.T) {
	queryCalls := make(map[string]int)
	connection := &testConnection{
		queryFunc: func(
			_ context.Context,
			request *providerv1.QueryRequest,
		) (providersdk.ResultStream, error) {
			entity, _ := normalizedEntityKey(request.GetEntity())
			queryCalls[entity]++
			return &testStream{batches: []*providerv1.TupleBatch{
				testBatch(entity),
			}}, nil
		},
		insertFunc: func(
			context.Context,
			*providerv1.InsertRequest,
		) (*providerv1.InsertResponse, error) {
			return &providerv1.InsertResponse{}, nil
		},
	}
	cachedProvider, _ := Wrap(
		&testProvider{connection: connection},
		Config{Dependencies: []Dependency{
			{Entity: testEntity("task"), Dependents: []*providerv1.EntityReference{testEntity("project_summary")}},
			{Entity: testEntity("PROJECT_SUMMARY"), Dependents: []*providerv1.EntityReference{testEntity("dashboard")}},
			{Entity: testEntity("DASHBOARD"), Dependents: []*providerv1.EntityReference{testEntity("TASK")}},
		}},
	)
	cachedConnection := openTestConnection(t, cachedProvider, "source")

	for _, entity := range []string{"PROJECT_SUMMARY", "DASHBOARD", "AUDIT"} {
		queryAndClose(t, cachedConnection, testQuery("", entity))
		queryAndClose(t, cachedConnection, testQuery("", entity))
	}
	if _, err := cachedConnection.Insert(
		context.Background(),
		&providerv1.InsertRequest{Entity: testEntity("TASK")},
	); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	for _, entity := range []string{"PROJECT_SUMMARY", "DASHBOARD", "AUDIT"} {
		queryAndClose(t, cachedConnection, testQuery("", entity))
	}
	projectSummary, _ := normalizedEntityKey(testEntity("PROJECT_SUMMARY"))
	dashboard, _ := normalizedEntityKey(testEntity("DASHBOARD"))
	audit, _ := normalizedEntityKey(testEntity("AUDIT"))
	if queryCalls[projectSummary] != 2 {
		t.Fatalf("PROJECT_SUMMARY Query calls = %d, want 2", queryCalls[projectSummary])
	}
	if queryCalls[dashboard] != 2 {
		t.Fatalf("DASHBOARD Query calls = %d, want 2", queryCalls[dashboard])
	}
	if queryCalls[audit] != 1 {
		t.Fatalf("AUDIT Query calls = %d, want 1", queryCalls[audit])
	}
}

func TestTransactionBypassesCacheAndInvalidatesOnCommit(t *testing.T) {
	var queryCalls atomic.Int32
	active := false
	connection := queryTestConnection(&queryCalls)
	connection.beginFunc = func(context.Context) error {
		active = true
		return nil
	}
	connection.commitFunc = func(context.Context) error {
		active = false
		return nil
	}
	connection.rollbackFunc = func(context.Context) error {
		active = false
		return nil
	}
	connection.inTransactionFunc = func(context.Context) (bool, error) {
		return active, nil
	}
	connection.insertFunc = func(
		context.Context,
		*providerv1.InsertRequest,
	) (*providerv1.InsertResponse, error) {
		return &providerv1.InsertResponse{}, nil
	}

	cachedProvider, _ := Wrap(&testProvider{connection: connection}, Config{})
	cachedConnection := openTestConnection(t, cachedProvider, "source")
	request := testQuery("", "TASK")
	queryAndClose(t, cachedConnection, request)

	if err := cachedConnection.Begin(context.Background()); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 3 {
		t.Fatalf("transactional Query calls = %d, want 3", queryCalls.Load())
	}
	if _, err := cachedConnection.Insert(
		context.Background(),
		&providerv1.InsertRequest{Entity: testEntity("TASK")},
	); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := cachedConnection.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 3 {
		t.Fatalf("Rollback invalidated shared cache: Query calls = %d", queryCalls.Load())
	}

	if err := cachedConnection.Begin(context.Background()); err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if _, err := cachedConnection.Insert(
		context.Background(),
		&providerv1.InsertRequest{Entity: testEntity("TASK")},
	); err != nil {
		t.Fatalf("second Insert() error = %v", err)
	}
	if err := cachedConnection.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 4 {
		t.Fatalf("Query calls after Commit = %d, want 4", queryCalls.Load())
	}
}

func TestUncertainTransactionOutcomeInvalidatesPendingChanges(t *testing.T) {
	commitErr := errors.New("commit outcome unknown")
	rollbackErr := errors.New("rollback outcome unknown")
	closeErr := errors.New("close outcome unknown")

	tests := []struct {
		name      string
		operation func(providersdk.Connection) error
	}{
		{
			name: "commit",
			operation: func(connection providersdk.Connection) error {
				return connection.Commit(context.Background())
			},
		},
		{
			name: "rollback",
			operation: func(connection providersdk.Connection) error {
				return connection.Rollback(context.Background())
			},
		},
		{
			name: "close",
			operation: func(connection providersdk.Connection) error {
				return connection.Close(context.Background())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &testConnection{
				beginFunc:    func(context.Context) error { return nil },
				commitFunc:   func(context.Context) error { return commitErr },
				rollbackFunc: func(context.Context) error { return rollbackErr },
				closeFunc:    func(context.Context) error { return closeErr },
				insertFunc: func(
					context.Context,
					*providerv1.InsertRequest,
				) (*providerv1.InsertResponse, error) {
					return &providerv1.InsertResponse{}, nil
				},
			}
			cachedProvider, _ := Wrap(
				&testProvider{connection: connection},
				Config{},
			)
			wrapped := openTestConnection(t, cachedProvider, "source")
			cached := wrapped.(*cachedConnection)

			if err := wrapped.Begin(context.Background()); err != nil {
				t.Fatalf("Begin() error = %v", err)
			}
			if _, err := wrapped.Insert(
				context.Background(),
				&providerv1.InsertRequest{Entity: testEntity("TASK")},
			); err != nil {
				t.Fatalf("Insert() error = %v", err)
			}
			entity, _ := normalizedEntityKey(testEntity("TASK"))
			before := cached.state.generation(entity)
			if err := test.operation(wrapped); err == nil {
				t.Fatal("transaction operation error = nil, want uncertainty error")
			}
			after := cached.state.generation(entity)
			if after.entity <= before.entity {
				t.Fatalf("entity generation = %d, want greater than %d", after.entity, before.entity)
			}
		})
	}
}

func TestInvalidationPreventsStaleFillPublication(t *testing.T) {
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	cachedProvider, controller := Wrap(
		&testProvider{connection: connection},
		Config{},
	)
	cachedConnection := openTestConnection(t, cachedProvider, "source-a")
	request := testQuery("", "TASK")

	stream, err := cachedConnection.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("Next() batch error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("Next() terminal error = %v, want io.EOF", err)
	}
	if err := controller.Invalidate(context.Background(), Invalidation{
		Entities: []*providerv1.EntityReference{testEntity("TASK")},
	}); err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	queryAndClose(t, cachedConnection, request)
	if queryCalls.Load() != 2 {
		t.Fatalf("underlying Query calls = %d, want 2", queryCalls.Load())
	}
}

func TestOversizedQueryIsNotCached(t *testing.T) {
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	cachedProvider, _ := Wrap(
		&testProvider{connection: connection},
		Config{
			MaxBytes:      1_024,
			MaxEntryBytes: 1,
		},
	)
	cachedConnection := openTestConnection(t, cachedProvider, "source")
	request := testQuery("", "TASK")

	queryAndClose(t, cachedConnection, request)
	queryAndClose(t, cachedConnection, request)

	if queryCalls.Load() != 2 {
		t.Fatalf("underlying Query calls = %d, want 2", queryCalls.Load())
	}
}

func TestControllerValidatesRequestContextAndEntities(t *testing.T) {
	_, controller := Wrap(&testProvider{}, Config{})

	if err := controller.Invalidate(
		context.Background(),
		Invalidation{},
	); err == nil {
		t.Fatal("Invalidate() error = nil, want missing entity error")
	}
	if err := controller.Invalidate(
		context.Background(),
		Invalidation{Entities: []*providerv1.EntityReference{testEntity(" ")}},
	); err == nil {
		t.Fatal("Invalidate() error = nil, want blank entity error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.InvalidateAll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("InvalidateAll() error = %v, want context.Canceled", err)
	}
}

func TestIncompleteOrFailedStreamIsNotCached(t *testing.T) {
	streamErr := errors.New("stream failed")
	closeErr := errors.New("close failed")

	tests := []struct {
		name   string
		stream func() *testStream
		drain  bool
	}{
		{
			name: "closed before EOF",
			stream: func() *testStream {
				return &testStream{batches: []*providerv1.TupleBatch{testBatch("value")}}
			},
		},
		{
			name: "read failure",
			stream: func() *testStream {
				return &testStream{nextErr: streamErr}
			},
			drain: true,
		},
		{
			name: "close failure",
			stream: func() *testStream {
				return &testStream{closeErr: closeErr}
			},
			drain: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var queryCalls atomic.Int32
			connection := &testConnection{
				queryFunc: func(
					context.Context,
					*providerv1.QueryRequest,
				) (providersdk.ResultStream, error) {
					queryCalls.Add(1)
					return test.stream(), nil
				},
			}
			cachedProvider, _ := Wrap(
				&testProvider{connection: connection},
				Config{},
			)
			cachedConnection := openTestConnection(t, cachedProvider, "source")
			request := testQuery("", "TASK")

			stream, err := cachedConnection.Query(context.Background(), request)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if test.drain {
				_, _ = stream.Next(context.Background())
			}
			_ = stream.Close()

			stream, err = cachedConnection.Query(context.Background(), request)
			if err != nil {
				t.Fatalf("second Query() error = %v", err)
			}
			_ = stream.Close()
			if queryCalls.Load() != 2 {
				t.Fatalf("underlying Query calls = %d, want 2", queryCalls.Load())
			}
		})
	}
}

func TestConcurrentQueriesAndInvalidations(t *testing.T) {
	var queryCalls atomic.Int32
	connection := queryTestConnection(&queryCalls)
	cachedProvider, controller := Wrap(
		&testProvider{connection: connection},
		Config{TTL: time.Minute},
	)
	cachedConnection := openTestConnection(t, cachedProvider, "source")
	request := testQuery("", "TASK")

	start := make(chan struct{})
	errorsChannel := make(chan error, 64)
	var waitGroup sync.WaitGroup

	for index := 0; index < 32; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start

			stream, err := cachedConnection.Query(context.Background(), request)
			if err != nil {
				errorsChannel <- err
				return
			}
			for {
				_, err = stream.Next(context.Background())
				if err == io.EOF {
					break
				}
				if err != nil {
					errorsChannel <- err
					return
				}
			}
			if err := stream.Close(); err != nil {
				errorsChannel <- err
			}
		}()
	}

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		<-start
		for index := 0; index < 32; index++ {
			if err := controller.Invalidate(context.Background(), Invalidation{
				Entities: []*providerv1.EntityReference{testEntity("TASK")},
			}); err != nil {
				errorsChannel <- err
				return
			}
		}
	}()

	close(start)
	waitGroup.Wait()
	close(errorsChannel)

	for err := range errorsChannel {
		t.Fatalf("concurrent operation error = %v", err)
	}
}

func queryTestConnection(queryCalls *atomic.Int32) *testConnection {
	return &testConnection{
		queryFunc: func(
			context.Context,
			*providerv1.QueryRequest,
		) (providersdk.ResultStream, error) {
			call := queryCalls.Add(1)
			return &testStream{
				batches: []*providerv1.TupleBatch{
					testBatch(stringValueForCall(call)),
				},
			}, nil
		},
	}
}

func stringValueForCall(call int32) string {
	return string(rune('a' + call - 1))
}

func openTestConnection(
	t *testing.T,
	provider *Provider,
	_ string,
) providersdk.Connection {
	t.Helper()

	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	return connection
}

func queryAndClose(
	t *testing.T,
	connection providersdk.Connection,
	request *providerv1.QueryRequest,
) []*providerv1.TupleBatch {
	t.Helper()

	stream, err := connection.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	var batches []*providerv1.TupleBatch
	for {
		batch, err := stream.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		batches = append(batches, batch)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return batches
}

func testQuery(
	connectionID string,
	entity string,
) *providerv1.QueryRequest {
	return &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       &providerv1.EntityReference{Name: entity},
	}
}

func testEntity(name string) *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: name}
}

func testNamespacedQuery(
	namespace string,
	entity string,
) *providerv1.QueryRequest {
	return &providerv1.QueryRequest{Entity: &providerv1.EntityReference{
		Name:      entity,
		Namespace: namespace,
	}}
}

func testBatch(value string) *providerv1.TupleBatch {
	return &providerv1.TupleBatch{
		Fields: []*providerv1.Field{
			{
				Name: "value",
				Type: kublingv1.ValueType_VALUE_TYPE_STRING,
			},
		},
		Tuples: []*providerv1.Tuple{
			{Values: []*kublingv1.Value{stringValue(value)}},
		},
	}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{
		Kind: &kublingv1.Value_StringValue{StringValue: value},
	}
}
