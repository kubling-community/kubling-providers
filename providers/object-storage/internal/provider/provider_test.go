package provider

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExactQueryAndRangedLobRead(t *testing.T) {
	implementation, store := testProvider(t, false)
	store.seed("docs/hello.txt", []byte("hello"), "text/plain")
	connection := openConnection(t, implementation)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: entity(),
		Projections: []*providerv1.Projection{
			fieldProjection(catalog.KeyColumn),
			fieldProjection(catalog.ContentColumn),
		},
		Filter:           keyFilter("docs/hello.txt"),
		AcceptedFeatures: []string{grpcfeatures.LobReadV1},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got := batch.GetTuples()[0].GetValues()[0].GetStringValue(); got != "docs/hello.txt" {
		t.Fatalf("key = %q", got)
	}
	reference := batch.GetTuples()[0].GetValues()[1].GetLobReference()
	if reference == nil || reference.GetSizeBytes() != 5 {
		t.Fatalf("LOB reference = %+v", reference)
	}
	length := uint64(3)
	lobStream, err := connection.ReadLob(context.Background(), &providerv1.ReadLobRequest{
		LobId: reference.GetLobId(), Offset: 1, Length: &length, MaxChunkBytes: 2,
	})
	if err != nil {
		t.Fatalf("ReadLob() error = %v", err)
	}
	first, err := lobStream.Next(context.Background())
	if err != nil || string(first.GetData()) != "el" || first.GetEndOfRead() {
		t.Fatalf("first chunk = %q/%t, %v", first.GetData(), first.GetEndOfRead(), err)
	}
	second, err := lobStream.Next(context.Background())
	if err != nil || string(second.GetData()) != "l" || !second.GetEndOfRead() {
		t.Fatalf("second chunk = %q/%t, %v", second.GetData(), second.GetEndOfRead(), err)
	}
	if store.lastOpen.Revision == "" {
		t.Fatal("LOB read did not preserve the object ETag")
	}
}

func TestProviderCapabilitiesAndSemanticFragmentPassSDKValidation(t *testing.T) {
	implementation, _ := testProvider(t, false)
	server := providersdk.NewServer(implementation)
	capabilities, err := server.GetCapabilities(
		context.Background(),
		&providerv1.GetCapabilitiesRequest{},
	)
	if err != nil {
		t.Fatalf("GetCapabilities() error = %v", err)
	}
	if !capabilities.GetMutations().GetInsert() ||
		!capabilities.GetMutations().GetUpdate() ||
		!capabilities.GetMutations().GetDelete() {
		t.Fatalf("mutation capabilities = %+v", capabilities.GetMutations())
	}
	schema, err := server.GetSchema(context.Background(), &providerv1.GetSchemaRequest{})
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if len(schema.GetMetadata().GetTables()) != 2 {
		t.Fatalf("schema tables = %d", len(schema.GetMetadata().GetTables()))
	}
	fragment, err := server.GetSemanticFragment(
		context.Background(),
		&providerv1.GetSemanticFragmentRequest{},
	)
	if err != nil {
		t.Fatalf("GetSemanticFragment() error = %v", err)
	}
	if fragment.GetFragment() == nil || fragment.GetFragment().GetDigest() == "" {
		t.Fatalf("semantic fragment = %+v", fragment.GetFragment())
	}
}

func TestQueryRejectsUnboundedListingAndContentWithoutFeature(t *testing.T) {
	implementation, _ := testProvider(t, false)
	connection := openConnection(t, implementation)

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      entity(),
		Projections: []*providerv1.Projection{fieldProjection(catalog.KeyColumn)},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unbounded Query() error = %v", err)
	}
	_, err = connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      entity(),
		Projections: []*providerv1.Projection{fieldProjection(catalog.ContentColumn)},
		Filter:      keyFilter("one.bin"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("content Query() error = %v", err)
	}
}

func TestListingUsesCompleteBackendFieldsWithoutStat(t *testing.T) {
	implementation, store := testProvider(t, true)
	store.seed("one.txt", []byte("one"), "text/plain")
	store.objects["one.txt"] = storedObject{
		object: backend.Object{
			Key: "one.txt", Size: 3, ETag: "etag-one.txt", Revision: "etag-one.txt",
			ContentType: "text/plain", LastModified: time.Now().UTC(),
			Metadata: map[string]string{"stage": "reviewed"},
		},
		data: []byte("one"),
	}
	connection := openConnection(t, implementation)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: entity(),
		Projections: []*providerv1.Projection{
			fieldProjection(catalog.KeyColumn),
			fieldProjection(catalog.MetadataColumn),
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "one.txt" ||
		values[1].GetJsonValue() != `{"stage":"reviewed"}` {
		t.Fatalf("row = %+v", values)
	}
	if got := store.statCallCount(); got != 0 {
		t.Fatalf("Stat() calls = %d, want 0", got)
	}
}

func TestListingStatsWhenBackendDoesNotGuaranteeProjectedFields(t *testing.T) {
	implementation, store := testProvider(t, true)
	store.seed("one.txt", []byte("one"), "text/plain")
	store.listFields &^= backend.ObjectFieldMetadata
	connection := openConnection(t, implementation)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      entity(),
		Projections: []*providerv1.Projection{fieldProjection(catalog.MetadataColumn)},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got := store.statCallCount(); got != 1 {
		t.Fatalf("Stat() calls = %d, want 1", got)
	}
}

func TestAllowedListingUsesLogicalNamespace(t *testing.T) {
	implementation, store := testProvider(t, true)
	store.seed("b.txt", []byte("b"), "text/plain")
	store.seed("a.txt", []byte("a"), "text/plain")
	connection := openConnection(t, implementation)
	limit := uint64(1)
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: entity(),
		Projections: []*providerv1.Projection{
			fieldProjection(catalog.BucketColumn),
			fieldProjection(catalog.KeyColumn),
		},
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "media" || values[1].GetStringValue() != "a.txt" {
		t.Fatalf("row = %q/%q", values[0].GetStringValue(), values[1].GetStringValue())
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next() error = %v", err)
	}
}

func TestInsertUpdateAndDelete(t *testing.T) {
	implementation, store := testProvider(t, false)
	connection := openConnection(t, implementation)

	inserted, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: entity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: catalog.KeyColumn},
				{Name: catalog.ContentColumn},
				{Name: catalog.ContentTypeColumn},
			},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("images/logo.bin"),
				blobValue([]byte("old")),
				stringValue("application/octet-stream"),
			}}},
		},
	})
	if err != nil || inserted.GetAffectedRows() != 1 {
		t.Fatalf("Insert() = %+v, %v", inserted, err)
	}
	if _, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: entity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: catalog.KeyColumn}, {Name: catalog.ContentColumn}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue("images/logo.bin"), blobValue([]byte("duplicate")),
			}}},
		},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate Insert() error = %v", err)
	}

	updated, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: entity(),
		Assignments: []*providerv1.Assignment{{
			Field: catalog.ContentColumn,
			Value: literal(blobValue([]byte("new"))),
		}},
		Filter: keyFilter("images/logo.bin"),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("Update() = %+v, %v", updated, err)
	}
	if got := string(store.data("images/logo.bin")); got != "new" {
		t.Fatalf("updated content = %q", got)
	}

	updated, err = connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: entity(),
		Assignments: []*providerv1.Assignment{
			{
				Field: catalog.CacheControlColumn,
				Value: literal(stringValue("public, max-age=60")),
			},
			{
				Field: catalog.MetadataColumn,
				Value: literal(&kublingv1.Value{Kind: &kublingv1.Value_JsonValue{
					JsonValue: `{"owner":"media","stage":"reviewed"}`,
				}}),
			},
		},
		Filter: keyFilter("images/logo.bin"),
	})
	if err != nil || updated.GetAffectedRows() != 1 {
		t.Fatalf("attribute Update() = %+v, %v", updated, err)
	}
	if got := string(store.data("images/logo.bin")); got != "new" {
		t.Fatalf("attribute update changed content to %q", got)
	}
	if !store.lastAttributeUpdate.Fields.CacheControl ||
		!store.lastAttributeUpdate.Fields.Metadata ||
		store.lastAttributeUpdate.IfRevisionMatch == "" {
		t.Fatalf("attribute update request = %+v", store.lastAttributeUpdate)
	}
	attributes, err := store.Stat(context.Background(), backend.Bucket{}, "images/logo.bin")
	if err != nil || attributes.CacheControl != "public, max-age=60" ||
		attributes.Metadata["stage"] != "reviewed" {
		t.Fatalf("attribute update object = %+v, %v", attributes, err)
	}

	deleted, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: entity(),
		Filter: keyFilter("images/logo.bin"),
	})
	if err != nil || deleted.GetAffectedRows() != 1 {
		t.Fatalf("Delete() = %+v, %v", deleted, err)
	}
	if store.exists("images/logo.bin") {
		t.Fatal("deleted object still exists")
	}
	if store.lastDelete.IfRevisionMatch == "" || store.lastDelete.VersionID != "" {
		t.Fatalf("delete condition = %+v", store.lastDelete)
	}
}

func TestDeleteRejectsObjectWithoutRevision(t *testing.T) {
	implementation, store := testProvider(t, false)
	store.objects["without-revision.bin"] = storedObject{
		object: backend.Object{Key: "without-revision.bin", Size: 1},
		data:   []byte("x"),
	}
	connection := openConnection(t, implementation)

	_, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: entity(),
		Filter: keyFilter("without-revision.bin"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Delete() error = %v", err)
	}
	if store.lastDelete.Key != "" {
		t.Fatalf("backend Delete() request = %+v", store.lastDelete)
	}
}

func testProvider(t *testing.T, allowUnbounded bool) (*Provider, *memoryStore) {
	t.Helper()
	configuration := config.Config{
		Lobs: config.Lobs{
			MaxBytes:      1024 * 1024,
			MaxChunkBytes: 2,
			Retention:     time.Minute,
		},
		Backends: map[string]config.Backend{
			"primary": {Type: "s3", Region: "test"},
		},
		Buckets: map[string]config.Bucket{
			"media": {
				Backend: "primary", Name: "physical", Prefix: "root/",
				AllowUnboundedList: allowUnbounded,
			},
			"archive": {
				Backend: "primary", Name: "physical-archive",
			},
		},
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	store := &memoryStore{
		objects:    make(map[string]storedObject),
		listFields: backend.AllObjectFields,
	}
	implementation, err := New(configuration, compiled, map[string]backend.Store{"primary": store})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return implementation, store
}

func openConnection(t *testing.T, implementation *Provider) *Connection {
	t.Helper()
	opened, err := implementation.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return opened.(*Connection)
}

func entity() *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: catalog.ObjectsTable, Namespace: "media"}
}

func fieldProjection(name string) *providerv1.Projection {
	return &providerv1.Projection{Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
		Field: &providerv1.FieldReference{Name: name},
	}}}
}

func keyFilter(key string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: catalog.KeyColumn},
			}},
			Right: literal(stringValue(key)),
		},
	}}
}

func literal(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: value},
	}}
}

func blobValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BlobValue{
		BlobValue: &kublingv1.BlobValue{Data: value},
	}}
}

type storedObject struct {
	object backend.Object
	data   []byte
}

type memoryStore struct {
	mu                  sync.Mutex
	objects             map[string]storedObject
	listFields          backend.ObjectFields
	statCalls           int
	lastOpen            backend.GetRequest
	lastDelete          backend.DeleteRequest
	lastAttributeUpdate backend.UpdateAttributesRequest
}

func (s *memoryStore) seed(key string, data []byte, contentType string) {
	s.mu.Lock()
	s.objects[key] = storedObject{
		object: backend.Object{
			Key: key, Size: int64(len(data)), ETag: "etag-" + key, Revision: "etag-" + key,
			ContentType: contentType, LastModified: time.Now().UTC(),
		},
		data: append([]byte(nil), data...),
	}
	s.mu.Unlock()
}

func (s *memoryStore) Probe(context.Context, backend.Bucket) error { return nil }

func (s *memoryStore) List(
	_ context.Context,
	_ backend.Bucket,
	request backend.ListRequest,
) (backend.ListPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, request.Prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	start := 0
	if request.ContinuationToken != "" {
		for index, key := range keys {
			if key == request.ContinuationToken {
				start = index
				break
			}
		}
	}
	end := len(keys)
	if request.Limit > 0 && start+int(request.Limit) < end {
		end = start + int(request.Limit)
	}
	page := backend.ListPage{Fields: s.listFields}
	for _, key := range keys[start:end] {
		page.Objects = append(page.Objects, s.objects[key].object)
	}
	if end < len(keys) {
		page.ContinuationToken = keys[end]
	}
	return page, nil
}

func (s *memoryStore) Stat(_ context.Context, _ backend.Bucket, key string) (backend.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statCalls++
	object, found := s.objects[key]
	if !found {
		return backend.Object{}, backend.ErrNotFound
	}
	return object.object, nil
}

func (s *memoryStore) Open(
	_ context.Context,
	_ backend.Bucket,
	request backend.GetRequest,
) (backend.ObjectReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, found := s.objects[request.Key]
	if !found {
		return backend.ObjectReader{}, backend.ErrNotFound
	}
	s.lastOpen = request
	end := int64(len(object.data))
	if request.Length != nil && request.Offset+*request.Length < end {
		end = request.Offset + *request.Length
	}
	return backend.ObjectReader{
		Object: object.object,
		Body:   io.NopCloser(bytes.NewReader(object.data[request.Offset:end])),
	}, nil
}

func (s *memoryStore) Put(
	_ context.Context,
	_ backend.Bucket,
	request backend.PutRequest,
) (backend.Object, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return backend.Object{}, err
	}
	s.mu.Lock()
	current, exists := s.objects[request.Key]
	if request.IfNoneMatch && exists {
		s.mu.Unlock()
		return backend.Object{}, backend.ErrConflict
	}
	if request.IfRevisionMatch != "" &&
		(!exists || current.object.Revision != request.IfRevisionMatch) {
		s.mu.Unlock()
		return backend.Object{}, backend.ErrConflict
	}
	s.mu.Unlock()
	s.mu.Lock()
	s.objects[request.Key] = storedObject{
		object: backend.Object{
			Key:                request.Key,
			Size:               int64(len(data)),
			ETag:               "etag-" + request.Key,
			Revision:           "etag-" + request.Key,
			LastModified:       time.Now().UTC(),
			ContentType:        request.ContentType,
			ContentEncoding:    request.ContentEncoding,
			ContentDisposition: request.ContentDisposition,
			CacheControl:       request.CacheControl,
			Metadata:           cloneStringMap(request.Metadata),
		},
		data: append([]byte(nil), data...),
	}
	s.mu.Unlock()
	return s.Stat(context.Background(), backend.Bucket{}, request.Key)
}

func (s *memoryStore) UpdateAttributes(
	_ context.Context,
	_ backend.Bucket,
	request backend.UpdateAttributesRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAttributeUpdate = request
	current, exists := s.objects[request.Key]
	if !exists {
		return backend.ErrNotFound
	}
	if request.IfRevisionMatch != "" && current.object.Revision != request.IfRevisionMatch {
		return backend.ErrConflict
	}
	if request.Fields.ContentType {
		current.object.ContentType = request.ContentType
	}
	if request.Fields.ContentEncoding {
		current.object.ContentEncoding = request.ContentEncoding
	}
	if request.Fields.ContentDisposition {
		current.object.ContentDisposition = request.ContentDisposition
	}
	if request.Fields.CacheControl {
		current.object.CacheControl = request.CacheControl
	}
	if request.Fields.Metadata {
		current.object.Metadata = cloneStringMap(request.Metadata)
	}
	current.object.Revision += "-attributes"
	current.object.LastModified = time.Now().UTC()
	s.objects[request.Key] = current
	return nil
}

func (s *memoryStore) Delete(
	_ context.Context,
	_ backend.Bucket,
	request backend.DeleteRequest,
) error {
	s.mu.Lock()
	s.lastDelete = request
	current, exists := s.objects[request.Key]
	if !exists {
		s.mu.Unlock()
		return backend.ErrNotFound
	}
	if request.IfRevisionMatch != "" && current.object.Revision != request.IfRevisionMatch {
		s.mu.Unlock()
		return backend.ErrConflict
	}
	delete(s.objects, request.Key)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) data(key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.objects[key].data...)
}

func (s *memoryStore) exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.objects[key]
	return found
}

func (s *memoryStore) statCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statCalls
}
