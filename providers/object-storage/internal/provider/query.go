package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultBatchSize = 100
	maxListPageSize  = 1000
)

type projection struct {
	column string
	field  *providerv1.Field
}

type columnDefinition struct {
	name       string
	valueType  kublingv1.ValueType
	listFields backend.ObjectFields
}

var objectColumns = []columnDefinition{
	{name: catalog.BucketColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
	{name: catalog.KeyColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldKey},
	{name: catalog.ContentColumn, valueType: kublingv1.ValueType_VALUE_TYPE_BLOB, listFields: backend.ObjectFieldContent},
	{name: catalog.ContentTypeColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldContentType},
	{name: catalog.ContentEncodingColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldContentEncoding},
	{name: catalog.ContentDispositionColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldContentDisposition},
	{name: catalog.CacheControlColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldCacheControl},
	{name: catalog.ContentLengthColumn, valueType: kublingv1.ValueType_VALUE_TYPE_LONG, listFields: backend.ObjectFieldContentLength},
	{name: catalog.ETagColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldETag},
	{name: catalog.LastModifiedColumn, valueType: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, listFields: backend.ObjectFieldLastModified},
	{name: catalog.VersionIDColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldVersionID},
	{name: catalog.ChecksumColumn, valueType: kublingv1.ValueType_VALUE_TYPE_STRING, listFields: backend.ObjectFieldChecksum},
	{name: catalog.MetadataColumn, valueType: kublingv1.ValueType_VALUE_TYPE_JSON, listFields: backend.ObjectFieldMetadata},
}

// Query executes a key lookup or an explicitly enabled bucket listing.
func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := c.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	if request.Offset != nil || len(request.GetOrderBy()) != 0 ||
		len(request.GetGroupBy()) != 0 || request.GetHaving() != nil {
		return nil, status.Error(codes.InvalidArgument, "query shape is not supported")
	}
	route, store, err := c.route(request.GetEntity())
	if err != nil {
		return nil, err
	}
	identity, err := parseIdentityFilter(request.GetFilter(), route.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan object filter: %v", err)
	}
	if !identity.hasKey && !identity.noMatch && !route.AllowUnboundedList {
		return nil, status.Error(
			codes.FailedPrecondition,
			"unbounded object listing is disabled; constrain key with equality",
		)
	}
	projections, requiredListFields, err := planProjections(request)
	if err != nil {
		return nil, err
	}
	batchSize := int(request.GetBatchSize())
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchSize > maxListPageSize {
		batchSize = maxListPageSize
	}
	return &queryStream{
		connection:         c,
		store:              store,
		route:              route,
		identity:           identity,
		projections:        projections,
		requiredListFields: requiredListFields,
		batchSize:          batchSize,
		limit:              request.Limit,
	}, nil
}

func planProjections(
	request *providerv1.QueryRequest,
) ([]projection, backend.ObjectFields, error) {
	definitions := make(map[string]columnDefinition, len(objectColumns))
	for _, definition := range objectColumns {
		definitions[definition.name] = definition
	}
	requested := request.GetProjections()
	if len(requested) == 0 {
		requested = make([]*providerv1.Projection, 0, len(objectColumns))
		for _, definition := range objectColumns {
			requested = append(requested, &providerv1.Projection{
				Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{
					Field: &providerv1.FieldReference{Name: definition.name},
				}},
				OutputName: definition.name,
			})
		}
	}

	planned := make([]projection, 0, len(requested))
	var requiredListFields backend.ObjectFields
	for _, requestedProjection := range requested {
		if requestedProjection == nil || requestedProjection.GetExpression() == nil ||
			requestedProjection.GetExpression().GetField() == nil {
			return nil, 0, status.Error(codes.InvalidArgument, "only direct field projections are supported")
		}
		columnName := normalizeName(requestedProjection.GetExpression().GetField().GetName())
		definition, found := definitions[columnName]
		if !found {
			return nil, 0, status.Errorf(codes.InvalidArgument, "unknown object column %q", columnName)
		}
		if definition.name == catalog.ContentColumn &&
			!slices.Contains(request.GetAcceptedFeatures(), grpcfeatures.LobReadV1) {
			return nil, 0, status.Errorf(
				codes.FailedPrecondition,
				"content projection requires feature %q",
				grpcfeatures.LobReadV1,
			)
		}
		outputName := requestedProjection.GetOutputName()
		if outputName == "" {
			outputName = definition.name
		}
		planned = append(planned, projection{
			column: definition.name,
			field:  &providerv1.Field{Name: outputName, Type: definition.valueType},
		})
		requiredListFields |= definition.listFields
	}
	return planned, requiredListFields, nil
}

type queryStream struct {
	mu sync.Mutex

	connection         *Connection
	store              backend.Store
	route              catalog.Bucket
	identity           identityFilter
	projections        []projection
	requiredListFields backend.ObjectFields
	batchSize          int
	limit              *uint64

	pending           []backend.Object
	pendingIndex      int
	pendingFields     backend.ObjectFields
	continuationToken string
	emitted           uint64
	exactDone         bool
	exhausted         bool
	closed            bool
}

func (s *queryStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
	if err := s.connection.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer s.connection.unlockOpen()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.exhausted || s.limitReached() {
		return nil, io.EOF
	}
	if s.identity.noMatch {
		s.exhausted = true
		return nil, io.EOF
	}
	if s.identity.hasKey {
		return s.nextExact(ctx)
	}
	return s.nextList(ctx)
}

func (s *queryStream) nextExact(ctx context.Context) (*providerv1.TupleBatch, error) {
	if s.exactDone {
		return nil, io.EOF
	}
	s.exactDone = true
	object, err := s.store.Stat(ctx, s.route.Target, s.identity.key)
	if errors.Is(err, backend.ErrNotFound) {
		s.exhausted = true
		return nil, io.EOF
	}
	if err != nil {
		return nil, backendStatus("stat object", err)
	}
	tuple, err := s.tuple(object)
	if err != nil {
		return nil, err
	}
	s.emitted++
	s.exhausted = true
	return s.batch([]*providerv1.Tuple{tuple}), nil
}

func (s *queryStream) nextList(ctx context.Context) (*providerv1.TupleBatch, error) {
	tuples := make([]*providerv1.Tuple, 0, s.batchSize)
	for len(tuples) < s.batchSize && !s.limitReached() {
		if s.pendingIndex >= len(s.pending) {
			previousToken := s.continuationToken
			pageSize := s.pageSize(len(tuples))
			page, err := s.store.List(ctx, s.route.Target, backend.ListRequest{
				ContinuationToken: previousToken,
				Limit:             int32(pageSize),
			})
			if err != nil {
				return nil, backendStatus("list objects", err)
			}
			s.pending = page.Objects
			s.pendingIndex = 0
			s.pendingFields = page.Fields
			s.continuationToken = page.ContinuationToken
			if len(s.pending) == 0 {
				if s.continuationToken == "" {
					s.exhausted = true
					break
				}
				if s.continuationToken == previousToken {
					return nil, status.Error(codes.DataLoss, "object backend returned a non-progressing page")
				}
				continue
			}
		}

		object := s.pending[s.pendingIndex]
		s.pendingIndex++
		if !s.pendingFields.Contains(s.requiredListFields) {
			complete, err := s.store.Stat(ctx, s.route.Target, object.Key)
			if errors.Is(err, backend.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, backendStatus("stat listed object", err)
			}
			object = complete
		}
		tuple, err := s.tuple(object)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tuple)
		s.emitted++

		if s.pendingIndex >= len(s.pending) && s.continuationToken == "" {
			s.exhausted = true
			break
		}
	}
	if len(tuples) == 0 {
		return nil, io.EOF
	}
	return s.batch(tuples), nil
}

func (s *queryStream) pageSize(current int) int {
	size := s.batchSize - current
	if s.limit != nil {
		remaining := *s.limit - s.emitted
		if uint64(size) > remaining {
			size = int(remaining)
		}
	}
	if size > maxListPageSize {
		return maxListPageSize
	}
	return size
}

func (s *queryStream) limitReached() bool {
	return s.limit != nil && s.emitted >= *s.limit
}

func (s *queryStream) tuple(object backend.Object) (*providerv1.Tuple, error) {
	values := make([]*kublingv1.Value, 0, len(s.projections))
	for _, projection := range s.projections {
		value, err := s.value(object, projection.column)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return &providerv1.Tuple{Values: values}, nil
}

func (s *queryStream) value(object backend.Object, column string) (*kublingv1.Value, error) {
	switch column {
	case catalog.BucketColumn:
		return stringValue(s.route.Namespace), nil
	case catalog.KeyColumn:
		return stringValue(object.Key), nil
	case catalog.ContentColumn:
		return s.connection.newLobValue(s.route, object)
	case catalog.ContentTypeColumn:
		return nullableStringValue(object.ContentType), nil
	case catalog.ContentEncodingColumn:
		return nullableStringValue(object.ContentEncoding), nil
	case catalog.ContentDispositionColumn:
		return nullableStringValue(object.ContentDisposition), nil
	case catalog.CacheControlColumn:
		return nullableStringValue(object.CacheControl), nil
	case catalog.ContentLengthColumn:
		return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: object.Size}}, nil
	case catalog.ETagColumn:
		return nullableStringValue(object.ETag), nil
	case catalog.LastModifiedColumn:
		if object.LastModified.IsZero() {
			return nullValue(), nil
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{
			TimestampValue: object.LastModified.UTC().Format("2006-01-02T15:04:05.999999999"),
		}}, nil
	case catalog.VersionIDColumn:
		return nullableStringValue(object.VersionID), nil
	case catalog.ChecksumColumn:
		return nullableStringValue(object.Checksum), nil
	case catalog.MetadataColumn:
		if len(object.Metadata) == 0 {
			return nullValue(), nil
		}
		encoded, err := json.Marshal(object.Metadata)
		if err != nil {
			return nil, status.Error(codes.Internal, "encode object metadata")
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: string(encoded)}}, nil
	default:
		return nil, status.Errorf(codes.Internal, "unsupported planned column %q", column)
	}
}

func (s *queryStream) batch(tuples []*providerv1.Tuple) *providerv1.TupleBatch {
	fields := make([]*providerv1.Field, 0, len(s.projections))
	for _, projection := range s.projections {
		fields = append(fields, projection.field)
	}
	return &providerv1.TupleBatch{Fields: fields, Tuples: tuples}
}

func (s *queryStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.pending = nil
	s.mu.Unlock()
	return nil
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func nullableStringValue(value string) *kublingv1.Value {
	if value == "" {
		return nullValue()
	}
	return stringValue(value)
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}

var _ providersdk.ResultStream = (*queryStream)(nil)
