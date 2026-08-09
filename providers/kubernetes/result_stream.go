package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type kubernetesResultStream struct {
	mu            sync.Mutex
	resource      dynamic.ResourceInterface
	options       metav1.ListOptions
	projections   []queryProjection
	fields        []*providerv1.Field
	batchSize     int64
	remaining     uint64
	hasLimit      bool
	continueToken string
	done          bool
	closed        bool
	terminalErr   error
}

func newKubernetesResultStream(
	resource dynamic.ResourceInterface,
	options metav1.ListOptions,
	projections []queryProjection,
	batchSize int64,
	limit *uint64,
) providersdk.ResultStream {
	stream := &kubernetesResultStream{
		resource:    resource,
		options:     options,
		projections: projections,
		fields:      projectionFields(projections),
		batchSize:   batchSize,
	}
	if limit != nil {
		stream.hasLimit = true
		stream.remaining = *limit
	}
	return stream
}

func newEmptyKubernetesResultStream(fields []*providerv1.Field) providersdk.ResultStream {
	return &kubernetesResultStream{fields: fields, done: true}
}

func (s *kubernetesResultStream) Next(ctx context.Context) (*providerv1.TupleBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.closed || s.done {
		if s.terminalErr != nil {
			return nil, s.terminalErr
		}
		return nil, io.EOF
	}

	for {
		options := s.options
		options.Continue = s.continueToken
		options.Limit = s.batchSize
		if s.hasLimit && s.remaining < uint64(options.Limit) {
			options.Limit = int64(s.remaining)
		}
		if options.Limit == 0 {
			s.done = true
			return nil, io.EOF
		}

		list, err := s.resource.List(ctx, options)
		if err != nil {
			s.done = true
			s.terminalErr = kubernetesOperationError("list Kubernetes resources", err)
			return nil, s.terminalErr
		}
		if list == nil {
			s.done = true
			s.terminalErr = status.Error(codes.Internal, "Kubernetes dynamic client returned nil resource list")
			return nil, s.terminalErr
		}
		s.continueToken = list.GetContinue()

		tupleCount := len(list.Items)
		if s.hasLimit && uint64(tupleCount) > s.remaining {
			tupleCount = int(s.remaining)
		}
		tuples := make([]*providerv1.Tuple, 0, tupleCount)
		for index := 0; index < tupleCount; index++ {
			tuple, err := resourceTuple(&list.Items[index], s.projections)
			if err != nil {
				s.done = true
				s.terminalErr = status.Errorf(codes.Internal, "convert Kubernetes resource: %v", err)
				return nil, s.terminalErr
			}
			tuples = append(tuples, tuple)
		}
		if s.hasLimit {
			s.remaining -= uint64(len(tuples))
		}
		if s.continueToken == "" || s.hasLimit && s.remaining == 0 {
			s.done = true
		}
		if len(tuples) == 0 {
			if s.done {
				return nil, io.EOF
			}
			continue
		}
		return &providerv1.TupleBatch{Fields: s.fields, Tuples: tuples}, nil
	}
}

func (s *kubernetesResultStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.resource = nil
	s.projections = nil
	s.fields = nil
	err := s.terminalErr
	s.mu.Unlock()
	return err
}

func resourceTuple(
	resource *unstructured.Unstructured,
	projections []queryProjection,
) (*providerv1.Tuple, error) {
	values := make([]*kublingv1.Value, 0, len(projections))
	for _, projection := range projections {
		if projection.literal != nil {
			values = append(values, proto.Clone(projection.literal).(*kublingv1.Value))
			continue
		}
		value, err := resourceColumnValue(resource, projection.column)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", projection.column.GetName(), err)
		}
		values = append(values, value)
	}
	return &providerv1.Tuple{Values: values}, nil
}

func resourceColumnValue(
	resource *unstructured.Unstructured,
	column *providerv1.ColumnMetadata,
) (*kublingv1.Value, error) {
	if resource == nil || column == nil {
		return nil, fmt.Errorf("resource and column are required")
	}

	sourceName := strings.TrimSpace(column.GetSourceName())
	if sourceName == "" {
		sourceName = strings.ReplaceAll(column.GetName(), "__", ".")
	}

	var (
		value any
		found bool
		err   error
	)
	if sourceName == "$" {
		value = resource.Object
		found = true
	} else {
		path := strings.Split(sourceName, ".")
		value, found, err = unstructured.NestedFieldNoCopy(resource.Object, path...)
		if err != nil {
			return nil, err
		}
	}
	if !found || value == nil {
		return nullValue(), nil
	}

	return kubernetesTypedValue(value, column.GetType())
}

func kubernetesTypedValue(
	value any,
	valueType kublingv1.ValueType,
) (*kublingv1.Value, error) {
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		stringValueRaw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return stringValue(stringValueRaw), nil

	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		booleanValue, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean, got %T", value)
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_BooleanValue{BooleanValue: booleanValue},
		}, nil

	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		integerValue, err := kubernetesInt64(value)
		if err != nil {
			return nil, err
		}
		if integerValue < -2147483648 || integerValue > 2147483647 {
			return nil, fmt.Errorf("integer value %d exceeds int32 range", integerValue)
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_IntegerValue{IntegerValue: int32(integerValue)},
		}, nil

	case kublingv1.ValueType_VALUE_TYPE_LONG:
		longValue, err := kubernetesInt64(value)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_LongValue{LongValue: longValue},
		}, nil

	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		floatValue, err := kubernetesFloat64(value)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_FloatValue{FloatValue: float32(floatValue)},
		}, nil

	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		doubleValue, err := kubernetesFloat64(value)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{
			Kind: &kublingv1.Value_DoubleValue{DoubleValue: doubleValue},
		}, nil

	case kublingv1.ValueType_VALUE_TYPE_JSON:
		return jsonValue(value)

	default:
		return nil, fmt.Errorf("unsupported Kubernetes column type %s", valueType)
	}
}

func kubernetesInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("integer value %d exceeds int64 range", typed)
		}
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("integer value %d exceeds int64 range", typed)
		}
		return int64(typed), nil
	case float32:
		converted := float64(typed)
		if math.Trunc(converted) != converted {
			return 0, fmt.Errorf("expected integer, got %v", typed)
		}
		return int64(converted), nil
	case float64:
		if math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, fmt.Errorf("expected int64-compatible integer, got %v", typed)
		}
		return int64(typed), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", value)
	}
}

func kubernetesFloat64(value any) (float64, error) {
	switch typed := value.(type) {
	case int:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint8:
		return float64(typed), nil
	case uint16:
		return float64(typed), nil
	case uint32:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	default:
		return 0, fmt.Errorf("expected number, got %T", value)
	}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}

func jsonValue(value any) (*kublingv1.Value, error) {
	if value == nil {
		return nullValue(), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: string(encoded)}}, nil
}

var _ providersdk.ResultStream = (*kubernetesResultStream)(nil)
