package openapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultQueryBatchSize = 100

type queryProjection struct {
	field   *providerv1.Field
	column  *providerv1.ColumnMetadata
	literal *kublingv1.Value
}

func (c *Connection) Query(ctx context.Context, request *providerv1.QueryRequest) (providersdk.ResultStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	if len(request.GetOrderBy()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "OpenAPI queries do not support ordering pushdown")
	}

	descriptor, err := c.resolveEntity(request.GetEntity())
	if err != nil {
		return nil, err
	}
	parameters, err := planQueryParameters(descriptor, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI filter: %v", err)
	}
	projections, err := planQueryProjections(descriptor.table, request.GetProjections())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI projections: %v", err)
	}
	return newResultStream(
		c,
		descriptor,
		projections,
		queryBatchSize(request.BatchSize),
		parameters,
		request.Limit,
		request.Offset,
	), nil
}

func (c *Connection) resolveEntity(entity *providerv1.EntityReference) (*entityDescriptor, error) {
	if entity == nil || strings.TrimSpace(entity.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query entity name is required")
	}
	if entity.GetNamespace() != c.provider.config.Namespace {
		return nil, status.Errorf(codes.NotFound, "OpenAPI namespace %q was not found", entity.GetNamespace())
	}
	descriptor := c.provider.catalog.entities[strings.ToUpper(strings.TrimSpace(entity.GetName()))]
	if descriptor == nil {
		return nil, status.Errorf(codes.NotFound, "OpenAPI entity %q was not found", entity.GetName())
	}
	return descriptor, nil
}

func planQueryProjections(table *providerv1.TableMetadata, requested []*providerv1.Projection) ([]queryProjection, error) {
	columns := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	if len(requested) == 0 {
		result := make([]queryProjection, 0, len(table.GetColumns()))
		for _, column := range table.GetColumns() {
			result = append(result, queryProjection{
				field:  &providerv1.Field{Name: column.GetName(), Type: column.GetType()},
				column: column,
			})
		}
		return result, nil
	}

	result := make([]queryProjection, 0, len(requested))
	for _, requestedProjection := range requested {
		if requestedProjection == nil || requestedProjection.GetExpression() == nil {
			return nil, fmt.Errorf("projection expression is required")
		}
		outputName := strings.TrimSpace(requestedProjection.GetOutputName())
		if field := requestedProjection.GetExpression().GetField(); field != nil {
			column := columns[strings.ToUpper(strings.TrimSpace(field.GetName()))]
			if column == nil {
				return nil, fmt.Errorf("column %q was not found", field.GetName())
			}
			if outputName == "" {
				outputName = column.GetName()
			}
			result = append(result, queryProjection{
				field:  &providerv1.Field{Name: outputName, Type: column.GetType()},
				column: column,
			})
			continue
		}
		if literal := requestedProjection.GetExpression().GetLiteral(); literal != nil && literal.GetValue() != nil {
			if outputName == "" {
				return nil, fmt.Errorf("literal projection output_name is required")
			}
			valueType, err := literalValueType(literal.GetValue())
			if err != nil {
				return nil, err
			}
			result = append(result, queryProjection{
				field:   &providerv1.Field{Name: outputName, Type: valueType},
				literal: proto.Clone(literal.GetValue()).(*kublingv1.Value),
			})
			continue
		}
		return nil, fmt.Errorf("only field and literal projections are supported")
	}
	return result, nil
}

func (c *Connection) fetchPage(
	ctx context.Context,
	descriptor *entityDescriptor,
	state *paginationState,
	parameters url.Values,
) ([]map[string]any, bool, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, false, err
	}
	endpoint, err := operationURL(c.provider.config.BaseURL, descriptor.path)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "build OpenAPI request URL: %v", err)
	}
	endpoint, err = queryParameterizedURL(endpoint, parameters)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "build OpenAPI query parameters: %v", err)
	}
	endpoint, err = paginatedURL(endpoint, descriptor.config.Pagination, state)
	if err != nil {
		return nil, false, status.Errorf(codes.FailedPrecondition, "build OpenAPI pagination request: %v", err)
	}
	requestContext, cancel := context.WithTimeout(ctx, c.provider.config.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "build OpenAPI request: %v", err)
	}
	request.Header.Set("Accept", "application/json")
	for name, value := range c.provider.config.Headers {
		request.Header.Set(name, value)
	}
	c.provider.authentication.apply(request)

	response, err := c.provider.client.Do(request)
	if err != nil {
		if requestContext.Err() != nil {
			return nil, false, status.FromContextError(requestContext.Err()).Err()
		}
		return nil, false, status.Errorf(codes.Unavailable, "execute OpenAPI request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, false, httpStatusError(response.StatusCode, strings.TrimSpace(string(body)))
	}

	if response.ContentLength > c.provider.config.MaxResponseBytes {
		return nil, false, responseSizeError(c.provider.config.MaxResponseBytes)
	}
	limitedBody := &io.LimitedReader{
		R: response.Body,
		N: c.provider.config.MaxResponseBytes + 1,
	}
	decoder := json.NewDecoder(limitedBody)
	decoder.UseNumber()
	var document any
	decodeErr := decoder.Decode(&document)
	if limitedBody.N == 0 {
		return nil, false, responseSizeError(c.provider.config.MaxResponseBytes)
	}
	if decodeErr != nil {
		return nil, false, status.Errorf(codes.Internal, "decode OpenAPI JSON response: %v", decodeErr)
	}
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if limitedBody.N == 0 {
		return nil, false, responseSizeError(c.provider.config.MaxResponseBytes)
	}
	if !errors.Is(trailingErr, io.EOF) {
		if trailingErr == nil {
			return nil, false, status.Error(codes.Internal, "decode OpenAPI JSON response: trailing JSON value")
		}
		return nil, false, status.Errorf(codes.Internal, "decode OpenAPI JSON response: %v", trailingErr)
	}
	rows, err := responseRows(document, descriptor.config.ResponsePath)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "decode OpenAPI responsePath: %v", err)
	}
	done, err := advancePagination(descriptor.config.Pagination, state, document, len(rows))
	if err != nil {
		return nil, false, status.Errorf(codes.FailedPrecondition, "advance OpenAPI pagination: %v", err)
	}
	return rows, done, nil
}

func responseSizeError(maximum int64) error {
	return status.Errorf(codes.ResourceExhausted, "OpenAPI JSON response exceeds maxResponseBytes (%d)", maximum)
}

func queryParameterizedURL(endpoint string, parameters url.Values) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	for name, values := range parameters {
		for _, value := range values {
			query.Add(name, value)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func responseRows(document any, responsePath string) ([]map[string]any, error) {
	target, err := valueAtJSONPointer(document, responsePath)
	if err != nil {
		return nil, err
	}
	items, ok := target.([]any)
	if !ok {
		return nil, fmt.Errorf("OpenAPI responsePath resolved to %T, want array", target)
	}
	rows := make([]map[string]any, 0, len(items))
	for index, item := range items {
		row, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("OpenAPI response item %d is %T, want object", index, item)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func paginatedURL(endpoint string, pagination *PaginationConfig, state *paginationState) (string, error) {
	if pagination == nil {
		return endpoint, nil
	}
	if state.pages >= pagination.MaxPages {
		return "", fmt.Errorf("maximum of %d pages reached", pagination.MaxPages)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	if pagination.PageSizeParameter != "" {
		query.Set(pagination.PageSizeParameter, strconv.FormatUint(uint64(pagination.PageSize), 10))
	}
	switch pagination.Mode {
	case PaginationModeOffset:
		query.Set(pagination.OffsetParameter, strconv.FormatUint(state.offset, 10))
	case PaginationModePage:
		query.Set(pagination.PageParameter, strconv.FormatUint(state.page, 10))
	case PaginationModeCursor:
		if state.cursor != "" {
			query.Set(pagination.CursorParameter, state.cursor)
		}
	default:
		return "", fmt.Errorf("unsupported pagination mode %q", pagination.Mode)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func advancePagination(
	pagination *PaginationConfig,
	state *paginationState,
	document any,
	rowCount int,
) (bool, error) {
	if pagination == nil {
		return true, nil
	}
	state.pages++
	switch pagination.Mode {
	case PaginationModeOffset:
		state.offset += uint64(rowCount)
		return rowCount < int(pagination.PageSize), nil
	case PaginationModePage:
		state.page++
		return rowCount < int(pagination.PageSize), nil
	case PaginationModeCursor:
		if pagination.HasMorePath != "" {
			rawHasMore, found, err := optionalValueAtJSONPointer(document, pagination.HasMorePath)
			if err != nil {
				return false, fmt.Errorf("hasMorePath: %w", err)
			}
			if !found {
				return false, fmt.Errorf("hasMorePath property was not found")
			}
			hasMore, ok := rawHasMore.(bool)
			if !ok {
				return false, fmt.Errorf("hasMorePath resolved to %T, want boolean", rawHasMore)
			}
			if !hasMore {
				return true, nil
			}
		}
		rawCursor, found, err := optionalValueAtJSONPointer(document, pagination.NextCursorPath)
		if err != nil {
			return false, fmt.Errorf("nextCursorPath: %w", err)
		}
		if !found {
			if pagination.HasMorePath != "" {
				return false, fmt.Errorf("nextCursorPath property was not found while hasMorePath is true")
			}
			return true, nil
		}
		cursor, done, err := cursorToken(rawCursor)
		if done && pagination.HasMorePath != "" {
			return false, fmt.Errorf("nextCursorPath is empty while hasMorePath is true")
		}
		if err != nil || done {
			return done, err
		}
		if _, repeated := state.seenCursors[cursor]; repeated {
			return false, fmt.Errorf("cursor %q was repeated", cursor)
		}
		state.seenCursors[cursor] = struct{}{}
		state.cursor = cursor
		return false, nil
	default:
		return false, fmt.Errorf("unsupported pagination mode %q", pagination.Mode)
	}
}

func cursorToken(raw any) (string, bool, error) {
	switch value := raw.(type) {
	case nil:
		return "", true, nil
	case string:
		return value, value == "", nil
	case json.Number:
		return value.String(), false, nil
	default:
		return "", false, fmt.Errorf("next cursor is %T, want string, number or null", raw)
	}
}

func operationURL(baseURL, operationPath string) (string, error) {
	if strings.Contains(operationPath, "{") {
		return "", fmt.Errorf("path parameters are not supported in list operation path %q", operationPath)
	}
	base, err := url.Parse(strings.TrimRight(baseURL, "/") + "/")
	if err != nil {
		return "", err
	}
	reference, err := url.Parse(strings.TrimLeft(operationPath, "/"))
	if err != nil {
		return "", err
	}
	resolved := base.ResolveReference(reference)
	if !sameOrigin(base, resolved) {
		return "", fmt.Errorf("operation path %q changed datasource origin", operationPath)
	}
	return resolved.String(), nil
}

func valueAtJSONPointer(document any, pointer string) (any, error) {
	value, found, err := optionalValueAtJSONPointer(document, pointer)
	if err != nil {
		return nil, err
	}
	if !found {
		tokens, _ := parseJSONPointer(pointer)
		return nil, fmt.Errorf("property %q was not found", tokens[len(tokens)-1])
	}
	return value, nil
}

func optionalValueAtJSONPointer(document any, pointer string) (any, bool, error) {
	tokens, err := parseJSONPointer(pointer)
	if err != nil {
		return nil, false, err
	}
	current := document
	for _, token := range tokens {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("property %q cannot be read from %T", token, current)
		}
		value, exists := object[token]
		if !exists {
			return nil, false, nil
		}
		current = value
	}
	return current, true, nil
}

func rowTuple(row map[string]any, projections []queryProjection) (*providerv1.Tuple, error) {
	values := make([]*kublingv1.Value, 0, len(projections))
	for _, projection := range projections {
		if projection.literal != nil {
			values = append(values, proto.Clone(projection.literal).(*kublingv1.Value))
			continue
		}
		raw, exists := row[projection.column.GetSourceName()]
		if !exists || raw == nil {
			if !projection.column.GetNullable() {
				return nil, fmt.Errorf("required property %q is missing or null", projection.column.GetSourceName())
			}
			values = append(values, nullValue())
			continue
		}
		value, err := openAPIValue(raw, projection.column)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", projection.column.GetSourceName(), err)
		}
		values = append(values, value)
	}
	return &providerv1.Tuple{Values: values}, nil
}

func openAPIValue(raw any, column *providerv1.ColumnMetadata) (*kublingv1.Value, error) {
	switch column.GetType() {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		value, ok := raw.(string)
		if !ok {
			return nil, expectedJSONType("string", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_VARBINARY:
		value, ok := raw.(string)
		if !ok {
			return nil, expectedJSONType("string", raw)
		}
		if column.Properties["openapi.format"] == "byte" {
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return nil, fmt.Errorf("decode base64 byte value: %w", err)
			}
			return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{VarbinaryValue: decoded}}, nil
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{VarbinaryValue: []byte(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		value, ok := raw.(bool)
		if !ok {
			return nil, expectedJSONType("boolean", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		value, err := jsonInteger(raw, math.MinInt32, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: int32(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		value, err := jsonInteger(raw, math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		value, ok := raw.(json.Number)
		if !ok || strings.ContainsAny(value.String(), ".eE") {
			return nil, expectedJSONType("integer", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: value.String()}}, nil
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		value, err := jsonFloat(raw)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{FloatValue: float32(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		value, err := jsonFloat(raw)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		value, ok := raw.(json.Number)
		if !ok {
			return nil, expectedJSONType("number", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: value.String()}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DATE:
		return temporalValue(raw, func(value string) *kublingv1.Value {
			return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{DateValue: value}}
		})
	case kublingv1.ValueType_VALUE_TYPE_TIME:
		return temporalValue(raw, func(value string) *kublingv1.Value {
			return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{TimeValue: value}}
		})
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		return temporalValue(raw, func(value string) *kublingv1.Value {
			return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{TimestampValue: value}}
		})
	case kublingv1.ValueType_VALUE_TYPE_JSON:
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: string(encoded)}}, nil
	default:
		return nil, fmt.Errorf("unsupported OpenAPI column type %s", column.GetType())
	}
}

func jsonInteger(raw any, minimum, maximum int64) (int64, error) {
	number, ok := raw.(json.Number)
	if !ok {
		return 0, expectedJSONType("integer", raw)
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("value %q is outside the supported integer range", number.String())
	}
	return value, nil
}

func jsonFloat(raw any) (float64, error) {
	number, ok := raw.(json.Number)
	if !ok {
		return 0, expectedJSONType("number", raw)
	}
	value, err := number.Float64()
	if err != nil {
		return 0, fmt.Errorf("decode number %q: %w", number.String(), err)
	}
	return value, nil
}

func temporalValue(raw any, build func(string) *kublingv1.Value) (*kublingv1.Value, error) {
	value, ok := raw.(string)
	if !ok {
		return nil, expectedJSONType("string", raw)
	}
	return build(value), nil
}

func expectedJSONType(expected string, raw any) error {
	return fmt.Errorf("expected JSON %s, got %T", expected, raw)
}

func literalValueType(value *kublingv1.Value) (kublingv1.ValueType, error) {
	switch value.GetKind().(type) {
	case *kublingv1.Value_NullValue:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, nil
	case *kublingv1.Value_StringValue:
		return kublingv1.ValueType_VALUE_TYPE_STRING, nil
	case *kublingv1.Value_VarbinaryValue:
		return kublingv1.ValueType_VALUE_TYPE_VARBINARY, nil
	case *kublingv1.Value_BooleanValue:
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nil
	case *kublingv1.Value_IntegerValue:
		return kublingv1.ValueType_VALUE_TYPE_INTEGER, nil
	case *kublingv1.Value_LongValue:
		return kublingv1.ValueType_VALUE_TYPE_LONG, nil
	case *kublingv1.Value_BigintegerValue:
		return kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, nil
	case *kublingv1.Value_FloatValue:
		return kublingv1.ValueType_VALUE_TYPE_FLOAT, nil
	case *kublingv1.Value_DoubleValue:
		return kublingv1.ValueType_VALUE_TYPE_DOUBLE, nil
	case *kublingv1.Value_BigdecimalValue:
		return kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL, nil
	case *kublingv1.Value_DateValue:
		return kublingv1.ValueType_VALUE_TYPE_DATE, nil
	case *kublingv1.Value_TimeValue:
		return kublingv1.ValueType_VALUE_TYPE_TIME, nil
	case *kublingv1.Value_TimestampValue:
		return kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, nil
	case *kublingv1.Value_JsonValue:
		return kublingv1.ValueType_VALUE_TYPE_JSON, nil
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("unsupported literal value")
	}
}

func queryBatchSize(batchSize *uint32) int {
	if batchSize == nil || *batchSize == 0 {
		return defaultQueryBatchSize
	}
	if uint64(*batchSize) > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(*batchSize)
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}

func httpStatusError(statusCode int, body string) error {
	code := codes.FailedPrecondition
	switch statusCode {
	case http.StatusUnauthorized:
		code = codes.Unauthenticated
	case http.StatusForbidden:
		code = codes.PermissionDenied
	case http.StatusNotFound:
		code = codes.NotFound
	case http.StatusTooManyRequests:
		code = codes.ResourceExhausted
	default:
		if statusCode >= http.StatusInternalServerError {
			code = codes.Unavailable
		}
	}
	message := fmt.Sprintf("OpenAPI request returned HTTP %d", statusCode)
	if body != "" {
		message += ": " + body
	}
	return status.Error(code, message)
}
