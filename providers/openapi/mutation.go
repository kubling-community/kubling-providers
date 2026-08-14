package openapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Connection) Insert(ctx context.Context, request *providerv1.InsertRequest) (*providerv1.InsertResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "insert request is required")
	}
	if err := c.validateMutationRequest(ctx); err != nil {
		return nil, err
	}
	if len(request.GetReturningFields()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "OpenAPI insert does not support generated values")
	}
	descriptor, err := c.resolveEntity(request.GetEntity())
	if err != nil {
		return nil, err
	}
	if descriptor.insert == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "OpenAPI entity %q does not support insert", descriptor.config.Name)
	}
	rows := request.GetRows()
	if rows == nil {
		return nil, status.Error(codes.InvalidArgument, "insert rows are required")
	}
	fields, err := mutationFields(descriptor, rows.GetFields(), descriptor.insert, true)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI insert: %v", err)
	}
	var affected uint64
	for index, tuple := range rows.GetTuples() {
		values, body, err := insertTuple(fields, tuple, descriptor.insert)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d: %v", index, err)
		}
		if err := c.executeMutation(ctx, descriptor.insert, values, body); err != nil {
			return nil, err
		}
		affected++
	}
	return &providerv1.InsertResponse{AffectedRows: &affected}, nil
}

func (c *Connection) Update(ctx context.Context, request *providerv1.UpdateRequest) (*providerv1.UpdateResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "update request is required")
	}
	if err := c.validateMutationRequest(ctx); err != nil {
		return nil, err
	}
	descriptor, err := c.resolveEntity(request.GetEntity())
	if err != nil {
		return nil, err
	}
	if descriptor.update == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "OpenAPI entity %q does not support update", descriptor.config.Name)
	}
	pathValues, err := mutationPathFilter(descriptor.update, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI update filter: %v", err)
	}
	body, err := assignmentBody(descriptor, descriptor.update, request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI update: %v", err)
	}
	if err := c.executeMutation(ctx, descriptor.update, pathValues, body); err != nil {
		return nil, err
	}
	affected := uint64(1)
	return &providerv1.UpdateResponse{AffectedRows: &affected}, nil
}

func (c *Connection) Delete(ctx context.Context, request *providerv1.DeleteRequest) (*providerv1.DeleteResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "delete request is required")
	}
	if err := c.validateMutationRequest(ctx); err != nil {
		return nil, err
	}
	descriptor, err := c.resolveEntity(request.GetEntity())
	if err != nil {
		return nil, err
	}
	if descriptor.delete == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "OpenAPI entity %q does not support delete", descriptor.config.Name)
	}
	pathValues, err := mutationPathFilter(descriptor.delete, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan OpenAPI delete filter: %v", err)
	}
	if err := c.executeMutation(ctx, descriptor.delete, pathValues, nil); err != nil {
		return nil, err
	}
	affected := uint64(1)
	return &providerv1.DeleteResponse{AffectedRows: &affected}, nil
}

func (c *Connection) validateMutationRequest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return nil
}

type mutationField struct {
	column *providerv1.ColumnMetadata
	body   bool
}

func mutationFields(
	descriptor *entityDescriptor,
	fields []*providerv1.Field,
	mutation *mutationDescriptor,
	allowPathFields bool,
) ([]mutationField, error) {
	columns := make(map[string]*providerv1.ColumnMetadata, len(descriptor.table.GetColumns()))
	for _, column := range descriptor.table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	pathFields := make(map[string]bool)
	if allowPathFields {
		for _, binding := range mutation.config.PathParameters {
			pathFields[strings.ToUpper(binding.Field)] = binding.Field != ""
		}
	}
	result := make([]mutationField, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		if field == nil || strings.TrimSpace(field.GetName()) == "" {
			return nil, fmt.Errorf("field %d name is required", index)
		}
		lookup := strings.ToUpper(strings.TrimSpace(field.GetName()))
		if _, exists := seen[lookup]; exists {
			return nil, fmt.Errorf("duplicate field %q", field.GetName())
		}
		seen[lookup] = struct{}{}
		column := columns[lookup]
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", field.GetName())
		}
		_, inBody := mutation.bodyProperties[column.GetSourceName()]
		if !inBody && !pathFields[lookup] {
			return nil, fmt.Errorf("column %q is not accepted by operation %q", field.GetName(), mutation.operation.OperationId)
		}
		if inBody && schemaProxyReadOnly(mutation.bodyProperties[column.GetSourceName()]) {
			return nil, fmt.Errorf("column %q is read-only", field.GetName())
		}
		result = append(result, mutationField{column: column, body: inBody})
	}
	return result, nil
}

func insertTuple(
	fields []mutationField,
	tuple *providerv1.Tuple,
	mutation *mutationDescriptor,
) (map[string]*kublingv1.Value, map[string]any, error) {
	if tuple == nil {
		return nil, nil, fmt.Errorf("tuple is required")
	}
	if len(tuple.GetValues()) != len(fields) {
		return nil, nil, fmt.Errorf("has %d values for %d fields", len(tuple.GetValues()), len(fields))
	}
	values := make(map[string]*kublingv1.Value, len(fields))
	body := make(map[string]any, len(fields))
	for index, field := range fields {
		value := tuple.GetValues()[index]
		values[strings.ToUpper(field.column.GetName())] = value
		if !field.body {
			continue
		}
		jsonValue, err := mutationJSONValue(value)
		if err != nil {
			return nil, nil, fmt.Errorf("field %q: %w", field.column.GetName(), err)
		}
		body[field.column.GetSourceName()] = jsonValue
	}
	missing := make([]string, 0)
	for sourceName := range mutation.bodyRequired {
		property := mutation.bodyProperties[sourceName]
		if schemaProxyReadOnly(property) {
			continue
		}
		if _, exists := body[sourceName]; !exists {
			missing = append(missing, sourceName)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, nil, fmt.Errorf("required body fields are missing: %s", strings.Join(missing, ", "))
	}
	return values, body, nil
}

func assignmentBody(
	descriptor *entityDescriptor,
	mutation *mutationDescriptor,
	assignments []*providerv1.Assignment,
) (map[string]any, error) {
	if len(assignments) == 0 {
		return nil, fmt.Errorf("at least one assignment is required")
	}
	columns := make(map[string]*providerv1.ColumnMetadata, len(descriptor.table.GetColumns()))
	for _, column := range descriptor.table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	body := make(map[string]any, len(assignments))
	for index, assignment := range assignments {
		if assignment == nil || strings.TrimSpace(assignment.GetField()) == "" {
			return nil, fmt.Errorf("assignment %d field is required", index)
		}
		column := columns[strings.ToUpper(strings.TrimSpace(assignment.GetField()))]
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", assignment.GetField())
		}
		property := mutation.bodyProperties[column.GetSourceName()]
		if property == nil || schemaProxyReadOnly(property) {
			return nil, fmt.Errorf("column %q is not writable by operation %q", assignment.GetField(), mutation.operation.OperationId)
		}
		if _, exists := body[column.GetSourceName()]; exists {
			return nil, fmt.Errorf("duplicate assignment for column %q", assignment.GetField())
		}
		literal := assignment.GetValue().GetLiteral()
		if literal == nil || literal.GetValue() == nil {
			return nil, fmt.Errorf("assignment for column %q must be a literal", assignment.GetField())
		}
		value, err := mutationJSONValue(literal.GetValue())
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", assignment.GetField(), err)
		}
		body[column.GetSourceName()] = value
	}
	return body, nil
}

func mutationPathFilter(mutation *mutationDescriptor, filter *providerv1.Expression) (map[string]*kublingv1.Value, error) {
	if filter == nil {
		return nil, fmt.Errorf("an equality filter is required")
	}
	values, err := filterLiteralValues(filter)
	if err != nil {
		return nil, err
	}
	required := make(map[string]struct{})
	for _, binding := range mutation.config.PathParameters {
		if binding.Field == "" {
			continue
		}
		lookup := strings.ToUpper(binding.Field)
		required[lookup] = struct{}{}
		if values[lookup] == nil {
			return nil, fmt.Errorf("path field %q requires an equality filter", binding.Field)
		}
	}
	for field := range values {
		if _, exists := required[field]; !exists {
			return nil, fmt.Errorf("filter field %q is not bound to the operation path", field)
		}
	}
	return values, nil
}

func filterLiteralValues(expression *providerv1.Expression) (map[string]*kublingv1.Value, error) {
	values := make(map[string]*kublingv1.Value)
	var appendExpression func(*providerv1.Expression) error
	appendExpression = func(current *providerv1.Expression) error {
		if current == nil {
			return fmt.Errorf("filter expression is required")
		}
		if comparison := current.GetComparison(); comparison != nil {
			if comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
				return fmt.Errorf("only equality comparisons are supported")
			}
			field, literal, ok := fieldAndLiteral(comparison.GetLeft(), comparison.GetRight())
			if !ok {
				field, literal, ok = fieldAndLiteral(comparison.GetRight(), comparison.GetLeft())
			}
			if !ok {
				return fmt.Errorf("equality comparison must contain one field and one literal")
			}
			lookup := strings.ToUpper(strings.TrimSpace(field.GetName()))
			if lookup == "" {
				return fmt.Errorf("filter field name is required")
			}
			if _, exists := values[lookup]; exists {
				return fmt.Errorf("duplicate equality filter for field %q", field.GetName())
			}
			values[lookup] = literal.GetValue()
			return nil
		}
		if logical := current.GetLogical(); logical != nil {
			if logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND || len(logical.GetOperands()) < 2 {
				return fmt.Errorf("only logical AND with at least two operands is supported")
			}
			for _, operand := range logical.GetOperands() {
				if err := appendExpression(operand); err != nil {
					return err
				}
			}
			return nil
		}
		return fmt.Errorf("only equality comparisons and logical AND are supported")
	}
	if err := appendExpression(expression); err != nil {
		return nil, err
	}
	return values, nil
}

func mutationJSONValue(value *kublingv1.Value) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("value is required")
	}
	switch kind := value.GetKind().(type) {
	case *kublingv1.Value_NullValue:
		return nil, nil
	case *kublingv1.Value_StringValue:
		return kind.StringValue, nil
	case *kublingv1.Value_CharValue:
		return kind.CharValue, nil
	case *kublingv1.Value_BooleanValue:
		return kind.BooleanValue, nil
	case *kublingv1.Value_ByteValue:
		return kind.ByteValue, nil
	case *kublingv1.Value_ShortValue:
		return kind.ShortValue, nil
	case *kublingv1.Value_IntegerValue:
		return kind.IntegerValue, nil
	case *kublingv1.Value_LongValue:
		return kind.LongValue, nil
	case *kublingv1.Value_BigintegerValue:
		return json.Number(kind.BigintegerValue), nil
	case *kublingv1.Value_FloatValue:
		return kind.FloatValue, nil
	case *kublingv1.Value_DoubleValue:
		return kind.DoubleValue, nil
	case *kublingv1.Value_BigdecimalValue:
		return json.Number(kind.BigdecimalValue), nil
	case *kublingv1.Value_DateValue:
		return kind.DateValue, nil
	case *kublingv1.Value_TimeValue:
		return kind.TimeValue, nil
	case *kublingv1.Value_TimestampValue:
		return kind.TimestampValue, nil
	case *kublingv1.Value_VarbinaryValue:
		return base64.StdEncoding.EncodeToString(kind.VarbinaryValue), nil
	case *kublingv1.Value_JsonValue:
		decoder := json.NewDecoder(strings.NewReader(kind.JsonValue))
		decoder.UseNumber()
		var result any
		if err := decoder.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode JSON value: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("decode JSON value: trailing content")
		}
		return result, nil
	default:
		return nil, fmt.Errorf("value type %T cannot be encoded as JSON", kind)
	}
}

func (c *Connection) executeMutation(
	ctx context.Context,
	descriptor *mutationDescriptor,
	fieldValues map[string]*kublingv1.Value,
	body map[string]any,
) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	path := descriptor.path
	for _, binding := range descriptor.config.PathParameters {
		value := binding.Value
		if binding.Field != "" {
			var err error
			value, err = queryLiteralValue(fieldValues[strings.ToUpper(binding.Field)])
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "path field %q: %v", binding.Field, err)
			}
		}
		path = strings.ReplaceAll(path, "{"+binding.Parameter+"}", url.PathEscape(value))
	}
	if strings.Contains(path, "{") {
		return status.Errorf(codes.FailedPrecondition, "operation path %q has an unbound parameter", path)
	}
	endpoint, err := operationURL(c.provider.config.BaseURL, path)
	if err != nil {
		return status.Errorf(codes.Internal, "build OpenAPI mutation URL: %v", err)
	}
	parameters := make(url.Values, len(descriptor.config.QueryParameters))
	for _, parameter := range descriptor.config.QueryParameters {
		parameters.Set(parameter.Name, parameter.Value)
	}
	endpoint, err = queryParameterizedURL(endpoint, parameters)
	if err != nil {
		return status.Errorf(codes.Internal, "build OpenAPI mutation parameters: %v", err)
	}
	var payload io.Reader
	if body != nil {
		wrapped, err := wrapMutationBody(body, descriptor.config.BodyPath)
		if err != nil {
			return status.Errorf(codes.Internal, "build OpenAPI mutation body: %v", err)
		}
		encoded, err := json.Marshal(wrapped)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "encode OpenAPI mutation body: %v", err)
		}
		payload = bytes.NewReader(encoded)
	}
	requestContext, cancel := context.WithTimeout(ctx, c.provider.config.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, descriptor.method, endpoint, payload)
	if err != nil {
		return status.Errorf(codes.Internal, "build OpenAPI mutation request: %v", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range c.provider.config.Headers {
		request.Header.Set(name, value)
	}
	c.provider.authentication.apply(request)
	response, err := c.provider.client.Do(request)
	if err != nil {
		if requestContext.Err() != nil {
			return status.FromContextError(requestContext.Err()).Err()
		}
		return status.Errorf(codes.Unavailable, "execute OpenAPI mutation: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return httpStatusError(response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return nil
}

func wrapMutationBody(body map[string]any, pointer string) (any, error) {
	tokens, err := parseJSONPointer(pointer)
	if err != nil {
		return nil, err
	}
	var result any = body
	for index := len(tokens) - 1; index >= 0; index-- {
		result = map[string]any{tokens[index]: result}
	}
	return result, nil
}
