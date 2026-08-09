package redis

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultBatchSize = 100

type plannedProjection struct {
	expression *providerv1.Expression
	field      *providerv1.Field
}

type plannedRow struct {
	row         redisRow
	orderValues []*kublingv1.Value
}

// Query reads Redis hashes selected by an exact key or LIKE pattern.
func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	resolved, err := c.resolveTable(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	projections, err := planProjections(resolved.table, request.GetProjections())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Redis projections: %v", err)
	}
	keys, err := resolveKeys(ctx, resolved, request.GetFilter())
	if err != nil {
		return nil, err
	}

	rows := make([]plannedRow, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		row, exists, err := readHashRow(ctx, resolved, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		matches, err := evaluateFilter(row, request.GetFilter())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "evaluate Redis filter: %v", err)
		}
		if !matches {
			continue
		}
		orderValues, err := evaluateOrderValues(row, request.GetOrderBy())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "evaluate Redis ordering: %v", err)
		}
		rows = append(rows, plannedRow{row: row, orderValues: orderValues})
	}
	if err := sortRows(rows, request.GetOrderBy()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sort Redis rows: %v", err)
	}
	rows = paginateRows(rows, request.Offset, request.Limit)

	fields := make([]*providerv1.Field, 0, len(projections))
	for _, projection := range projections {
		fields = append(fields, projection.field)
	}
	tuples := make([]*providerv1.Tuple, 0, len(rows))
	for _, planned := range rows {
		values := make([]*kublingv1.Value, 0, len(projections))
		for _, projection := range projections {
			value, err := evaluateExpression(planned.row, projection.expression)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "evaluate projection %q: %v", projection.field.GetName(), err)
			}
			values = append(values, value)
		}
		tuples = append(tuples, &providerv1.Tuple{Values: values})
	}
	return newResultStream(fields, tuples, queryBatchSize(request.BatchSize)), nil
}

func readHashRow(
	ctx context.Context,
	resolved *resolvedTable,
	key string,
) (redisRow, bool, error) {
	hash, err := resolved.client.HGetAll(ctx, key)
	if err != nil {
		return redisRow{}, false, operationError("read Redis hash", err)
	}
	if len(hash) == 0 {
		return redisRow{}, false, nil
	}
	keyValue, err := decodeRedisValue(strings.TrimPrefix(key, resolved.table.KeyPrefix), resolved.table.Key.Type)
	if err != nil {
		return redisRow{}, false, status.Errorf(codes.Internal, "decode Redis key %q: %v", key, err)
	}
	values := map[string]*kublingv1.Value{strings.ToUpper(resolved.table.Key.Name): keyValue}
	for _, field := range resolved.table.Fields {
		raw, exists := hash[field.Name]
		if !exists {
			if !field.Nullable {
				return redisRow{}, false, status.Errorf(
					codes.Internal,
					"Redis hash %q is missing required field %q",
					key,
					field.Name,
				)
			}
			values[strings.ToUpper(field.Name)] = nullValue()
			continue
		}
		value, err := decodeRedisValue(raw, field.Type)
		if err != nil {
			return redisRow{}, false, status.Errorf(codes.Internal, "decode Redis field %q: %v", field.Name, err)
		}
		values[strings.ToUpper(field.Name)] = value
	}
	return redisRow{key: key, values: values}, true, nil
}

func planProjections(table *TableConfig, requested []*providerv1.Projection) ([]plannedProjection, error) {
	if len(requested) == 0 {
		columns := append([]ColumnConfig{table.Key}, table.Fields...)
		planned := make([]plannedProjection, 0, len(columns))
		for _, column := range columns {
			planned = append(planned, plannedProjection{
				expression: fieldExpression(column.Name),
				field:      &providerv1.Field{Name: column.Name, Type: column.Type},
			})
		}
		return planned, nil
	}
	planned := make([]plannedProjection, 0, len(requested))
	for _, projection := range requested {
		if projection == nil || projection.GetExpression() == nil {
			return nil, fmt.Errorf("projection expression is required")
		}
		valueType, err := expressionType(table, projection.GetExpression())
		if err != nil {
			return nil, err
		}
		name := strings.TrimSpace(projection.GetOutputName())
		if name == "" && projection.GetExpression().GetField() != nil {
			name = projection.GetExpression().GetField().GetName()
		}
		if name == "" {
			return nil, fmt.Errorf("projection output_name is required")
		}
		planned = append(planned, plannedProjection{
			expression: projection.GetExpression(),
			field:      &providerv1.Field{Name: name, Type: valueType},
		})
	}
	return planned, nil
}

func expressionType(table *TableConfig, expression *providerv1.Expression) (kublingv1.ValueType, error) {
	if field := expression.GetField(); field != nil {
		column := table.columns[strings.ToUpper(field.GetName())]
		if column == nil {
			return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("column %q was not found", field.GetName())
		}
		return column.Type, nil
	}
	if literal := expression.GetLiteral(); literal != nil {
		return inferValueType(literal.GetValue())
	}
	if expression.GetComparison() != nil || expression.GetLogical() != nil ||
		expression.GetNullPredicate() != nil || expression.GetPattern() != nil {
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nil
	}
	return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("unsupported projection expression")
}

func inferValueType(value *kublingv1.Value) (kublingv1.ValueType, error) {
	if value == nil {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("literal value is required")
	}
	switch value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return kublingv1.ValueType_VALUE_TYPE_STRING, nil
	case *kublingv1.Value_VarbinaryValue:
		return kublingv1.ValueType_VALUE_TYPE_VARBINARY, nil
	case *kublingv1.Value_CharValue:
		return kublingv1.ValueType_VALUE_TYPE_CHAR, nil
	case *kublingv1.Value_BooleanValue:
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nil
	case *kublingv1.Value_ByteValue:
		return kublingv1.ValueType_VALUE_TYPE_BYTE, nil
	case *kublingv1.Value_ShortValue:
		return kublingv1.ValueType_VALUE_TYPE_SHORT, nil
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
	case *kublingv1.Value_BlobValue:
		return kublingv1.ValueType_VALUE_TYPE_BLOB, nil
	case *kublingv1.Value_ClobValue:
		return kublingv1.ValueType_VALUE_TYPE_CLOB, nil
	case *kublingv1.Value_GeometryValue:
		return kublingv1.ValueType_VALUE_TYPE_GEOMETRY, nil
	case *kublingv1.Value_GeographyValue:
		return kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY, nil
	case *kublingv1.Value_JsonValue:
		return kublingv1.ValueType_VALUE_TYPE_JSON, nil
	case *kublingv1.Value_XmlValue:
		return kublingv1.ValueType_VALUE_TYPE_XML, nil
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("cannot infer literal type")
	}
}

func evaluateOrderValues(row redisRow, orderBy []*providerv1.OrderBy) ([]*kublingv1.Value, error) {
	values := make([]*kublingv1.Value, 0, len(orderBy))
	for _, ordering := range orderBy {
		if ordering == nil || ordering.GetExpression() == nil {
			return nil, fmt.Errorf("ordering expression is required")
		}
		value, err := evaluateExpression(row, ordering.GetExpression())
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func sortRows(rows []plannedRow, orderBy []*providerv1.OrderBy) error {
	var compareErr error
	sort.SliceStable(rows, func(leftIndex int, rightIndex int) bool {
		for index, ordering := range orderBy {
			order, err := compareOrderedValues(rows[leftIndex].orderValues[index], rows[rightIndex].orderValues[index], ordering)
			if err != nil {
				compareErr = err
				return false
			}
			if order != 0 {
				return order < 0
			}
		}
		return rows[leftIndex].row.key < rows[rightIndex].row.key
	})
	return compareErr
}

func compareOrderedValues(left *kublingv1.Value, right *kublingv1.Value, ordering *providerv1.OrderBy) (int, error) {
	leftNull := left.GetNullValue() != nil
	rightNull := right.GetNullValue() != nil
	if leftNull || rightNull {
		if leftNull && rightNull {
			return 0, nil
		}
		nullOrdering := ordering.GetNullOrdering()
		if nullOrdering == providerv1.NullOrdering_NULL_ORDERING_UNSPECIFIED {
			nullOrdering = providerv1.NullOrdering_NULL_ORDERING_LAST
		}
		if leftNull {
			if nullOrdering == providerv1.NullOrdering_NULL_ORDERING_FIRST {
				return -1, nil
			}
			return 1, nil
		}
		if nullOrdering == providerv1.NullOrdering_NULL_ORDERING_FIRST {
			return 1, nil
		}
		return -1, nil
	}
	result, err := compareValues(left, right)
	if err != nil {
		return 0, err
	}
	if ordering.GetDirection() == providerv1.SortDirection_SORT_DIRECTION_DESCENDING {
		result = -result
	}
	return result, nil
}

func paginateRows(rows []plannedRow, offset *uint64, limit *uint64) []plannedRow {
	start := uint64(0)
	if offset != nil {
		start = *offset
	}
	if start >= uint64(len(rows)) {
		return nil
	}
	end := uint64(len(rows))
	if limit != nil && *limit < end-start {
		end = start + *limit
	}
	return rows[int(start):int(end)]
}

func queryBatchSize(batchSize *uint32) int {
	if batchSize == nil || *batchSize == 0 {
		return defaultBatchSize
	}
	if *batchSize > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(*batchSize)
}

func fieldExpression(name string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Field{Field: &providerv1.FieldReference{Name: name}}}
}
