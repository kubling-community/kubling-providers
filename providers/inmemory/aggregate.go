package inmemory

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/inmemory/internal/querylog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type aggregateGroup struct {
	key  string
	rows []entityRow
}

type plannedAggregateRow struct {
	key         string
	values      []*kublingv1.Value
	orderValues []*kublingv1.Value
}

func aggregateQuery(request *providerv1.QueryRequest) bool {
	if len(request.GetGroupBy()) > 0 || request.GetHaving() != nil {
		return true
	}
	for _, projection := range request.GetProjections() {
		if projection != nil && containsAggregate(projection.GetExpression()) {
			return true
		}
	}
	for _, ordering := range request.GetOrderBy() {
		if ordering != nil && containsAggregate(ordering.GetExpression()) {
			return true
		}
	}

	return false
}

func containsAggregate(expression *providerv1.Expression) bool {
	if expression == nil {
		return false
	}
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Aggregate:
		return kind.Aggregate != nil
	case *providerv1.Expression_Comparison:
		return kind.Comparison != nil &&
			(containsAggregate(kind.Comparison.GetLeft()) ||
				containsAggregate(kind.Comparison.GetRight()))
	case *providerv1.Expression_Logical:
		if kind.Logical == nil {
			return false
		}
		return slices.ContainsFunc(kind.Logical.GetOperands(), containsAggregate)
	case *providerv1.Expression_NullPredicate:
		return kind.NullPredicate != nil &&
			containsAggregate(kind.NullPredicate.GetExpression())
	case *providerv1.Expression_Pattern:
		return kind.Pattern != nil &&
			(containsAggregate(kind.Pattern.GetValue()) ||
				containsAggregate(kind.Pattern.GetPattern()))
	case *providerv1.Expression_FunctionCall:
		return kind.FunctionCall != nil &&
			slices.ContainsFunc(kind.FunctionCall.GetArguments(), containsAggregate)
	default:
		return false
	}
}

func (c *Connection) queryAggregates(
	ctx context.Context,
	request *providerv1.QueryRequest,
	projections []plannedProjection,
	rows []entityRow,
) (providersdk.ResultStream, error) {
	if len(request.GetProjections()) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"aggregate queries require explicit projections",
		)
	}
	if containsAggregate(request.GetFilter()) {
		return nil, status.Error(
			codes.InvalidArgument,
			"aggregate expressions are not valid in filters",
		)
	}
	for index, expression := range request.GetGroupBy() {
		if expression == nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"group_by %d is required",
				index,
			)
		}
		if containsAggregate(expression) {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"group_by %d contains an aggregate",
				index,
			)
		}
	}
	for _, projection := range projections {
		if err := validateGroupedExpression(
			projection.expression,
			request.GetGroupBy(),
			false,
		); err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"projection %q: %v",
				projection.field.GetName(),
				err,
			)
		}
	}
	if err := validateGroupedExpression(
		request.GetHaving(),
		request.GetGroupBy(),
		false,
	); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "having: %v", err)
	}
	for index, ordering := range request.GetOrderBy() {
		if ordering == nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"order_by %d is required",
				index,
			)
		}
		if err := validateGroupedExpression(
			ordering.GetExpression(),
			request.GetGroupBy(),
			false,
		); err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"order_by %d: %v",
				index,
				err,
			)
		}
	}

	filtered := make([]entityRow, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		matches, err := evaluateFilter(row, request.GetFilter())
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"evaluate filter: %v",
				err,
			)
		}
		if matches {
			filtered = append(filtered, row)
		}
	}

	groups, err := groupAggregateRows(filtered, request.GetGroupBy())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "group rows: %v", err)
	}
	planned := make([]plannedAggregateRow, 0, len(groups))
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if request.GetHaving() != nil {
			matches, err := evaluateGroupedBooleanExpression(
				group.rows,
				request.GetHaving(),
			)
			if err != nil {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"evaluate having: %v",
					err,
				)
			}
			if !matches {
				continue
			}
		}

		values := make([]*kublingv1.Value, 0, len(projections))
		for _, projection := range projections {
			value, err := evaluateGroupedExpression(
				group.rows,
				projection.expression,
			)
			if err != nil {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"evaluate projection %q: %v",
					projection.field.GetName(),
					err,
				)
			}
			output, err := c.outputValue(value, request.GetAcceptedFeatures())
			if err != nil {
				return nil, err
			}
			values = append(values, output)
		}

		orderValues := make([]*kublingv1.Value, 0, len(request.GetOrderBy()))
		for _, ordering := range request.GetOrderBy() {
			value, err := evaluateGroupedExpression(
				group.rows,
				ordering.GetExpression(),
			)
			if err != nil {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"evaluate ordering: %v",
					err,
				)
			}
			orderValues = append(orderValues, value)
		}
		planned = append(planned, plannedAggregateRow{
			key:         group.key,
			values:      values,
			orderValues: orderValues,
		})
	}

	if err := sortAggregateRows(planned, request.GetOrderBy()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sort rows: %v", err)
	}
	planned = paginateAggregateRows(planned, request.Offset, request.Limit)

	tuples := make([]*providerv1.Tuple, 0, len(planned))
	for _, row := range planned {
		tuples = append(tuples, &providerv1.Tuple{Values: row.values})
	}
	fields := make([]*providerv1.Field, 0, len(projections))
	for _, projection := range projections {
		fields = append(fields, projection.field)
	}
	querylog.Executed(ctx, c.queryLogger, request, len(tuples))

	return newResultStream(fields, tuples, queryBatchSize(request.BatchSize)), nil
}

func validateGroupedExpression(
	expression *providerv1.Expression,
	groupBy []*providerv1.Expression,
	insideAggregate bool,
) error {
	if expression == nil {
		return nil
	}
	if !insideAggregate && slices.ContainsFunc(groupBy, func(group *providerv1.Expression) bool {
		return proto.Equal(group, expression)
	}) {
		return nil
	}

	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Field:
		if insideAggregate {
			return nil
		}
		return fmt.Errorf("field %q is not grouped", kind.Field.GetName())
	case *providerv1.Expression_Literal:
		return nil
	case *providerv1.Expression_Aggregate:
		if insideAggregate {
			return fmt.Errorf("nested aggregates are not supported")
		}
		if kind.Aggregate == nil {
			return fmt.Errorf("aggregate is required")
		}
		for _, argument := range kind.Aggregate.GetArguments() {
			if err := validateGroupedExpression(argument, groupBy, true); err != nil {
				return err
			}
		}
		return nil
	case *providerv1.Expression_Comparison:
		if kind.Comparison == nil {
			return fmt.Errorf("comparison is required")
		}
		if err := validateGroupedExpression(kind.Comparison.GetLeft(), groupBy, insideAggregate); err != nil {
			return err
		}
		return validateGroupedExpression(kind.Comparison.GetRight(), groupBy, insideAggregate)
	case *providerv1.Expression_Logical:
		if kind.Logical == nil {
			return fmt.Errorf("logical expression is required")
		}
		for _, operand := range kind.Logical.GetOperands() {
			if err := validateGroupedExpression(operand, groupBy, insideAggregate); err != nil {
				return err
			}
		}
		return nil
	case *providerv1.Expression_NullPredicate:
		if kind.NullPredicate == nil {
			return fmt.Errorf("null predicate is required")
		}
		return validateGroupedExpression(kind.NullPredicate.GetExpression(), groupBy, insideAggregate)
	case *providerv1.Expression_FunctionCall:
		if kind.FunctionCall == nil {
			return fmt.Errorf("function call is required")
		}
		for _, argument := range kind.FunctionCall.GetArguments() {
			if err := validateGroupedExpression(argument, groupBy, insideAggregate); err != nil {
				return err
			}
		}
		return nil
	case *providerv1.Expression_Pattern:
		if kind.Pattern == nil {
			return fmt.Errorf("pattern is required")
		}
		if err := validateGroupedExpression(kind.Pattern.GetValue(), groupBy, insideAggregate); err != nil {
			return err
		}
		return validateGroupedExpression(kind.Pattern.GetPattern(), groupBy, insideAggregate)
	default:
		return fmt.Errorf("expression kind is required")
	}
}

func groupAggregateRows(
	rows []entityRow,
	groupBy []*providerv1.Expression,
) ([]aggregateGroup, error) {
	if len(groupBy) == 0 {
		return []aggregateGroup{{rows: rows}}, nil
	}

	groupsByKey := make(map[string]*aggregateGroup)
	for _, row := range rows {
		values := make([]*kublingv1.Value, 0, len(groupBy))
		for _, expression := range groupBy {
			value, err := evaluateExpression(row, expression)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		key, err := aggregateValuesKey(values)
		if err != nil {
			return nil, err
		}
		group := groupsByKey[key]
		if group == nil {
			group = &aggregateGroup{key: key}
			groupsByKey[key] = group
		}
		group.rows = append(group.rows, row)
	}

	groups := make([]aggregateGroup, 0, len(groupsByKey))
	for _, group := range groupsByKey {
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(left, right int) bool {
		return groups[left].key < groups[right].key
	})

	return groups, nil
}

func aggregateValuesKey(values []*kublingv1.Value) (string, error) {
	var key []byte
	for _, value := range values {
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("encode grouped value: %w", err)
		}
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(encoded)))
		key = append(key, length...)
		key = append(key, encoded...)
	}

	return string(key), nil
}

func evaluateGroupedBooleanExpression(
	rows []entityRow,
	expression *providerv1.Expression,
) (bool, error) {
	context := expressionContext{rows: rows, grouped: true}
	if len(rows) > 0 {
		context.row = &rows[0]
	}
	return evaluateBooleanExpression(context, expression)
}

func evaluateAggregate(
	rows []entityRow,
	aggregate *providerv1.AggregateCall,
) (*kublingv1.Value, error) {
	if aggregate == nil {
		return nil, fmt.Errorf("aggregate is required")
	}
	function := aggregate.GetFunction()
	if function == providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR {
		if len(aggregate.GetArguments()) != 0 || aggregate.GetDistinct() {
			return nil, fmt.Errorf("COUNT_STAR accepts no arguments or DISTINCT")
		}
		return aggregateNumberValue(
			new(big.Rat).SetInt64(int64(len(rows))),
			aggregate.GetResultType(),
		)
	}
	if len(aggregate.GetArguments()) != 1 {
		return nil, fmt.Errorf("%s requires exactly one argument", function)
	}

	values := make([]*kublingv1.Value, 0, len(rows))
	seen := make(map[string]struct{})
	for _, row := range rows {
		value, err := evaluateExpression(row, aggregate.GetArguments()[0])
		if err != nil {
			return nil, err
		}
		if isNullValue(value) {
			continue
		}
		if aggregate.GetDistinct() {
			key, err := aggregateValuesKey([]*kublingv1.Value{value})
			if err != nil {
				return nil, err
			}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
		}
		values = append(values, value)
	}

	switch function {
	case providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT,
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_BIG:
		return aggregateNumberValue(
			new(big.Rat).SetInt64(int64(len(values))),
			aggregate.GetResultType(),
		)
	case providerv1.AggregateFunction_AGGREGATE_FUNCTION_MIN,
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_MAX:
		if len(values) == 0 {
			return nullValue(), nil
		}
		selected := values[0]
		for _, candidate := range values[1:] {
			order, err := compareValues(candidate, selected)
			if err != nil {
				return nil, err
			}
			if (function == providerv1.AggregateFunction_AGGREGATE_FUNCTION_MIN && order < 0) ||
				(function == providerv1.AggregateFunction_AGGREGATE_FUNCTION_MAX && order > 0) {
				selected = candidate
			}
		}
		return coerceAggregateValue(selected, aggregate.GetResultType())
	case providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM,
		providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG:
		if len(values) == 0 {
			return nullValue(), nil
		}
		total := new(big.Rat)
		for _, value := range values {
			number, err := aggregateNumber(value)
			if err != nil {
				return nil, err
			}
			total.Add(total, number)
		}
		if function == providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG {
			total.Quo(total, new(big.Rat).SetInt64(int64(len(values))))
		}
		return aggregateNumberValue(total, aggregate.GetResultType())
	default:
		return nil, fmt.Errorf("unsupported aggregate function %s", function)
	}
}

func coerceAggregateValue(
	value *kublingv1.Value,
	resultType *kublingv1.TypeDescriptor,
) (*kublingv1.Value, error) {
	if resultType == nil {
		return nil, fmt.Errorf("aggregate result type is required")
	}
	actualType, err := valueType(value)
	if err != nil {
		return nil, err
	}
	if actualType == resultType.GetType() {
		return proto.Clone(value).(*kublingv1.Value), nil
	}
	number, err := aggregateNumber(value)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot return %s aggregate as %s",
			actualType,
			resultType.GetType(),
		)
	}
	return aggregateNumberValue(number, resultType)
}

func aggregateNumber(value *kublingv1.Value) (*big.Rat, error) {
	var text string
	switch kind := value.GetKind().(type) {
	case *kublingv1.Value_ByteValue:
		return new(big.Rat).SetInt64(int64(kind.ByteValue)), nil
	case *kublingv1.Value_ShortValue:
		return new(big.Rat).SetInt64(int64(kind.ShortValue)), nil
	case *kublingv1.Value_IntegerValue:
		return new(big.Rat).SetInt64(int64(kind.IntegerValue)), nil
	case *kublingv1.Value_LongValue:
		return new(big.Rat).SetInt64(kind.LongValue), nil
	case *kublingv1.Value_BigintegerValue:
		text = kind.BigintegerValue
	case *kublingv1.Value_FloatValue:
		text = strconv.FormatFloat(float64(kind.FloatValue), 'g', -1, 32)
	case *kublingv1.Value_DoubleValue:
		text = strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64)
	case *kublingv1.Value_BigdecimalValue:
		text = kind.BigdecimalValue
	default:
		return nil, fmt.Errorf("aggregate value is not numeric")
	}
	number, ok := new(big.Rat).SetString(text)
	if !ok {
		return nil, fmt.Errorf("invalid numeric value %q", text)
	}
	return number, nil
}

func aggregateNumberValue(
	number *big.Rat,
	resultType *kublingv1.TypeDescriptor,
) (*kublingv1.Value, error) {
	if resultType == nil {
		return nil, fmt.Errorf("aggregate result type is required")
	}
	typeName := resultType.GetType()
	switch typeName {
	case kublingv1.ValueType_VALUE_TYPE_BYTE,
		kublingv1.ValueType_VALUE_TYPE_SHORT,
		kublingv1.ValueType_VALUE_TYPE_INTEGER,
		kublingv1.ValueType_VALUE_TYPE_LONG,
		kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		if !number.IsInt() {
			return nil, fmt.Errorf("aggregate result %s is not integral", number.RatString())
		}
		integer := number.Num()
		switch typeName {
		case kublingv1.ValueType_VALUE_TYPE_BYTE:
			if !integer.IsInt64() || integer.Int64() < -128 || integer.Int64() > 127 {
				return nil, fmt.Errorf("aggregate result overflows BYTE")
			}
			return byteValue(int32(integer.Int64())), nil
		case kublingv1.ValueType_VALUE_TYPE_SHORT:
			if !integer.IsInt64() || integer.Int64() < -32768 || integer.Int64() > 32767 {
				return nil, fmt.Errorf("aggregate result overflows SHORT")
			}
			return shortValue(int32(integer.Int64())), nil
		case kublingv1.ValueType_VALUE_TYPE_INTEGER:
			if !integer.IsInt64() || integer.Int64() < -2147483648 || integer.Int64() > 2147483647 {
				return nil, fmt.Errorf("aggregate result overflows INTEGER")
			}
			return integerValue(int32(integer.Int64())), nil
		case kublingv1.ValueType_VALUE_TYPE_LONG:
			if !integer.IsInt64() {
				return nil, fmt.Errorf("aggregate result overflows LONG")
			}
			return longValue(integer.Int64()), nil
		default:
			return bigintegerValue(integer.String()), nil
		}
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		converted, _ := number.Float64()
		if math.IsInf(converted, 0) || math.Abs(converted) > math.MaxFloat32 {
			return nil, fmt.Errorf("aggregate result overflows FLOAT")
		}
		return floatValue(float32(converted)), nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		converted, _ := number.Float64()
		if math.IsInf(converted, 0) {
			return nil, fmt.Errorf("aggregate result overflows DOUBLE")
		}
		return doubleValue(converted), nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		return bigdecimalValue(aggregateDecimalString(number, resultType)), nil
	default:
		return nil, fmt.Errorf("aggregate result type %s is not numeric", typeName)
	}
}

func aggregateDecimalString(
	number *big.Rat,
	descriptor *kublingv1.TypeDescriptor,
) string {
	if descriptor.Scale != nil {
		scale := int(descriptor.GetScale())
		if scale >= 0 {
			return number.FloatString(scale)
		}
		factor := new(big.Int).Exp(
			big.NewInt(10),
			big.NewInt(int64(-scale)),
			nil,
		)
		scaled := new(big.Rat).Quo(number, new(big.Rat).SetInt(factor))
		rounded, _ := new(big.Int).SetString(scaled.FloatString(0), 10)
		return rounded.Mul(rounded, factor).String()
	}

	denominator := new(big.Int).Set(number.Denom())
	twos := 0
	for denominator.Bit(0) == 0 {
		denominator.Rsh(denominator, 1)
		twos++
	}
	fives := 0
	five := big.NewInt(5)
	remainder := new(big.Int)
	for {
		quotient := new(big.Int)
		quotient.QuoRem(denominator, five, remainder)
		if remainder.Sign() != 0 {
			break
		}
		denominator = quotient
		fives++
	}
	precision := max(twos, fives)
	if denominator.Cmp(big.NewInt(1)) != 0 {
		precision = 18
	}
	text := number.FloatString(precision)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return text
}

func sortAggregateRows(
	rows []plannedAggregateRow,
	orderBy []*providerv1.OrderBy,
) error {
	var compareErr error
	sort.SliceStable(rows, func(leftIndex, rightIndex int) bool {
		if compareErr != nil {
			return false
		}
		left := rows[leftIndex]
		right := rows[rightIndex]
		for index, ordering := range orderBy {
			order, err := compareOrderedValues(
				left.orderValues[index],
				right.orderValues[index],
				ordering,
			)
			if err != nil {
				compareErr = err
				return false
			}
			if order != 0 {
				return order < 0
			}
		}
		return left.key < right.key
	})
	return compareErr
}

func paginateAggregateRows(
	rows []plannedAggregateRow,
	offset *uint64,
	limit *uint64,
) []plannedAggregateRow {
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
