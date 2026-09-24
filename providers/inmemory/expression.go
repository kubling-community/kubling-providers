package inmemory

import (
	"fmt"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

func evaluateExpression(
	row entityRow,
	expression *providerv1.Expression,
) (*kublingv1.Value, error) {
	return evaluateExpressionInContext(expressionContext{row: &row}, expression)
}

type expressionContext struct {
	row     *entityRow
	rows    []entityRow
	grouped bool
}

func evaluateGroupedExpression(
	rows []entityRow,
	expression *providerv1.Expression,
) (*kublingv1.Value, error) {
	context := expressionContext{rows: rows, grouped: true}
	if len(rows) > 0 {
		context.row = &rows[0]
	}

	return evaluateExpressionInContext(context, expression)
}

func evaluateExpressionInContext(
	context expressionContext,
	expression *providerv1.Expression,
) (*kublingv1.Value, error) {
	if expression == nil {
		return nil, fmt.Errorf("expression is required")
	}

	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Field:
		if kind.Field == nil || kind.Field.GetName() == "" {
			return nil, fmt.Errorf("field name is required")
		}
		if context.row == nil {
			return nil, fmt.Errorf(
				"field %q is unavailable for an empty aggregate group",
				kind.Field.GetName(),
			)
		}

		return rowValue(*context.row, kind.Field.GetName())
	case *providerv1.Expression_Literal:
		if kind.Literal == nil || kind.Literal.GetValue() == nil {
			return nil, fmt.Errorf("literal value is required")
		}

		return proto.Clone(kind.Literal.GetValue()).(*kublingv1.Value), nil
	case *providerv1.Expression_Comparison:
		return evaluateComparison(context, kind.Comparison)
	case *providerv1.Expression_Logical:
		return evaluateLogical(context, kind.Logical)
	case *providerv1.Expression_NullPredicate:
		return evaluateNullPredicate(context, kind.NullPredicate)
	case *providerv1.Expression_FunctionCall:
		if kind.FunctionCall == nil {
			return nil, fmt.Errorf("function call is required")
		}

		return nil, fmt.Errorf(
			"function %q is not supported",
			kind.FunctionCall.GetName(),
		)
	case *providerv1.Expression_Aggregate:
		if !context.grouped {
			return nil, fmt.Errorf("aggregate is not valid in a row expression")
		}
		return evaluateAggregate(context.rows, kind.Aggregate)
	default:
		return nil, fmt.Errorf("expression kind is required")
	}
}

func evaluateFilter(
	row entityRow,
	expression *providerv1.Expression,
) (bool, error) {
	if expression == nil {
		return true, nil
	}

	value, err := evaluateExpression(row, expression)
	if err != nil {
		return false, err
	}

	if isNullValue(value) {
		return false, nil
	}

	booleanKind, ok := value.GetKind().(*kublingv1.Value_BooleanValue)
	if !ok {
		return false, fmt.Errorf("filter expression must return boolean")
	}

	return booleanKind.BooleanValue, nil
}

func evaluateComparison(
	context expressionContext,
	comparison *providerv1.ComparisonExpression,
) (*kublingv1.Value, error) {
	if comparison == nil {
		return nil, fmt.Errorf("comparison is required")
	}

	left, err := evaluateExpressionInContext(context, comparison.GetLeft())
	if err != nil {
		return nil, fmt.Errorf("evaluate comparison left operand: %w", err)
	}

	right, err := evaluateExpressionInContext(context, comparison.GetRight())
	if err != nil {
		return nil, fmt.Errorf("evaluate comparison right operand: %w", err)
	}

	if isNullValue(left) || isNullValue(right) {
		return booleanValue(false), nil
	}

	order, err := compareValues(left, right)
	if err != nil {
		return nil, err
	}

	var result bool

	switch comparison.GetOperator() {
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL:
		result = order == 0
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_NOT_EQUAL:
		result = order != 0
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN:
		result = order < 0
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
		result = order <= 0
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN:
		result = order > 0
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
		result = order >= 0
	default:
		return nil, fmt.Errorf("comparison operator is required")
	}

	return booleanValue(result), nil
}

func evaluateLogical(
	context expressionContext,
	logical *providerv1.LogicalExpression,
) (*kublingv1.Value, error) {
	if logical == nil {
		return nil, fmt.Errorf("logical expression is required")
	}

	operands := logical.GetOperands()

	switch logical.GetOperator() {
	case providerv1.LogicalOperator_LOGICAL_OPERATOR_AND:
		if len(operands) < 2 {
			return nil, fmt.Errorf("AND requires at least two operands")
		}

		for _, operand := range operands {
			value, err := evaluateBooleanExpression(context, operand)
			if err != nil {
				return nil, err
			}
			if !value {
				return booleanValue(false), nil
			}
		}

		return booleanValue(true), nil
	case providerv1.LogicalOperator_LOGICAL_OPERATOR_OR:
		if len(operands) < 2 {
			return nil, fmt.Errorf("OR requires at least two operands")
		}

		for _, operand := range operands {
			value, err := evaluateBooleanExpression(context, operand)
			if err != nil {
				return nil, err
			}
			if value {
				return booleanValue(true), nil
			}
		}

		return booleanValue(false), nil
	case providerv1.LogicalOperator_LOGICAL_OPERATOR_NOT:
		if len(operands) != 1 {
			return nil, fmt.Errorf("NOT requires exactly one operand")
		}

		value, err := evaluateBooleanExpression(context, operands[0])
		if err != nil {
			return nil, err
		}

		return booleanValue(!value), nil
	default:
		return nil, fmt.Errorf("logical operator is required")
	}
}

func evaluateNullPredicate(
	context expressionContext,
	predicate *providerv1.NullPredicate,
) (*kublingv1.Value, error) {
	if predicate == nil {
		return nil, fmt.Errorf("null predicate is required")
	}

	value, err := evaluateExpressionInContext(context, predicate.GetExpression())
	if err != nil {
		return nil, err
	}

	isNull := isNullValue(value)

	switch predicate.GetOperator() {
	case providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NULL:
		return booleanValue(isNull), nil
	case providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NOT_NULL:
		return booleanValue(!isNull), nil
	default:
		return nil, fmt.Errorf("null predicate operator is required")
	}
}

func evaluateBooleanExpression(
	context expressionContext,
	expression *providerv1.Expression,
) (bool, error) {
	value, err := evaluateExpressionInContext(context, expression)
	if err != nil {
		return false, err
	}
	if isNullValue(value) {
		return false, nil
	}
	booleanKind, ok := value.GetKind().(*kublingv1.Value_BooleanValue)
	if !ok {
		return false, fmt.Errorf("predicate expression must return boolean")
	}

	return booleanKind.BooleanValue, nil
}

func expressionValueType(
	entity *entityDefinition,
	expression *providerv1.Expression,
) (kublingv1.ValueType, error) {
	if expression == nil {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("projection expression is required")
	}

	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Field:
		if kind.Field == nil {
			return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
				fmt.Errorf("projection field is required")
		}

		field, found := entity.fieldByName(kind.Field.GetName())
		if !found {
			return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
				fmt.Errorf(
					"unknown %s field %q",
					entity.name,
					kind.Field.GetName(),
				)
		}

		return field.valueType, nil
	case *providerv1.Expression_Literal:
		if kind.Literal == nil || kind.Literal.GetValue() == nil {
			return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
				fmt.Errorf("projection literal is required")
		}
		if declaredType := kind.Literal.GetDeclaredType(); declaredType != nil {
			return declaredType.GetType(), nil
		}

		return valueType(kind.Literal.GetValue())
	case *providerv1.Expression_Comparison,
		*providerv1.Expression_Logical,
		*providerv1.Expression_NullPredicate:
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nil
	case *providerv1.Expression_FunctionCall:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("function projections are not supported")
	case *providerv1.Expression_Aggregate:
		if kind.Aggregate == nil || kind.Aggregate.GetResultType() == nil {
			return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
				fmt.Errorf("aggregate result type is required")
		}
		return kind.Aggregate.GetResultType().GetType(), nil
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("projection expression kind is required")
	}
}

func valueType(value *kublingv1.Value) (kublingv1.ValueType, error) {
	if value == nil {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("value is required")
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
	case *kublingv1.Value_GeometryWithCrs:
		return kublingv1.ValueType_VALUE_TYPE_GEOMETRY, nil
	case *kublingv1.Value_GeographyValue:
		return kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY, nil
	case *kublingv1.Value_GeographyWithCrs:
		return kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY, nil
	case *kublingv1.Value_JsonValue:
		return kublingv1.ValueType_VALUE_TYPE_JSON, nil
	case *kublingv1.Value_XmlValue:
		return kublingv1.ValueType_VALUE_TYPE_XML, nil
	case *kublingv1.Value_ArrayValue:
		return kublingv1.ValueType_VALUE_TYPE_ARRAY, nil
	case *kublingv1.Value_NullValue:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("cannot infer the type of a null literal")
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN,
			fmt.Errorf("value kind is required")
	}
}

func compareValues(
	left *kublingv1.Value,
	right *kublingv1.Value,
) (int, error) {
	if isNumericValue(left) {
		if !isNumericValue(right) {
			return 0, fmt.Errorf("cannot compare numeric and non-numeric values")
		}
		leftNumber, err := aggregateNumber(left)
		if err != nil {
			return 0, err
		}
		rightNumber, err := aggregateNumber(right)
		if err != nil {
			return 0, err
		}
		return leftNumber.Cmp(rightNumber), nil
	}

	switch leftKind := left.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		rightKind, ok :=
			right.GetKind().(*kublingv1.Value_StringValue)
		if !ok {
			return 0, fmt.Errorf("cannot compare string and non-string values")
		}

		return compareOrdered(leftKind.StringValue, rightKind.StringValue), nil
	case *kublingv1.Value_BooleanValue:
		rightKind, ok :=
			right.GetKind().(*kublingv1.Value_BooleanValue)
		if !ok {
			return 0, fmt.Errorf("cannot compare boolean and non-boolean values")
		}

		switch {
		case leftKind.BooleanValue == rightKind.BooleanValue:
			return 0, nil
		case !leftKind.BooleanValue && rightKind.BooleanValue:
			return -1, nil
		default:
			return 1, nil
		}
	default:
		return 0, fmt.Errorf("value type is not comparable")
	}
}

func isNumericValue(value *kublingv1.Value) bool {
	switch value.GetKind().(type) {
	case *kublingv1.Value_ByteValue,
		*kublingv1.Value_ShortValue,
		*kublingv1.Value_IntegerValue,
		*kublingv1.Value_LongValue,
		*kublingv1.Value_BigintegerValue,
		*kublingv1.Value_FloatValue,
		*kublingv1.Value_DoubleValue,
		*kublingv1.Value_BigdecimalValue:
		return true
	default:
		return false
	}
}

func compareOrdered[T ~string](left, right T) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func isNullValue(value *kublingv1.Value) bool {
	if value == nil {
		return true
	}

	_, ok := value.GetKind().(*kublingv1.Value_NullValue)

	return ok
}
