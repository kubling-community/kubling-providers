package redis

import (
	"bytes"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

type redisRow struct {
	key    string
	values map[string]*kublingv1.Value
}

func (r redisRow) value(name string) (*kublingv1.Value, error) {
	value := r.values[strings.ToUpper(name)]
	if value == nil {
		return nil, fmt.Errorf("unknown field %q", name)
	}
	return proto.Clone(value).(*kublingv1.Value), nil
}

func evaluateExpression(row redisRow, expression *providerv1.Expression) (*kublingv1.Value, error) {
	if expression == nil {
		return nil, fmt.Errorf("expression is required")
	}
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Field:
		if kind.Field == nil || kind.Field.GetName() == "" {
			return nil, fmt.Errorf("field name is required")
		}
		return row.value(kind.Field.GetName())
	case *providerv1.Expression_Literal:
		if kind.Literal == nil || kind.Literal.GetValue() == nil {
			return nil, fmt.Errorf("literal value is required")
		}
		return proto.Clone(kind.Literal.GetValue()).(*kublingv1.Value), nil
	case *providerv1.Expression_Comparison:
		return evaluateComparison(row, kind.Comparison)
	case *providerv1.Expression_Logical:
		return evaluateLogical(row, kind.Logical)
	case *providerv1.Expression_NullPredicate:
		return evaluateNullPredicate(row, kind.NullPredicate)
	case *providerv1.Expression_Pattern:
		return evaluatePattern(row, kind.Pattern)
	default:
		return nil, fmt.Errorf("unsupported expression kind %T", kind)
	}
}

func evaluateFilter(row redisRow, expression *providerv1.Expression) (bool, error) {
	if expression == nil {
		return true, nil
	}
	value, err := evaluateExpression(row, expression)
	if err != nil {
		return false, err
	}
	if value.GetNullValue() != nil {
		return false, nil
	}
	boolean, ok := value.GetKind().(*kublingv1.Value_BooleanValue)
	if !ok {
		return false, fmt.Errorf("filter expression must return boolean")
	}
	return boolean.BooleanValue, nil
}

func evaluateComparison(row redisRow, comparison *providerv1.ComparisonExpression) (*kublingv1.Value, error) {
	if comparison == nil {
		return nil, fmt.Errorf("comparison is required")
	}
	left, err := evaluateExpression(row, comparison.GetLeft())
	if err != nil {
		return nil, err
	}
	right, err := evaluateExpression(row, comparison.GetRight())
	if err != nil {
		return nil, err
	}
	if left.GetNullValue() != nil || right.GetNullValue() != nil {
		return boolValue(false), nil
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
	return boolValue(result), nil
}

func evaluateLogical(row redisRow, logical *providerv1.LogicalExpression) (*kublingv1.Value, error) {
	if logical == nil || len(logical.GetOperands()) < 2 {
		return nil, fmt.Errorf("logical expression requires at least two operands")
	}
	switch logical.GetOperator() {
	case providerv1.LogicalOperator_LOGICAL_OPERATOR_AND:
		for _, operand := range logical.GetOperands() {
			matches, err := evaluateFilter(row, operand)
			if err != nil {
				return nil, err
			}
			if !matches {
				return boolValue(false), nil
			}
		}
		return boolValue(true), nil
	case providerv1.LogicalOperator_LOGICAL_OPERATOR_OR:
		for _, operand := range logical.GetOperands() {
			matches, err := evaluateFilter(row, operand)
			if err != nil {
				return nil, err
			}
			if matches {
				return boolValue(true), nil
			}
		}
		return boolValue(false), nil
	default:
		return nil, fmt.Errorf("unsupported logical operator %s", logical.GetOperator())
	}
}

func evaluateNullPredicate(row redisRow, predicate *providerv1.NullPredicate) (*kublingv1.Value, error) {
	if predicate == nil {
		return nil, fmt.Errorf("null predicate is required")
	}
	value, err := evaluateExpression(row, predicate.GetExpression())
	if err != nil {
		return nil, err
	}
	isNull := value.GetNullValue() != nil
	switch predicate.GetOperator() {
	case providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NULL:
		return boolValue(isNull), nil
	case providerv1.NullPredicateOperator_NULL_PREDICATE_OPERATOR_IS_NOT_NULL:
		return boolValue(!isNull), nil
	default:
		return nil, fmt.Errorf("null predicate operator is required")
	}
}

func evaluatePattern(row redisRow, pattern *providerv1.PatternExpression) (*kublingv1.Value, error) {
	if pattern == nil {
		return nil, fmt.Errorf("pattern expression is required")
	}
	value, err := evaluateExpression(row, pattern.GetValue())
	if err != nil {
		return nil, err
	}
	patternValue, err := evaluateExpression(row, pattern.GetPattern())
	if err != nil {
		return nil, err
	}
	if value.GetNullValue() != nil || patternValue.GetNullValue() != nil {
		return boolValue(false), nil
	}
	valueString, ok := value.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return nil, fmt.Errorf("LIKE value must be a string")
	}
	patternString, ok := patternValue.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return nil, fmt.Errorf("LIKE pattern must be a string")
	}
	matcher, err := compileLikePattern(patternString.StringValue, pattern.Escape)
	if err != nil {
		return nil, err
	}
	matched := matcher.MatchString(valueString.StringValue)
	switch pattern.GetOperator() {
	case providerv1.PatternOperator_PATTERN_OPERATOR_LIKE:
		return boolValue(matched), nil
	case providerv1.PatternOperator_PATTERN_OPERATOR_NOT_LIKE:
		return boolValue(!matched), nil
	default:
		return nil, fmt.Errorf("pattern operator is required")
	}
}

func compileLikePattern(pattern string, escape *string) (*regexp.Regexp, error) {
	var escapeRune rune
	if escape != nil {
		runes := []rune(*escape)
		if len(runes) != 1 {
			return nil, fmt.Errorf("LIKE escape must contain exactly one character")
		}
		escapeRune = runes[0]
	}
	var expression strings.Builder
	expression.WriteString("^")
	escaped := false
	for _, character := range pattern {
		if escape != nil && !escaped && character == escapeRune {
			escaped = true
			continue
		}
		if !escaped {
			switch character {
			case '%':
				expression.WriteString(".*")
				continue
			case '_':
				expression.WriteString(".")
				continue
			}
		}
		expression.WriteString(regexp.QuoteMeta(string(character)))
		escaped = false
	}
	if escaped {
		return nil, fmt.Errorf("LIKE pattern ends with its escape character")
	}
	expression.WriteString("$")
	return regexp.Compile(expression.String())
}

func compareValues(left *kublingv1.Value, right *kublingv1.Value) (int, error) {
	if leftNumber, ok := numericValue(left); ok {
		rightNumber, rightOK := numericValue(right)
		if !rightOK {
			return 0, fmt.Errorf("cannot compare numeric and non-numeric values")
		}
		return leftNumber.Cmp(rightNumber), nil
	}
	leftBytes, leftOK := binaryValue(left)
	if leftOK {
		rightBytes, rightOK := binaryValue(right)
		if !rightOK {
			return 0, fmt.Errorf("cannot compare binary and non-binary values")
		}
		return bytes.Compare(leftBytes, rightBytes), nil
	}
	leftText, leftOK := textValue(left)
	rightText, rightOK := textValue(right)
	if leftOK && rightOK {
		return strings.Compare(leftText, rightText), nil
	}
	leftBool, leftOK := left.GetKind().(*kublingv1.Value_BooleanValue)
	rightBool, rightOK := right.GetKind().(*kublingv1.Value_BooleanValue)
	if leftOK && rightOK {
		switch {
		case leftBool.BooleanValue == rightBool.BooleanValue:
			return 0, nil
		case !leftBool.BooleanValue:
			return -1, nil
		default:
			return 1, nil
		}
	}
	return 0, fmt.Errorf("incompatible comparison values %T and %T", left.GetKind(), right.GetKind())
}

func numericValue(value *kublingv1.Value) (*big.Rat, bool) {
	var raw string
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_ByteValue:
		raw = fmt.Sprint(typed.ByteValue)
	case *kublingv1.Value_ShortValue:
		raw = fmt.Sprint(typed.ShortValue)
	case *kublingv1.Value_IntegerValue:
		raw = fmt.Sprint(typed.IntegerValue)
	case *kublingv1.Value_LongValue:
		raw = fmt.Sprint(typed.LongValue)
	case *kublingv1.Value_BigintegerValue:
		raw = typed.BigintegerValue
	case *kublingv1.Value_FloatValue:
		raw = fmt.Sprint(typed.FloatValue)
	case *kublingv1.Value_DoubleValue:
		raw = fmt.Sprint(typed.DoubleValue)
	case *kublingv1.Value_BigdecimalValue:
		raw = typed.BigdecimalValue
	default:
		return nil, false
	}
	parsed, ok := new(big.Rat).SetString(raw)
	return parsed, ok
}

func textValue(value *kublingv1.Value) (string, bool) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return typed.StringValue, true
	case *kublingv1.Value_CharValue:
		return typed.CharValue, true
	case *kublingv1.Value_DateValue:
		return typed.DateValue, true
	case *kublingv1.Value_TimeValue:
		return typed.TimeValue, true
	case *kublingv1.Value_TimestampValue:
		return typed.TimestampValue, true
	case *kublingv1.Value_ClobValue:
		return typed.ClobValue.GetData(), true
	case *kublingv1.Value_JsonValue:
		return typed.JsonValue, true
	case *kublingv1.Value_XmlValue:
		return typed.XmlValue, true
	default:
		return "", false
	}
}

func binaryValue(value *kublingv1.Value) ([]byte, bool) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_VarbinaryValue:
		return typed.VarbinaryValue, true
	case *kublingv1.Value_BlobValue:
		return typed.BlobValue.GetData(), true
	case *kublingv1.Value_GeometryValue:
		return typed.GeometryValue, true
	case *kublingv1.Value_GeographyValue:
		return typed.GeographyValue, true
	default:
		return nil, false
	}
}

func boolValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}
