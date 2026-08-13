package openapi

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func planQueryParameters(descriptor *entityDescriptor, filter *providerv1.Expression) (url.Values, error) {
	parameters := make(url.Values, len(descriptor.config.QueryParameters)+len(descriptor.config.EqualityFilters))
	for _, configured := range descriptor.config.QueryParameters {
		parameters.Set(configured.Name, configured.Value)
	}
	if filter != nil {
		if err := appendFilterParameters(parameters, descriptor, filter); err != nil {
			return nil, err
		}
	}
	for parameter, field := range descriptor.requiredBindings {
		if _, exists := parameters[parameter]; !exists {
			return nil, fmt.Errorf("required query parameter %q requires an equality filter on field %q", parameter, field)
		}
	}
	return parameters, nil
}

func appendFilterParameters(parameters url.Values, descriptor *entityDescriptor, expression *providerv1.Expression) error {
	if expression == nil {
		return fmt.Errorf("filter expression is required")
	}
	if comparison := expression.GetComparison(); comparison != nil {
		return appendComparisonParameter(parameters, descriptor, comparison)
	}
	if logical := expression.GetLogical(); logical != nil {
		if logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND {
			return fmt.Errorf("only logical AND is supported")
		}
		if len(logical.GetOperands()) < 2 {
			return fmt.Errorf("logical AND requires at least two operands")
		}
		for _, operand := range logical.GetOperands() {
			if err := appendFilterParameters(parameters, descriptor, operand); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("only equality comparisons and logical AND are supported")
}

func appendComparisonParameter(
	parameters url.Values,
	descriptor *entityDescriptor,
	comparison *providerv1.ComparisonExpression,
) error {
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
	fieldName := strings.TrimSpace(field.GetName())
	parameter := descriptor.equalityFilters[strings.ToUpper(fieldName)]
	if parameter == "" {
		return fmt.Errorf("field %q has no equality filter binding", fieldName)
	}
	value, err := queryLiteralValue(literal.GetValue())
	if err != nil {
		return fmt.Errorf("field %q: %w", fieldName, err)
	}
	if existing, exists := parameters[parameter]; exists {
		if len(existing) != 1 || existing[0] != value {
			return fmt.Errorf("query parameter %q has conflicting filter values", parameter)
		}
		return nil
	}
	parameters.Set(parameter, value)
	return nil
}

func fieldAndLiteral(fieldExpression, literalExpression *providerv1.Expression) (*providerv1.FieldReference, *providerv1.Literal, bool) {
	if fieldExpression == nil || literalExpression == nil {
		return nil, nil, false
	}
	field := fieldExpression.GetField()
	literal := literalExpression.GetLiteral()
	return field, literal, field != nil && literal != nil && literal.GetValue() != nil
}

func queryLiteralValue(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("literal value is required")
	}
	switch kind := value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return kind.StringValue, nil
	case *kublingv1.Value_CharValue:
		return kind.CharValue, nil
	case *kublingv1.Value_BooleanValue:
		return strconv.FormatBool(kind.BooleanValue), nil
	case *kublingv1.Value_ByteValue:
		return strconv.FormatInt(int64(kind.ByteValue), 10), nil
	case *kublingv1.Value_ShortValue:
		return strconv.FormatInt(int64(kind.ShortValue), 10), nil
	case *kublingv1.Value_IntegerValue:
		return strconv.FormatInt(int64(kind.IntegerValue), 10), nil
	case *kublingv1.Value_LongValue:
		return strconv.FormatInt(kind.LongValue, 10), nil
	case *kublingv1.Value_BigintegerValue:
		return kind.BigintegerValue, nil
	case *kublingv1.Value_FloatValue:
		return strconv.FormatFloat(float64(kind.FloatValue), 'g', -1, 32), nil
	case *kublingv1.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64), nil
	case *kublingv1.Value_BigdecimalValue:
		return kind.BigdecimalValue, nil
	case *kublingv1.Value_DateValue:
		return kind.DateValue, nil
	case *kublingv1.Value_TimeValue:
		return kind.TimeValue, nil
	case *kublingv1.Value_TimestampValue:
		return kind.TimestampValue, nil
	case *kublingv1.Value_VarbinaryValue:
		return base64.StdEncoding.EncodeToString(kind.VarbinaryValue), nil
	case *kublingv1.Value_NullValue:
		return "", fmt.Errorf("null literals cannot be encoded as query parameters")
	default:
		return "", fmt.Errorf("literal type %T cannot be encoded as a query parameter", kind)
	}
}
