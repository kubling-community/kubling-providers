package openapi

import (
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

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

func fieldAndLiteral(fieldExpression, literalExpression *providerv1.Expression) (*providerv1.FieldReference, *providerv1.Literal, bool) {
	if fieldExpression == nil || literalExpression == nil {
		return nil, nil, false
	}
	field := fieldExpression.GetField()
	literal := literalExpression.GetLiteral()
	return field, literal, field != nil && literal != nil && literal.GetValue() != nil
}
