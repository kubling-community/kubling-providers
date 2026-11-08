package provider

import (
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

type identityFilter struct {
	key     string
	hasKey  bool
	noMatch bool
}

func parseIdentityFilter(expression *providerv1.Expression, namespace string) (identityFilter, error) {
	var result identityFilter
	if expression == nil {
		return result, nil
	}
	if err := collectIdentityPredicates(expression, namespace, &result); err != nil {
		return identityFilter{}, err
	}
	if result.hasKey {
		if err := backend.ValidateKey(result.key); err != nil {
			return identityFilter{}, err
		}
	}
	return result, nil
}

func collectIdentityPredicates(
	expression *providerv1.Expression,
	namespace string,
	result *identityFilter,
) error {
	if expression == nil {
		return fmt.Errorf("filter expression is required")
	}
	if logical := expression.GetLogical(); logical != nil {
		if logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND {
			return fmt.Errorf("only logical AND is supported")
		}
		if len(logical.GetOperands()) < 2 {
			return fmt.Errorf("logical AND requires at least two operands")
		}
		for _, operand := range logical.GetOperands() {
			if err := collectIdentityPredicates(operand, namespace, result); err != nil {
				return err
			}
		}
		return nil
	}

	comparison := expression.GetComparison()
	if comparison == nil ||
		comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
		return fmt.Errorf("only equality predicates on bucket and key are supported")
	}
	field, value, ok := fieldStringLiteral(comparison.GetLeft(), comparison.GetRight())
	if !ok {
		field, value, ok = fieldStringLiteral(comparison.GetRight(), comparison.GetLeft())
	}
	if !ok {
		return fmt.Errorf("equality predicate must compare a field with a string literal")
	}

	switch normalizeName(field) {
	case catalog.BucketColumn:
		if value != namespace {
			result.noMatch = true
		}
	case catalog.KeyColumn:
		if result.hasKey && result.key != value {
			result.noMatch = true
		}
		result.key = value
		result.hasKey = true
	default:
		return fmt.Errorf("field %q cannot be pushed as an object identity predicate", field)
	}
	return nil
}

func fieldStringLiteral(
	fieldExpression *providerv1.Expression,
	literalExpression *providerv1.Expression,
) (string, string, bool) {
	field := fieldExpression.GetField()
	literal := literalExpression.GetLiteral()
	if field == nil || literal == nil || literal.GetValue() == nil {
		return "", "", false
	}
	value, ok := literal.GetValue().GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return "", "", false
	}
	return strings.TrimSpace(field.GetName()), value.StringValue, true
}

func literalValue(expression *providerv1.Expression) (*kublingv1.Value, error) {
	if expression == nil || expression.GetLiteral() == nil || expression.GetLiteral().GetValue() == nil {
		return nil, fmt.Errorf("a literal value is required")
	}
	return expression.GetLiteral().GetValue(), nil
}
