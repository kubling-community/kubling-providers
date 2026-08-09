package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type keySelector struct {
	exact   string
	pattern string
}

func resolveKeys(
	ctx context.Context,
	resolved *resolvedTable,
	filter *providerv1.Expression,
) ([]string, error) {
	selectors, covered, err := keySelectors(resolved.table, filter)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Redis key filter: %v", err)
	}
	if !covered || len(selectors) == 0 {
		selectors = []keySelector{{pattern: resolved.table.KeyPrefix + "*"}}
	}

	keys := make(map[string]struct{})
	for _, selector := range selectors {
		if selector.exact != "" {
			keys[selector.exact] = struct{}{}
			continue
		}
		var cursor uint64
		for {
			matched, next, err := resolved.client.Scan(
				ctx,
				cursor,
				selector.pattern,
				resolved.config.ScanCount,
			)
			if err != nil {
				return nil, operationError("scan Redis keys", err)
			}
			for _, key := range matched {
				keys[key] = struct{}{}
				if len(keys) > resolved.config.MaxScannedKeys {
					return nil, status.Errorf(
						codes.ResourceExhausted,
						"Redis query exceeded maxScannedKeys=%d",
						resolved.config.MaxScannedKeys,
					)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	result := make([]string, 0, len(keys))
	for key := range keys {
		if !strings.HasPrefix(key, resolved.table.KeyPrefix) {
			continue
		}
		keyType, err := resolved.client.Type(ctx, key)
		if err != nil {
			return nil, operationError("inspect Redis key type", err)
		}
		if keyType == "hash" {
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result, nil
}

func keySelectors(table *TableConfig, expression *providerv1.Expression) ([]keySelector, bool, error) {
	if expression == nil {
		return nil, false, nil
	}
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Comparison:
		return comparisonKeySelector(table, kind.Comparison)
	case *providerv1.Expression_Pattern:
		return patternKeySelector(table, kind.Pattern)
	case *providerv1.Expression_Logical:
		if kind.Logical == nil || len(kind.Logical.GetOperands()) < 2 {
			return nil, false, fmt.Errorf("logical expression requires at least two operands")
		}
		var selectors []keySelector
		switch kind.Logical.GetOperator() {
		case providerv1.LogicalOperator_LOGICAL_OPERATOR_AND:
			covered := false
			for _, operand := range kind.Logical.GetOperands() {
				planned, operandCovered, err := keySelectors(table, operand)
				if err != nil {
					return nil, false, err
				}
				selectors = append(selectors, planned...)
				covered = covered || operandCovered
			}
			return selectors, covered, nil
		case providerv1.LogicalOperator_LOGICAL_OPERATOR_OR:
			for _, operand := range kind.Logical.GetOperands() {
				planned, covered, err := keySelectors(table, operand)
				if err != nil {
					return nil, false, err
				}
				if !covered {
					return nil, false, nil
				}
				selectors = append(selectors, planned...)
			}
			return selectors, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported logical operator %s", kind.Logical.GetOperator())
		}
	default:
		return nil, false, nil
	}
}

func comparisonKeySelector(
	table *TableConfig,
	comparison *providerv1.ComparisonExpression,
) ([]keySelector, bool, error) {
	if comparison == nil || comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
		return nil, false, nil
	}
	field, literal := fieldAndLiteral(comparison.GetLeft(), comparison.GetRight())
	if field == nil || literal == nil || !strings.EqualFold(field.GetName(), table.Key.Name) {
		return nil, false, nil
	}
	raw, null, err := encodeRedisValue(literal.GetValue(), table.Key.Type)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, false, fmt.Errorf("Redis key cannot be null")
	}
	return []keySelector{{exact: table.KeyPrefix + raw}}, true, nil
}

func patternKeySelector(
	table *TableConfig,
	pattern *providerv1.PatternExpression,
) ([]keySelector, bool, error) {
	if pattern == nil || pattern.GetOperator() != providerv1.PatternOperator_PATTERN_OPERATOR_LIKE {
		return nil, false, nil
	}
	field := pattern.GetValue().GetField()
	literal := pattern.GetPattern().GetLiteral()
	if field == nil || literal == nil || !strings.EqualFold(field.GetName(), table.Key.Name) {
		return nil, false, nil
	}
	if table.Key.Type != kublingv1.ValueType_VALUE_TYPE_STRING &&
		table.Key.Type != kublingv1.ValueType_VALUE_TYPE_CHAR {
		return nil, false, fmt.Errorf("LIKE requires a string Redis key")
	}
	text, ok := literal.GetValue().GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return nil, false, fmt.Errorf("Redis key LIKE pattern must be a string")
	}
	glob, err := likeToRedisGlob(text.StringValue, pattern.Escape)
	if err != nil {
		return nil, false, err
	}
	return []keySelector{{pattern: table.KeyPrefix + glob}}, true, nil
}

func fieldAndLiteral(
	left *providerv1.Expression,
	right *providerv1.Expression,
) (*providerv1.FieldReference, *providerv1.Literal) {
	if left.GetField() != nil && right.GetLiteral() != nil {
		return left.GetField(), right.GetLiteral()
	}
	if right.GetField() != nil && left.GetLiteral() != nil {
		return right.GetField(), left.GetLiteral()
	}
	return nil, nil
}

func likeToRedisGlob(pattern string, escape *string) (string, error) {
	var escapeRune rune
	if escape != nil {
		runes := []rune(*escape)
		if len(runes) != 1 {
			return "", fmt.Errorf("LIKE escape must contain exactly one character")
		}
		escapeRune = runes[0]
	}
	var glob strings.Builder
	escaped := false
	for _, character := range pattern {
		if escape != nil && !escaped && character == escapeRune {
			escaped = true
			continue
		}
		if !escaped {
			switch character {
			case '%':
				glob.WriteRune('*')
				continue
			case '_':
				glob.WriteRune('?')
				continue
			}
		}
		if strings.ContainsRune(`*?[]\`, character) {
			glob.WriteRune('\\')
		}
		glob.WriteRune(character)
		escaped = false
	}
	if escaped {
		return "", fmt.Errorf("LIKE pattern ends with its escape character")
	}
	return glob.String(), nil
}
