package cassandra

import (
	"fmt"
	"math"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

type projectionPlan struct {
	column     *gocql.ColumnMetadata
	outputName string
}

func planQueryProjections(
	table *gocql.TableMetadata,
	projections []*providerv1.Projection,
) ([]projectionPlan, error) {
	if len(projections) == 0 {
		planned := make([]projectionPlan, 0, len(table.Columns))
		for _, columnName := range orderedColumnNames(table) {
			column := table.Columns[columnName]
			if column != nil {
				planned = append(planned, projectionPlan{
					column:     column,
					outputName: column.Name,
				})
			}
		}
		return planned, nil
	}

	planned := make([]projectionPlan, 0, len(projections))
	for _, projection := range projections {
		if projection == nil || projection.GetExpression() == nil {
			return nil, fmt.Errorf("projection expression is required")
		}
		field := projection.GetExpression().GetField()
		if field == nil || strings.TrimSpace(field.GetName()) == "" {
			return nil, fmt.Errorf("Cassandra projections support only field references")
		}
		column := findColumn(table, field.GetName())
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", field.GetName())
		}

		outputName := strings.TrimSpace(projection.GetOutputName())
		if outputName == "" {
			outputName = column.Name
		}
		planned = append(planned, projectionPlan{
			column:     column,
			outputName: outputName,
		})
	}

	return planned, nil
}

func selectColumns(projections []projectionPlan) string {
	columns := make([]string, 0, len(projections))
	for _, projection := range projections {
		columns = append(columns, quoteIdentifier(projection.column.Name))
	}
	return strings.Join(columns, ", ")
}

func buildFilter(
	table *gocql.TableMetadata,
	expression *providerv1.Expression,
) (string, []any, error) {
	if expression == nil {
		return "", nil, nil
	}

	if comparison := expression.GetComparison(); comparison != nil {
		return buildComparison(table, comparison)
	}
	if logical := expression.GetLogical(); logical != nil {
		if logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND {
			return "", nil, fmt.Errorf("Cassandra filters support only logical AND")
		}
		if len(logical.GetOperands()) < 2 {
			return "", nil, fmt.Errorf("logical AND requires at least two operands")
		}

		parts := make([]string, 0, len(logical.GetOperands()))
		var values []any
		for _, operand := range logical.GetOperands() {
			part, operandValues, err := buildFilter(table, operand)
			if err != nil {
				return "", nil, err
			}
			if part == "" {
				return "", nil, fmt.Errorf("logical operand is required")
			}
			parts = append(parts, "("+part+")")
			values = append(values, operandValues...)
		}

		return strings.Join(parts, " AND "), values, nil
	}

	return "", nil, fmt.Errorf("unsupported Cassandra filter expression")
}

func buildComparison(
	table *gocql.TableMetadata,
	comparison *providerv1.ComparisonExpression,
) (string, []any, error) {
	leftField := comparison.GetLeft().GetField()
	rightField := comparison.GetRight().GetField()
	leftLiteral := comparison.GetLeft().GetLiteral()
	rightLiteral := comparison.GetRight().GetLiteral()

	operator := comparison.GetOperator()
	var field *providerv1.FieldReference
	var literal *providerv1.Literal
	if leftField != nil && rightLiteral != nil {
		field = leftField
		literal = rightLiteral
	} else if rightField != nil && leftLiteral != nil {
		field = rightField
		literal = leftLiteral
		operator = reverseComparison(operator)
	} else {
		return "", nil, fmt.Errorf("comparison requires one field and one literal")
	}

	column := findColumn(table, field.GetName())
	if column == nil {
		return "", nil, fmt.Errorf("column %q was not found", field.GetName())
	}
	operatorText, err := comparisonOperator(operator)
	if err != nil {
		return "", nil, err
	}
	value, err := providerToNative(literal.GetValue(), column.Type)
	if err != nil {
		return "", nil, fmt.Errorf("column %q: %w", column.Name, err)
	}
	if value == nil {
		return "", nil, fmt.Errorf("Cassandra comparisons do not support null literals")
	}

	return quoteIdentifier(column.Name) + " " + operatorText + " ?", []any{value}, nil
}

func comparisonOperator(
	operator providerv1.ComparisonOperator,
) (string, error) {
	switch operator {
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL:
		return "=", nil
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN:
		return "<", nil
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
		return "<=", nil
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN:
		return ">", nil
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
		return ">=", nil
	default:
		return "", fmt.Errorf("unsupported Cassandra comparison operator %s", operator)
	}
}

func reverseComparison(
	operator providerv1.ComparisonOperator,
) providerv1.ComparisonOperator {
	switch operator {
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN:
		return providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
		return providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN:
		return providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN
	case providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
		return providerv1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL
	default:
		return operator
	}
}

func buildOrdering(
	table *gocql.TableMetadata,
	orderBy []*providerv1.OrderBy,
) (string, error) {
	if len(orderBy) == 0 {
		return "", nil
	}

	parts := make([]string, 0, len(orderBy))
	for _, ordering := range orderBy {
		if ordering == nil || ordering.GetExpression() == nil {
			return "", fmt.Errorf("ordering expression is required")
		}
		if ordering.GetNullOrdering() != providerv1.NullOrdering_NULL_ORDERING_UNSPECIFIED {
			return "", fmt.Errorf("Cassandra does not support explicit null ordering")
		}
		field := ordering.GetExpression().GetField()
		if field == nil {
			return "", fmt.Errorf("Cassandra ordering supports only field references")
		}
		column := findColumn(table, field.GetName())
		if column == nil {
			return "", fmt.Errorf("column %q was not found", field.GetName())
		}

		direction := "ASC"
		switch ordering.GetDirection() {
		case providerv1.SortDirection_SORT_DIRECTION_UNSPECIFIED,
			providerv1.SortDirection_SORT_DIRECTION_ASCENDING:
		case providerv1.SortDirection_SORT_DIRECTION_DESCENDING:
			direction = "DESC"
		default:
			return "", fmt.Errorf("unsupported sort direction %s", ordering.GetDirection())
		}
		parts = append(parts, quoteIdentifier(column.Name)+" "+direction)
	}

	return strings.Join(parts, ", "), nil
}

func queryLimit(limit *uint64) (string, error) {
	if limit == nil {
		return "", nil
	}
	if *limit > math.MaxInt32 {
		return "", fmt.Errorf("limit %d exceeds Cassandra maximum", *limit)
	}

	return fmt.Sprintf(" LIMIT %d", *limit), nil
}

func queryPageSize(batchSize *uint32) int {
	if batchSize == nil || *batchSize == 0 {
		return 100
	}
	if *batchSize > math.MaxInt32 {
		return math.MaxInt32
	}

	return int(*batchSize)
}
