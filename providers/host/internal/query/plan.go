package query

import (
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

type plannedProjection struct {
	sourceName string
	field      *providerv1.Field
}

type accessRoute struct {
	namespace  *string
	hostID     *string
	hostname   *string
	impossible bool
}

func (r accessRoute) unbounded() bool {
	return r.namespace == nil && r.hostID == nil && r.hostname == nil
}

type queryPlan struct {
	table       string
	projections []plannedProjection
	route       accessRoute
}

func (p queryPlan) fields() []*providerv1.Field {
	fields := make([]*providerv1.Field, 0, len(p.projections))
	for _, projection := range p.projections {
		fields = append(fields, projection.field)
	}
	return fields
}

func (p queryPlan) sourceColumns() []string {
	columns := make([]string, 0, len(p.projections))
	for _, projection := range p.projections {
		columns = append(columns, projection.sourceName)
	}
	return columns
}

func buildPlan(request *providerv1.QueryRequest) (queryPlan, error) {
	if request == nil {
		return queryPlan{}, fmt.Errorf("query request is required")
	}
	if request.GetEntity() == nil || strings.TrimSpace(request.GetEntity().GetName()) == "" {
		return queryPlan{}, fmt.Errorf("query entity is required")
	}
	if strings.TrimSpace(request.GetEntity().GetNamespace()) != "" {
		return queryPlan{}, fmt.Errorf("host tables do not use provider namespaces")
	}
	if len(request.GetOrderBy()) > 0 || request.Limit != nil || request.Offset != nil ||
		len(request.GetGroupBy()) > 0 || request.GetHaving() != nil {
		return queryPlan{}, fmt.Errorf("ordering, pagination and aggregation are not pushed to the Host Provider")
	}
	table := strings.ToUpper(strings.TrimSpace(request.GetEntity().GetName()))
	columns, exists := hostschema.Columns(table)
	if !exists {
		return queryPlan{}, fmt.Errorf("unknown host table %q", request.GetEntity().GetName())
	}
	columnByName := make(map[string]hostschema.Column, len(columns))
	for _, column := range columns {
		columnByName[strings.ToLower(column.Name)] = column
	}

	requested := request.GetProjections()
	projections := make([]plannedProjection, 0, len(requested))
	if len(requested) == 0 {
		for _, column := range columns {
			projections = append(projections, plannedProjection{
				sourceName: column.Name,
				field: &providerv1.Field{
					Name:           column.Name,
					Type:           column.Type,
					TypeDescriptor: column.TypeDescriptor,
				},
			})
		}
	} else {
		for index, projection := range requested {
			if projection == nil || projection.GetExpression() == nil {
				return queryPlan{}, fmt.Errorf("projection %d is required", index)
			}
			fieldReference := projection.GetExpression().GetField()
			if fieldReference == nil || strings.TrimSpace(fieldReference.GetName()) == "" {
				return queryPlan{}, fmt.Errorf("projection %d must be a field reference", index)
			}
			column, exists := columnByName[strings.ToLower(fieldReference.GetName())]
			if !exists {
				return queryPlan{}, fmt.Errorf("unknown %s column %q", table, fieldReference.GetName())
			}
			outputName := strings.TrimSpace(projection.GetOutputName())
			if outputName == "" {
				outputName = column.Name
			}
			projections = append(projections, plannedProjection{
				sourceName: column.Name,
				field: &providerv1.Field{
					Name:           outputName,
					Type:           column.Type,
					TypeDescriptor: column.TypeDescriptor,
				},
			})
		}
	}
	route, err := parseAccessFilter(request.GetFilter())
	if err != nil {
		return queryPlan{}, err
	}
	return queryPlan{table: table, projections: projections, route: route}, nil
}

func parseAccessFilter(expression *providerv1.Expression) (accessRoute, error) {
	if expression == nil {
		return accessRoute{}, nil
	}
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Comparison:
		if kind.Comparison == nil ||
			kind.Comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
			return accessRoute{}, fmt.Errorf("only equality access filters are supported")
		}
		field, value, ok := fieldStringEquality(kind.Comparison.GetLeft(), kind.Comparison.GetRight())
		if !ok {
			field, value, ok = fieldStringEquality(kind.Comparison.GetRight(), kind.Comparison.GetLeft())
		}
		if !ok {
			return accessRoute{}, fmt.Errorf("access filter must compare a field with a string literal")
		}
		route := accessRoute{}
		switch strings.ToLower(field) {
		case hostschema.NamespaceColumn:
			route.namespace = &value
		case hostschema.HostIDColumn:
			route.hostID = &value
		case hostschema.HostnameColumn:
			route.hostname = &value
		default:
			return accessRoute{}, fmt.Errorf("column %q is not an access field", field)
		}
		return route, nil
	case *providerv1.Expression_Logical:
		if kind.Logical == nil ||
			kind.Logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND ||
			len(kind.Logical.GetOperands()) < 2 {
			return accessRoute{}, fmt.Errorf("only AND combinations of access equalities are supported")
		}
		route := accessRoute{}
		for _, operand := range kind.Logical.GetOperands() {
			part, err := parseAccessFilter(operand)
			if err != nil {
				return accessRoute{}, err
			}
			mergeAccessValue(&route.namespace, part.namespace, &route.impossible)
			mergeAccessValue(&route.hostID, part.hostID, &route.impossible)
			mergeAccessValue(&route.hostname, part.hostname, &route.impossible)
		}
		return route, nil
	default:
		return accessRoute{}, fmt.Errorf("unsupported access filter expression")
	}
}

func fieldStringEquality(
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
	return field.GetName(), value.StringValue, true
}

func mergeAccessValue(destination **string, candidate *string, impossible *bool) {
	if candidate == nil || *impossible {
		return
	}
	if *destination != nil && **destination != *candidate {
		*impossible = true
		return
	}
	value := *candidate
	*destination = &value
}
