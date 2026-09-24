package querylog

import (
	"context"
	"log/slog"
	"strings"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// Summary contains only safe structural metadata about a provider query.
type Summary struct {
	Entity             string
	Mode               string
	AggregateFunctions []string
	ProjectionCount    int
	GroupByCount       int
	Having             bool
}

// Summarize describes a query without retaining values, predicates or
// connection identifiers.
func Summarize(request *providerv1.QueryRequest) Summary {
	if request == nil {
		return Summary{Mode: "row"}
	}

	functions := aggregateFunctions(request)
	mode := "row"
	if len(functions) > 0 || len(request.GetGroupBy()) > 0 || request.GetHaving() != nil {
		mode = "aggregate"
	}

	return Summary{
		Entity:             request.GetEntity().GetName(),
		Mode:               mode,
		AggregateFunctions: functions,
		ProjectionCount:    len(request.GetProjections()),
		GroupByCount:       len(request.GetGroupBy()),
		Having:             request.GetHaving() != nil,
	}
}

// Received records a query at the gRPC boundary, before cache lookup.
func Received(
	ctx context.Context,
	logger *slog.Logger,
	request *providerv1.QueryRequest,
) {
	log(ctx, logger, "provider query received", request)
}

// Executed records a query after the in-memory implementation evaluates it.
func Executed(
	ctx context.Context,
	logger *slog.Logger,
	request *providerv1.QueryRequest,
	outputRows int,
) {
	if logger == nil {
		return
	}
	attributes := attributes(request)
	attributes = append(attributes, slog.Int("output_rows", outputRows))
	logger.LogAttrs(
		ctx,
		slog.LevelInfo,
		"provider query executed",
		attributes...,
	)
}

func log(
	ctx context.Context,
	logger *slog.Logger,
	message string,
	request *providerv1.QueryRequest,
) {
	if logger == nil {
		return
	}
	logger.LogAttrs(ctx, slog.LevelInfo, message, attributes(request)...)
}

func attributes(request *providerv1.QueryRequest) []slog.Attr {
	summary := Summarize(request)
	return []slog.Attr{
		slog.String("provider", "inmemory"),
		slog.String("entity", summary.Entity),
		slog.String("mode", summary.Mode),
		slog.Any("aggregate_functions", summary.AggregateFunctions),
		slog.Int("projection_count", summary.ProjectionCount),
		slog.Int("group_by_count", summary.GroupByCount),
		slog.Bool("having", summary.Having),
	}
}

func aggregateFunctions(request *providerv1.QueryRequest) []string {
	var functions []string
	seen := make(map[providerv1.AggregateFunction]struct{})
	var collect func(*providerv1.Expression)
	collect = func(expression *providerv1.Expression) {
		if expression == nil {
			return
		}
		switch kind := expression.GetKind().(type) {
		case *providerv1.Expression_Aggregate:
			if kind.Aggregate == nil {
				return
			}
			function := kind.Aggregate.GetFunction()
			if _, exists := seen[function]; !exists {
				seen[function] = struct{}{}
				functions = append(functions, strings.TrimPrefix(
					function.String(),
					"AGGREGATE_FUNCTION_",
				))
			}
			for _, argument := range kind.Aggregate.GetArguments() {
				collect(argument)
			}
		case *providerv1.Expression_Comparison:
			if kind.Comparison != nil {
				collect(kind.Comparison.GetLeft())
				collect(kind.Comparison.GetRight())
			}
		case *providerv1.Expression_Logical:
			if kind.Logical != nil {
				for _, operand := range kind.Logical.GetOperands() {
					collect(operand)
				}
			}
		case *providerv1.Expression_NullPredicate:
			if kind.NullPredicate != nil {
				collect(kind.NullPredicate.GetExpression())
			}
		case *providerv1.Expression_FunctionCall:
			if kind.FunctionCall != nil {
				for _, argument := range kind.FunctionCall.GetArguments() {
					collect(argument)
				}
			}
		case *providerv1.Expression_Pattern:
			if kind.Pattern != nil {
				collect(kind.Pattern.GetValue())
				collect(kind.Pattern.GetPattern())
			}
		}
	}

	for _, projection := range request.GetProjections() {
		if projection != nil {
			collect(projection.GetExpression())
		}
	}
	collect(request.GetFilter())
	for _, ordering := range request.GetOrderBy() {
		if ordering != nil {
			collect(ordering.GetExpression())
		}
	}
	for _, expression := range request.GetGroupBy() {
		collect(expression)
	}
	collect(request.GetHaving())

	return functions
}
