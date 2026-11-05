package com.kubling.provider.oracle;

import com.kubling.provider.grpc.AggregateCall;
import com.kubling.provider.grpc.AggregateFunction;
import com.kubling.provider.grpc.ComparisonExpression;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.EntityReference;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.LogicalExpression;
import com.kubling.provider.grpc.LogicalOperator;
import com.kubling.provider.grpc.NullOrdering;
import com.kubling.provider.grpc.NullPredicate;
import com.kubling.provider.grpc.NullPredicateOperator;
import com.kubling.provider.grpc.OrderBy;
import com.kubling.provider.grpc.PatternExpression;
import com.kubling.provider.grpc.PatternOperator;
import com.kubling.provider.grpc.Projection;
import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.SortDirection;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import java.util.ArrayList;
import java.util.List;

final class QueryTranslator {
    static final int DEFAULT_BATCH_SIZE = 256;
    static final int MAX_BATCH_SIZE = 4096;

    private static final int MAX_EXPRESSIONS_PER_CLAUSE = 1024;
    private static final int MAX_EXPRESSION_DEPTH = 64;
    private static final int MAX_PARAMETERS = 10_000;

    private final SchemaPolicy schemaPolicy;

    QueryTranslator(SchemaPolicy schemaPolicy) {
        this.schemaPolicy = schemaPolicy;
    }

    QueryPlan translate(QueryRequest request) {
        require(request.hasEntity(), "query entity is required");
        require(!request.getProjectionsList().isEmpty(), "at least one projection is required");
        requireBounded(request.getProjectionsCount(), "projections");
        requireBounded(request.getGroupByCount(), "group_by expressions");
        requireBounded(request.getOrderByCount(), "order_by expressions");

        List<Literal> parameters = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int index = 0; index < request.getProjectionsCount(); index++) {
            if (index > 0) {
                sql.append(", ");
            }
            Projection projection = request.getProjections(index);
            require(projection.hasExpression(), "projection expression is required");
            requireIdentifier(projection.getOutputName(), "projection output name");
            sql.append(renderProjection(projection.getExpression(), parameters))
                    .append(" AS \"KBL_C")
                    .append(index)
                    .append('"');
        }

        sql.append(" FROM ").append(qualifiedEntity(request.getEntity()));

        if (request.hasFilter()) {
            sql.append(" WHERE ")
                    .append(render(request.getFilter(), parameters, false, 0));
        }
        if (request.getGroupByCount() > 0) {
            sql.append(" GROUP BY ");
            appendExpressions(sql, request.getGroupByList(), parameters, false);
        }
        if (request.hasHaving()) {
            sql.append(" HAVING ")
                    .append(render(request.getHaving(), parameters, true, 0));
        }
        if (request.getOrderByCount() > 0) {
            sql.append(" ORDER BY ");
            for (int index = 0; index < request.getOrderByCount(); index++) {
                if (index > 0) {
                    sql.append(", ");
                }
                appendOrderBy(sql, request.getOrderBy(index), parameters);
            }
        }

        appendPagination(sql, request, parameters);
        return new QueryPlan(sql.toString(), parameters, batchSize(request));
    }

    String qualifiedEntity(EntityReference entity) {
        String namespace = requireIdentifier(entity.getNamespace(), "entity namespace");
        String name = requireIdentifier(entity.getName(), "entity name");
        require(schemaPolicy.includes(namespace), "entity namespace is not exposed");
        require(InternalObjects.isExposedRelation(name), "entity is reserved by the provider");
        return quoteIdentifier(namespace) + '.' + quoteIdentifier(name);
    }

    String renderMutationPredicate(Expression expression, List<Literal> parameters) {
        require(expression != null, "mutation expression is required");
        return render(expression, parameters, false, 0);
    }

    String renderAssignmentValue(Expression expression, List<Literal> parameters) {
        require(expression != null, "assignment value is required");
        require(
                expression.getKindCase() == Expression.KindCase.FIELD
                        || expression.getKindCase() == Expression.KindCase.LITERAL,
                "Oracle assignments support field references and literal values");
        return render(expression, parameters, false, 0);
    }

    private String renderProjection(Expression expression, List<Literal> parameters) {
        return switch (expression.getKindCase()) {
            case COMPARISON, LOGICAL, NULL_PREDICATE, PATTERN -> {
                String trueExpression = render(expression, parameters, true, 0);
                String falseExpression = render(expression, parameters, true, 0);
                yield "CASE WHEN " + trueExpression + " THEN 1 WHEN NOT "
                        + falseExpression + " THEN 0 ELSE NULL END";
            }
            default -> render(expression, parameters, true, 0);
        };
    }

    private void appendExpressions(
            StringBuilder sql,
            List<Expression> expressions,
            List<Literal> parameters,
            boolean allowAggregate) {
        for (int index = 0; index < expressions.size(); index++) {
            if (index > 0) {
                sql.append(", ");
            }
            sql.append(render(expressions.get(index), parameters, allowAggregate, 0));
        }
    }

    private void appendOrderBy(
            StringBuilder sql,
            OrderBy orderBy,
            List<Literal> parameters) {
        require(orderBy.hasExpression(), "order_by expression is required");
        sql.append(render(orderBy.getExpression(), parameters, true, 0));
        if (orderBy.getDirection() == SortDirection.SORT_DIRECTION_ASCENDING) {
            sql.append(" ASC");
        } else if (orderBy.getDirection() == SortDirection.SORT_DIRECTION_DESCENDING) {
            sql.append(" DESC");
        } else {
            throw invalid("order_by direction is required");
        }

        if (orderBy.getNullOrdering() == NullOrdering.NULL_ORDERING_FIRST) {
            sql.append(" NULLS FIRST");
        } else if (orderBy.getNullOrdering() == NullOrdering.NULL_ORDERING_LAST) {
            sql.append(" NULLS LAST");
        } else {
            throw invalid("order_by null ordering is required");
        }
    }

    private void appendPagination(
            StringBuilder sql,
            QueryRequest request,
            List<Literal> parameters) {
        if (request.hasOffset()) {
            sql.append(" OFFSET ? ROWS");
            parameters.add(unsignedLongLiteral(request.getOffset(), "offset"));
            if (request.hasLimit()) {
                sql.append(" FETCH NEXT ? ROWS ONLY");
                parameters.add(unsignedLongLiteral(request.getLimit(), "limit"));
            }
        } else if (request.hasLimit()) {
            sql.append(" FETCH FIRST ? ROWS ONLY");
            parameters.add(unsignedLongLiteral(request.getLimit(), "limit"));
        }
        require(parameters.size() <= MAX_PARAMETERS, "query contains too many parameters");
    }

    private String render(
            Expression expression,
            List<Literal> parameters,
            boolean allowAggregate,
            int depth) {
        require(depth <= MAX_EXPRESSION_DEPTH, "expression nesting is too deep");
        return switch (expression.getKindCase()) {
            case FIELD -> quoteIdentifier(requireIdentifier(
                    expression.getField().getName(), "field name"));
            case LITERAL -> renderLiteral(expression.getLiteral(), parameters);
            case COMPARISON -> renderComparison(
                    expression.getComparison(), parameters, allowAggregate, depth + 1);
            case LOGICAL -> renderLogical(
                    expression.getLogical(), parameters, allowAggregate, depth + 1);
            case NULL_PREDICATE -> renderNullPredicate(
                    expression.getNullPredicate(), parameters, allowAggregate, depth + 1);
            case PATTERN -> renderPattern(
                    expression.getPattern(), parameters, allowAggregate, depth + 1);
            case AGGREGATE -> {
                require(allowAggregate, "aggregate is not allowed in this clause");
                yield renderAggregate(expression.getAggregate(), parameters, depth + 1);
            }
            case FUNCTION_CALL -> throw invalid("scalar function pushdown is not supported");
            case KIND_NOT_SET -> throw invalid("expression kind is required");
        };
    }

    private String renderLiteral(Literal literal, List<Literal> parameters) {
        require(literal.hasValue(), "literal value is required");
        require(parameters.size() < MAX_PARAMETERS, "query contains too many parameters");
        parameters.add(literal);
        return "?";
    }

    private String renderComparison(
            ComparisonExpression comparison,
            List<Literal> parameters,
            boolean allowAggregate,
            int depth) {
        require(comparison.hasLeft() && comparison.hasRight(),
                "comparison operands are required");
        String operator = switch (comparison.getOperator()) {
            case COMPARISON_OPERATOR_EQUAL -> "=";
            case COMPARISON_OPERATOR_NOT_EQUAL -> "<>";
            case COMPARISON_OPERATOR_LESS_THAN -> "<";
            case COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL -> "<=";
            case COMPARISON_OPERATOR_GREATER_THAN -> ">";
            case COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL -> ">=";
            case COMPARISON_OPERATOR_UNSPECIFIED, UNRECOGNIZED ->
                    throw invalid("comparison operator is required");
        };
        return "(" + render(comparison.getLeft(), parameters, allowAggregate, depth)
                + " " + operator + " "
                + render(comparison.getRight(), parameters, allowAggregate, depth) + ")";
    }

    private String renderLogical(
            LogicalExpression logical,
            List<Literal> parameters,
            boolean allowAggregate,
            int depth) {
        List<Expression> operands = logical.getOperandsList();
        if (logical.getOperator() == LogicalOperator.LOGICAL_OPERATOR_NOT) {
            require(operands.size() == 1, "NOT requires exactly one operand");
            return "(NOT " + render(operands.get(0), parameters, allowAggregate, depth) + ")";
        }
        require(operands.size() >= 2, "AND and OR require at least two operands");
        requireBounded(operands.size(), "logical operands");
        String operator = switch (logical.getOperator()) {
            case LOGICAL_OPERATOR_AND -> " AND ";
            case LOGICAL_OPERATOR_OR -> " OR ";
            case LOGICAL_OPERATOR_NOT, LOGICAL_OPERATOR_UNSPECIFIED, UNRECOGNIZED ->
                    throw invalid("logical operator is required");
        };
        StringBuilder sql = new StringBuilder("(");
        for (int index = 0; index < operands.size(); index++) {
            if (index > 0) {
                sql.append(operator);
            }
            sql.append(render(operands.get(index), parameters, allowAggregate, depth));
        }
        return sql.append(')').toString();
    }

    private String renderNullPredicate(
            NullPredicate predicate,
            List<Literal> parameters,
            boolean allowAggregate,
            int depth) {
        require(predicate.hasExpression(), "null predicate expression is required");
        String operator = switch (predicate.getOperator()) {
            case NULL_PREDICATE_OPERATOR_IS_NULL -> " IS NULL";
            case NULL_PREDICATE_OPERATOR_IS_NOT_NULL -> " IS NOT NULL";
            case NULL_PREDICATE_OPERATOR_UNSPECIFIED, UNRECOGNIZED ->
                    throw invalid("null predicate operator is required");
        };
        return "(" + render(predicate.getExpression(), parameters, allowAggregate, depth)
                + operator + ")";
    }

    private String renderPattern(
            PatternExpression pattern,
            List<Literal> parameters,
            boolean allowAggregate,
            int depth) {
        require(pattern.hasValue() && pattern.hasPattern(), "pattern operands are required");
        String operator = switch (pattern.getOperator()) {
            case PATTERN_OPERATOR_LIKE -> " LIKE ";
            case PATTERN_OPERATOR_NOT_LIKE -> " NOT LIKE ";
            case PATTERN_OPERATOR_UNSPECIFIED, UNRECOGNIZED ->
                    throw invalid("pattern operator is required");
        };
        StringBuilder sql = new StringBuilder("(")
                .append(render(pattern.getValue(), parameters, allowAggregate, depth))
                .append(operator)
                .append(render(pattern.getPattern(), parameters, allowAggregate, depth));
        if (pattern.hasEscape()) {
            String escape = pattern.getEscape();
            require(escape.codePointCount(0, escape.length()) == 1,
                    "pattern escape must contain exactly one character");
            sql.append(" ESCAPE ").append(renderLiteral(stringLiteral(escape), parameters));
        }
        return sql.append(')').toString();
    }

    private String renderAggregate(
            AggregateCall aggregate,
            List<Literal> parameters,
            int depth) {
        require(aggregate.hasResultType(), "aggregate result type is required");
        require(aggregate.getResultType().getType() != ValueType.VALUE_TYPE_UNKNOWN,
                "aggregate result type is invalid");
        require(aggregate.getResultType().getType() != ValueType.VALUE_TYPE_ARRAY,
                "array aggregate results are not supported");

        if (aggregate.getFunction() == AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR) {
            require(aggregate.getArgumentsCount() == 0,
                    "COUNT_STAR must not contain arguments");
            require(!aggregate.getDistinct(), "COUNT_STAR does not support DISTINCT");
            return "COUNT(*)";
        }
        require(aggregate.getArgumentsCount() == 1,
                "aggregate requires exactly one argument");
        String function = switch (aggregate.getFunction()) {
            case AGGREGATE_FUNCTION_COUNT, AGGREGATE_FUNCTION_COUNT_BIG -> "COUNT";
            case AGGREGATE_FUNCTION_MIN -> "MIN";
            case AGGREGATE_FUNCTION_MAX -> "MAX";
            case AGGREGATE_FUNCTION_SUM -> "SUM";
            case AGGREGATE_FUNCTION_AVG -> "AVG";
            case AGGREGATE_FUNCTION_COUNT_STAR, AGGREGATE_FUNCTION_UNSPECIFIED, UNRECOGNIZED ->
                    throw invalid("aggregate function is required");
        };
        String argument = render(aggregate.getArguments(0), parameters, false, depth);
        return function + "(" + (aggregate.getDistinct() ? "DISTINCT " : "")
                + argument + ")";
    }

    private static Literal unsignedLongLiteral(long value, String name) {
        require(value >= 0, name + " exceeds the supported signed 64-bit range");
        return Literal.newBuilder()
                .setValue(Value.newBuilder().setLongValue(value))
                .setDeclaredType(TypeDescriptor.newBuilder().setType(ValueType.VALUE_TYPE_LONG))
                .build();
    }

    private static Literal stringLiteral(String value) {
        return Literal.newBuilder()
                .setValue(Value.newBuilder().setStringValue(value))
                .setDeclaredType(TypeDescriptor.newBuilder().setType(ValueType.VALUE_TYPE_STRING))
                .build();
    }

    private static int batchSize(QueryRequest request) {
        if (!request.hasBatchSize()) {
            return DEFAULT_BATCH_SIZE;
        }
        long requested = Integer.toUnsignedLong(request.getBatchSize());
        require(requested > 0, "batch_size must be positive");
        return (int) Math.min(requested, MAX_BATCH_SIZE);
    }

    static String requireIdentifier(String value, String name) {
        require(value != null && !value.isBlank(), name + " is required");
        require(value.indexOf('\0') < 0, name + " contains an invalid character");
        return value;
    }

    static String quoteIdentifier(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static void requireBounded(int size, String name) {
        require(size <= MAX_EXPRESSIONS_PER_CLAUSE, name + " exceed the supported limit");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw invalid(message);
        }
    }

    private static RequestValidationException invalid(String message) {
        return new RequestValidationException(message);
    }
}
