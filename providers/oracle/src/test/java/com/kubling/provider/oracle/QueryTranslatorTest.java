package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.kubling.provider.grpc.AggregateCall;
import com.kubling.provider.grpc.AggregateFunction;
import com.kubling.provider.grpc.ComparisonExpression;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.EntityReference;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.FieldReference;
import com.kubling.provider.grpc.FunctionCall;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.LogicalExpression;
import com.kubling.provider.grpc.LogicalOperator;
import com.kubling.provider.grpc.NullOrdering;
import com.kubling.provider.grpc.OrderBy;
import com.kubling.provider.grpc.PatternExpression;
import com.kubling.provider.grpc.PatternOperator;
import com.kubling.provider.grpc.Projection;
import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.SortDirection;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class QueryTranslatorTest {
    @Test
    void rejectsTheInternalTransactionJournal() {
        QueryTranslator translator = new QueryTranslator(
                new SchemaPolicy(Set.of("KUBLING"), Set.of()));
        QueryRequest request = QueryRequest.newBuilder()
                .setEntity(EntityReference.newBuilder()
                        .setNamespace("KUBLING")
                        .setName(JdbcTransactionJournal.TABLE_NAME))
                .addProjections(projection(field("RESOURCE_MANAGER_ID"), "resource_manager_id"))
                .build();

        assertThrows(RequestValidationException.class, () -> translator.translate(request));
    }

    private final QueryTranslator translator = new QueryTranslator(
            new SchemaPolicy(Set.of("KUBLING"), Set.of()));

    @Test
    void translatesTheSupportedQueryAstWithBoundParameters() {
        Expression amount = field("Amount");
        Expression name = field("select");
        Expression sum = aggregate(
                AggregateFunction.AGGREGATE_FUNCTION_SUM,
                ValueType.VALUE_TYPE_BIGDECIMAL,
                amount);
        Expression count = aggregate(
                AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR,
                ValueType.VALUE_TYPE_LONG);
        Expression filter = Expression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                        .addOperands(comparison(
                                ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
                                amount,
                                decimalLiteral("10.50")))
                        .addOperands(Expression.newBuilder()
                                .setPattern(PatternExpression.newBuilder()
                                        .setOperator(PatternOperator.PATTERN_OPERATOR_LIKE)
                                        .setValue(name)
                                        .setPattern(stringLiteral("A!_%"))
                                        .setEscape("!"))))
                .build();
        QueryRequest request = QueryRequest.newBuilder()
                .setConnectionId("connection")
                .setEntity(EntityReference.newBuilder()
                        .setNamespace("KUBLING")
                        .setName("Order"))
                .addProjections(projection(name, "select"))
                .addProjections(projection(sum, "total"))
                .setFilter(filter)
                .addGroupBy(name)
                .setHaving(comparison(
                        ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN,
                        count,
                        longLiteral(1)))
                .addOrderBy(OrderBy.newBuilder()
                        .setExpression(name)
                        .setDirection(SortDirection.SORT_DIRECTION_DESCENDING)
                        .setNullOrdering(NullOrdering.NULL_ORDERING_FIRST))
                .setOffset(2)
                .setLimit(10)
                .setBatchSize(5_000)
                .build();

        QueryPlan plan = translator.translate(request);

        assertEquals(
                "SELECT \"select\" AS \"KBL_C0\", SUM(\"Amount\") AS \"KBL_C1\" "
                        + "FROM \"KUBLING\".\"Order\" "
                        + "WHERE ((\"Amount\" >= ?) AND (\"select\" LIKE ? ESCAPE ?)) "
                        + "GROUP BY \"select\" HAVING (COUNT(*) > ?) "
                        + "ORDER BY \"select\" DESC NULLS FIRST "
                        + "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                plan.sql());
        assertEquals(QueryTranslator.MAX_BATCH_SIZE, plan.batchSize());
        assertEquals(6, plan.parameters().size());
        assertEquals("10.50", plan.parameters().get(0).getValue().getBigdecimalValue());
        assertEquals("A!_%", plan.parameters().get(1).getValue().getStringValue());
        assertEquals("!", plan.parameters().get(2).getValue().getStringValue());
        assertEquals(1, plan.parameters().get(3).getValue().getLongValue());
        assertEquals(2, plan.parameters().get(4).getValue().getLongValue());
        assertEquals(10, plan.parameters().get(5).getValue().getLongValue());
    }

    @Test
    void quotesIdentifiersAndNeverInterpolatesLiteralValues() {
        String namespace = "APP\"; DROP USER APP; --";
        QueryTranslator unusualTranslator = new QueryTranslator(
                new SchemaPolicy(Set.of(namespace), Set.of()));
        String maliciousValue = "x' OR 1=1 --";
        QueryRequest request = QueryRequest.newBuilder()
                .setEntity(EntityReference.newBuilder()
                        .setNamespace(namespace)
                        .setName("table\"name"))
                .addProjections(projection(field("column\"name"), "result"))
                .setFilter(comparison(
                        ComparisonOperator.COMPARISON_OPERATOR_EQUAL,
                        field("column\"name"),
                        stringLiteral(maliciousValue)))
                .build();

        QueryPlan plan = unusualTranslator.translate(request);

        assertEquals(
                "SELECT \"column\"\"name\" AS \"KBL_C0\" "
                        + "FROM \"APP\"\"; DROP USER APP; --\"."
                        + "\"table\"\"name\" WHERE (\"column\"\"name\" = ?)",
                plan.sql());
        assertFalse(plan.sql().contains(maliciousValue));
        assertEquals(maliciousValue, plan.parameters().get(0).getValue().getStringValue());
    }

    @Test
    void rejectsUnsupportedOrUnsafeQueryShapes() {
        QueryRequest base = QueryRequest.newBuilder()
                .setEntity(EntityReference.newBuilder()
                        .setNamespace("KUBLING")
                        .setName("Order"))
                .addProjections(projection(field("id"), "id"))
                .build();

        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder()
                        .setEntity(base.getEntity().toBuilder().setNamespace("SYS"))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder().clearProjections().build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder()
                        .setFilter(aggregate(
                                AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR,
                                ValueType.VALUE_TYPE_LONG))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder()
                        .setProjections(0, projection(
                                Expression.newBuilder()
                                        .setFunctionCall(FunctionCall.newBuilder().setName("LOWER"))
                                        .build(),
                                "lower_id"))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder().setLimit(-1L).build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder().setBatchSize(0).build()));
    }

    private static Projection projection(Expression expression, String outputName) {
        return Projection.newBuilder()
                .setExpression(expression)
                .setOutputName(outputName)
                .build();
    }

    private static Expression field(String name) {
        return Expression.newBuilder()
                .setField(FieldReference.newBuilder().setName(name))
                .build();
    }

    private static Expression comparison(
            ComparisonOperator operator,
            Expression left,
            Expression right) {
        return Expression.newBuilder()
                .setComparison(ComparisonExpression.newBuilder()
                        .setOperator(operator)
                        .setLeft(left)
                        .setRight(right))
                .build();
    }

    private static Expression aggregate(
            AggregateFunction function,
            ValueType resultType,
            Expression... arguments) {
        AggregateCall.Builder aggregate = AggregateCall.newBuilder()
                .setFunction(function)
                .setResultType(TypeDescriptor.newBuilder().setType(resultType));
        for (Expression argument : arguments) {
            aggregate.addArguments(argument);
        }
        return Expression.newBuilder().setAggregate(aggregate).build();
    }

    private static Expression stringLiteral(String value) {
        return literal(Value.newBuilder().setStringValue(value), ValueType.VALUE_TYPE_STRING);
    }

    private static Expression decimalLiteral(String value) {
        return literal(Value.newBuilder().setBigdecimalValue(value), ValueType.VALUE_TYPE_BIGDECIMAL);
    }

    private static Expression longLiteral(long value) {
        return literal(Value.newBuilder().setLongValue(value), ValueType.VALUE_TYPE_LONG);
    }

    private static Expression literal(Value.Builder value, ValueType type) {
        return Expression.newBuilder()
                .setLiteral(Literal.newBuilder()
                        .setValue(value)
                        .setDeclaredType(TypeDescriptor.newBuilder().setType(type)))
                .build();
    }
}
